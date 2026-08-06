//! Postgres destination. Arrow `RecordBatch` → binary COPY into a staging table, then atomic swap.

use std::future::ready;
use std::pin::{Pin, pin};
use std::time::Instant;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls, binary_copy::BinaryCopyInWriter};
use tracing::warn;
use transferred_core::{BatchStream, Destination, Result, RunReport, TransferredError};

use postgres_protocol::escape::escape_identifier;

use crate::arrow_to_pg::{ArrowToPg, PgValue};

/// Suffix for the staging table the load fills before the swap. A table carrying it is a
/// leftover from an interrupted run and is safe to drop.
pub const STAGING_SUFFIX: &str = "__transferred_staging";

/// PG truncates identifiers past `NAMEDATALEN - 1`, which would let staging collide with its target.
/// <https://www.postgresql.org/docs/17/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS>
const MAX_IDENTIFIER_BYTES: usize = 63;

/// A `Destination` that replaces a Postgres table with the transferred rows.
/// Loads into a staging table first, then swaps it in one transaction, so
/// the target stays readable until the swap and is never half-written.
pub struct PostgresDestination {
    /// Postgres connection string.
    pub dsn: String,
    /// Table to replace. Created if absent; qualify it to pick a schema.
    pub table: String,
}

impl PostgresDestination {
    /// Construct a `PostgresDestination`. No I/O performed.
    #[must_use]
    pub fn new(dsn: String, table: String) -> Self {
        Self { dsn, table }
    }
}

#[async_trait]
impl Destination for PostgresDestination {
    async fn write_partitions(self: Box<Self>, partitions: Vec<BatchStream>) -> Result<RunReport> {
        let start = Instant::now();
        let mut client = connect(&self.dsn).await?;
        let target = Target::resolve(&client, &self.table).await?;

        let rows = match load_staging(&client, &target, partitions).await {
            Ok(rows) => rows,
            Err(error) => {
                drop_staging(&client, &target).await;
                return Err(error);
            }
        };

        if let Err(error) = target.swap(&mut client).await {
            drop_staging(&client, &target).await;
            return Err(error);
        }

        Ok(RunReport {
            rows,
            bytes_written: 0,
            written_objects: vec![target.qualified.clone()],
            duration: start.elapsed(),
            coercions: vec![],
        })
    }
}

/// Create the staging table from the first batch's schema and COPY every batch into it.
async fn load_staging(
    client: &Client,
    target: &Target,
    partitions: Vec<BatchStream>,
) -> Result<u64> {
    // Every partition lands in one table, so partition identity carries no meaning here.
    let mut rest = futures::stream::iter(partitions).flatten().boxed();

    // The staging DDL needs a schema, which only the first batch can supply.
    let Some(first) = rest.try_next().await? else {
        return Err(TransferredError::EmptySource);
    };

    let arrow_to_pg = ArrowToPg::derive(&first.schema())?;
    client
        .batch_execute(&format!(
            "drop table if exists {staging}; create table {staging} ({declarations})",
            staging = target.staging,
            declarations = arrow_to_pg.declarations(),
        ))
        .await
        .map_err(TransferredError::destination)?;

    let sink = client
        .copy_in(&format!(
            "copy {staging} from stdin (format binary)",
            staging = target.staging
        ))
        .await
        .map_err(TransferredError::destination)?;

    let mut writer = pin!(BinaryCopyInWriter::new(sink, &arrow_to_pg.pg_types()));
    let mut batches = pin!(stream::once(ready(Ok(first))).chain(rest));

    while let Some(batch) = batches.try_next().await? {
        write_rows(writer.as_mut(), &arrow_to_pg.encode(&batch)?).await?;
    }

    writer.finish().await.map_err(TransferredError::destination)
}

/// Write one COPY row per Arrow row, reading the encoded columns in lockstep.
async fn write_rows(
    mut writer: Pin<&mut BinaryCopyInWriter>,
    columns: &[Vec<PgValue<'_>>],
) -> Result<()> {
    let num_rows = shared_row_count(columns)?;

    // One cursor per column, advanced in lockstep, so a row costs no allocation.
    let mut cursors: Vec<_> = columns.iter().map(|values| values.iter()).collect();
    let mut row: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(columns.len());

    for _ in 0..num_rows {
        row.clear();
        for cursor in &mut cursors {
            // Every column holds `num_rows` values, checked above, so each cursor yields here.
            if let Some(value) = cursor.next() {
                row.push(&**value);
            }
        }

        writer
            .as_mut()
            .write(&row)
            .await
            .map_err(TransferredError::destination)?;
    }

    Ok(())
}

/// Row count every encoded column agrees on. COPY sends whole rows, so disagreement is fatal.
fn shared_row_count(columns: &[Vec<PgValue<'_>>]) -> Result<usize> {
    let num_rows = columns.first().map_or(0, Vec::len);

    if let Some((index, values)) = columns
        .iter()
        .enumerate()
        .find(|(_, values)| values.len() != num_rows)
    {
        return Err(TransferredError::destination(format!(
            "encoded columns disagree on row count: \
             column {index} has {}, column 0 has {num_rows}",
            values.len()
        )));
    }

    Ok(num_rows)
}

/// Remove a leftover staging table, logging failures rather than masking the error that got us here.
async fn drop_staging(client: &Client, target: &Target) {
    let sql = format!("drop table if exists {}", target.staging);
    if let Err(error) = client.batch_execute(&sql).await {
        warn!(table = %target.staging, %error, "failed to drop staging table");
    }
}

/// A resolved target table and the staging table standing in for it during the load.
struct Target {
    /// Quoted, schema-qualified target, ready to interpolate into SQL.
    qualified: String,
    /// Quoted, schema-qualified staging table.
    staging: String,
    /// Quoted bare name the staging table is renamed to, which `ALTER TABLE` wants unqualified.
    bare: String,
}

impl Target {
    /// Split `table` into identifier parts using PG's own parser, then requote both names.
    async fn resolve(client: &Client, table: &str) -> Result<Self> {
        let parts: Vec<String> = client
            .query_one("select parse_ident($1)", &[&table])
            .await
            .map_err(TransferredError::destination)?
            .get(0);

        let (name, schema) = parts
            .split_last()
            .ok_or_else(|| TransferredError::destination("target table name is empty"))?;

        let staging = format!("{name}{STAGING_SUFFIX}");
        if staging.len() > MAX_IDENTIFIER_BYTES {
            return Err(TransferredError::destination(format!(
                "staging table name `{staging}` exceeds the \
                 {MAX_IDENTIFIER_BYTES}-byte Postgres identifier limit"
            )));
        }

        Ok(Self {
            qualified: qualify(schema, name),
            staging: qualify(schema, &staging),
            bare: escape_identifier(name),
        })
    }

    /// Replace the target with the staging table. Transactional DDL, so the swap is all-or-nothing.
    /// Failure drops the `Transaction`, which rolls back and leaves the session usable for cleanup.
    async fn swap(&self, client: &mut Client) -> Result<()> {
        let transaction = client
            .transaction()
            .await
            .map_err(TransferredError::destination)?;

        transaction
            .batch_execute(&format!(
                "drop table if exists {target}; \
                 alter table {staging} rename to {bare};",
                target = self.qualified,
                staging = self.staging,
                bare = self.bare,
            ))
            .await
            .map_err(TransferredError::destination)?;

        transaction
            .commit()
            .await
            .map_err(TransferredError::destination)
    }
}

/// Join identifier parts into one quoted, dotted table reference.
fn qualify(schema: &[String], name: &str) -> String {
    schema
        .iter()
        .map(String::as_str)
        .chain([name])
        .map(escape_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

async fn connect(dsn: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .map_err(TransferredError::destination)?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            warn!(%error, "postgres connection closed");
        }
    });

    Ok(client)
}
