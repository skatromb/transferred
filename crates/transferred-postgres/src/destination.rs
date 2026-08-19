//! Postgres destination. Arrow `RecordBatch` → binary COPY into a staging table, then atomic swap.

mod arrow_to_pg;
mod copy_in;

use std::future::ready;
use std::pin::pin;
use std::time::Instant;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use tokio_postgres::Client;
use tracing::warn;
use transferred_core::{BatchStream, Destination, Result, RunReport, TransferredError};

use postgres_protocol::escape::escape_identifier;

use self::arrow_to_pg::Encoder;
use self::copy_in::CopyIn;
use crate::connection::connect;

/// Suffix marking the staging table a load fills before the swap; a leftover one is safe to drop.
pub const STAGING_SUFFIX: &str = "__transferred_staging";

/// PG truncates identifiers past `NAMEDATALEN - 1`, which would let staging collide with its target.
/// <https://www.postgresql.org/docs/17/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS>
const MAX_IDENTIFIER_BYTES: usize = 63;

/// A `Destination` that replaces a Postgres table, loading into staging and swapping in one transaction.
pub struct PostgresDestination {
    /// Postgres connection string.
    pub dsn: String,
    /// Table to replace. Created if absent; qualify it to pick a schema.
    pub table: String,
}

impl PostgresDestination {
    /// Constructs a `PostgresDestination`. No I/O performed.
    #[must_use]
    pub fn new(dsn: String, table: String) -> Self {
        Self { dsn, table }
    }
}

#[async_trait]
impl Destination for PostgresDestination {
    async fn write_partitions(self: Box<Self>, partitions: Vec<BatchStream>) -> Result<RunReport> {
        let start = Instant::now();
        let mut client = connect(&self.dsn)
            .await
            .map_err(TransferredError::destination)?;
        let target = Target::resolve(&client, &self.table).await?;

        let rows = match target.load(&client, partitions).await {
            Ok(rows) => rows,
            Err(error) => {
                target.drop_staging(&client).await;
                return Err(error);
            }
        };

        if let Err(error) = target.swap(&mut client).await {
            target.drop_staging(&client).await;
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
    /// Splits `table` into identifier parts using PG's own parser, then requotes both names.
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

    /// Creates the staging table from the first batch's schema, then COPYs every batch into it.
    async fn load(&self, client: &Client, partitions: Vec<BatchStream>) -> Result<u64> {
        let mut rest = stream::iter(partitions).flatten().boxed();

        let Some(first) = rest.try_next().await? else {
            return Err(TransferredError::EmptySource);
        };

        let encoder = Encoder::new(first.schema())?;
        self.create_staging(client, &encoder.declarations()).await?;

        let mut batches = pin!(stream::once(ready(Ok(first))).chain(rest));
        let mut copy = CopyIn::open(client, &self.staging).await?;

        while let Some(batch) = batches.try_next().await? {
            copy.write_batch(&encoder, &batch).await?;
        }

        copy.finish().await
    }

    /// Replaces the target with the staging table in one transaction, so the swap is all-or-nothing.
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

    /// Creates the staging table, replacing whatever an interrupted load left behind.
    async fn create_staging(&self, client: &Client, declarations: &str) -> Result<()> {
        client
            .batch_execute(&format!(
                "drop table if exists {staging}; create table {staging} ({declarations})",
                staging = self.staging,
            ))
            .await
            .map_err(TransferredError::destination)
    }

    /// Removes a leftover staging table, logging failures rather than masking the error that got us here.
    async fn drop_staging(&self, client: &Client) {
        let sql = format!("drop table if exists {}", self.staging);
        if let Err(error) = client.batch_execute(&sql).await {
            warn!(target: "postgres::destination", table = %self.staging, %error, "failed to drop staging table");
        }
    }
}

/// Joins identifier parts into one quoted, dotted table reference.
fn qualify(schema: &[String], name: &str) -> String {
    schema
        .iter()
        .map(String::as_str)
        .chain([name])
        .map(escape_identifier)
        .collect::<Vec<_>>()
        .join(".")
}
