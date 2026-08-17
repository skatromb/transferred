//! Postgres destination. Arrow `RecordBatch` → binary COPY into a staging table, then atomic swap.

use std::future::ready;
use std::pin::{Pin, pin};
use std::time::Instant;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt, TryStreamExt, stream};
use tokio_postgres::types::IsNull;
use tokio_postgres::{Client, CopyInSink};
use tracing::warn;
use transferred_core::{BatchStream, Destination, Result, RunReport, TransferredError};

use postgres_protocol::escape::escape_identifier;

use crate::arrow_to_pg::{ArrowToPg, WriteValue};
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

/// Creates the staging table from the first batch's schema, then COPYs every batch into it.
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

    let mut copy = BinaryCopy::new(sink);
    let mut batches = pin!(stream::once(ready(Ok(first))).chain(rest));

    while let Some(batch) = batches.try_next().await? {
        copy.write_rows(&arrow_to_pg.bind(&batch)?, batch.num_rows())
            .await?;
    }

    copy.finish().await
}

/// Removes a leftover staging table, logging failures rather than masking the error that got us here.
async fn drop_staging(client: &Client, target: &Target) {
    let sql = format!("drop table if exists {}", target.staging);
    if let Err(error) = client.batch_execute(&sql).await {
        warn!(target: "postgres::destination", table = %target.staging, %error, "failed to drop staging table");
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

/// Bytes every binary COPY stream starts with, before the flags and header extension.
const COPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// Width of the length written before every field.
const FIELD_LEN_BYTES: usize = size_of::<i32>();

/// Field length that means NULL.
const NULL_FIELD: i32 = -1;

/// Field count that ends the rows.
const COPY_TRAILER: i16 = -1;

/// Bytes buffered before a chunk goes out; 4 KB costs a third more client CPU, 64 KB is the plateau.
const CHUNK_BYTES: usize = 64 << 10;

/// Writes rows into a Postgres binary COPY stream, sending them a chunk at a time.
/// <https://www.postgresql.org/docs/18/sql-copy.html#id-1.9.3.55.9.4.6>
/// Replaces `BinaryCopyInWriter` for performance, which boxes every value and flushes every 4 KB.
/// <https://docs.rs/tokio-postgres/0.7.18/src/tokio_postgres/binary_copy.rs.html>
struct BinaryCopy {
    sink: Pin<Box<CopyInSink<Bytes>>>,
    buf: BytesMut,
}

impl BinaryCopy {
    /// Opens the stream. The header goes out with the first chunk.
    fn new(sink: CopyInSink<Bytes>) -> Self {
        let mut buf = BytesMut::with_capacity(CHUNK_BYTES);
        buf.put_slice(COPY_SIGNATURE);
        buf.put_i32(0); // no flags
        buf.put_i32(0); // no header extension

        Self {
            sink: Box::pin(sink),
            buf,
        }
    }

    /// Writes one COPY row per Arrow row, sending whenever the buffer fills.
    async fn write_rows(&mut self, columns: &[WriteValue], num_rows: usize) -> Result<()> {
        let fields = i16::try_from(columns.len()).map_err(|_| {
            TransferredError::destination(format!(
                "a COPY row holds at most {} columns, not {}",
                i16::MAX,
                columns.len()
            ))
        })?;

        for row in 0..num_rows {
            self.push_row(columns, row, fields)?;
            if self.buf.len() >= CHUNK_BYTES {
                self.send().await?;
            }
        }

        Ok(())
    }

    /// Appends one row: how many fields it has, then the fields.
    fn push_row(&mut self, columns: &[WriteValue], row: usize, fields: i16) -> Result<()> {
        self.buf.put_i16(fields);

        for write in columns {
            self.push_field(write, row)?;
        }

        Ok(())
    }

    /// Appends one field: its length, then its bytes.
    fn push_field(&mut self, write: &WriteValue, row: usize) -> Result<()> {
        // The length is only known once the value is written, so leave a hole and come back.
        let start_at = self.buf.len();
        self.buf.put_i32(0);

        let len = match write(row, &mut self.buf)? {
            IsNull::Yes => NULL_FIELD,
            // Whatever the encoder appended past the hole is the value.
            IsNull::No => {
                i32::try_from(self.buf.len() - start_at - FIELD_LEN_BYTES).map_err(|_| {
                    TransferredError::destination("value is too large for a COPY field")
                })?
            }
        };

        let Some(slot) = self.buf.get_mut(start_at..start_at + FIELD_LEN_BYTES) else {
            return Err(TransferredError::destination(
                "COPY field length slot is out of bounds",
            ));
        };
        slot.copy_from_slice(&len.to_be_bytes());

        Ok(())
    }

    /// Writes the trailer, closes the stream and returns the rows Postgres took.
    async fn finish(mut self) -> Result<u64> {
        self.buf.put_i16(COPY_TRAILER);
        self.send().await?;

        self.sink
            .as_mut()
            .finish()
            .await
            .map_err(TransferredError::destination)
    }

    /// Sends the buffered bytes and empties the buffer.
    async fn send(&mut self) -> Result<()> {
        self.sink
            .send(self.buf.split().freeze())
            .await
            .map_err(TransferredError::destination)
    }
}
