//! Loads staging before atomically replacing the MySQL target.

mod encoder;
mod encoding;
mod values;
mod writer;

use std::time::Instant;

use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use log::warn;
use mysql_async::Conn;
use transferred_core::{BatchStream, Destination, Result, RunReport, TransferredError};

use self::encoder::Encoder;
use self::writer::Writer;
use crate::connection::Connection;
use crate::error::destination_context;
use crate::staging::Staging;
use crate::table::Table;

/// Replaces a MySQL `InnoDB` table atomically from Arrow batches.
pub struct MySqlDestination {
    connection: Connection,
    table: Table,
}

impl MySqlDestination {
    /// Validates connection and table syntax without connecting.
    ///
    /// # Errors
    /// Returns an error for invalid URLs, options or identifiers.
    pub fn new(dsn: &str, table: &str) -> Result<Self> {
        let connection = Connection::parse(dsn).map_err(TransferredError::in_destination)?;
        let resolved =
            Table::parse(table, connection.database()).map_err(TransferredError::in_destination)?;
        Ok(Self {
            connection,
            table: resolved,
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "keeps cleanup before publication separate from unknown publication outcomes"
    )]
    async fn transfer(&self, conn: &mut Conn, mut batches: BatchStream) -> Result<RunReport> {
        let started = Instant::now();
        let first = batches
            .try_next()
            .await?
            .ok_or(TransferredError::EmptySource)?;
        let encoder = Encoder::new(first.schema()).map_err(TransferredError::in_destination)?;
        let staging = Staging::create(
            conn,
            &self.connection,
            self.table.clone(),
            encoder.declarations(),
        )
        .await
        .map_err(TransferredError::in_destination)?;
        let input = stream::once(async move { Ok(first) })
            .chain(batches)
            .boxed();
        let loading = async {
            let writer = Writer::new(conn, &self.connection, encoder, &staging)
                .await
                .map_err(TransferredError::in_destination)?;
            let rows = writer.load(input).await?;
            staging
                .recheck(conn, &self.connection)
                .await
                .map_err(TransferredError::in_destination)?;
            Ok::<u64, TransferredError>(rows)
        }
        .await;
        let rows = match loading {
            Ok(rows) => rows,
            Err(error) => {
                staging.cleanup(conn, &self.connection).await;
                return Err(error);
            }
        };
        let table = staging
            .publish(conn, &self.connection)
            .await
            .map_err(TransferredError::in_destination)?;
        Ok(RunReport {
            rows,
            bytes_written: 0,
            written_objects: vec![table],
            duration: started.elapsed(),
        })
    }
}

#[async_trait]
impl Destination for MySqlDestination {
    async fn write_partitions(self: Box<Self>, partitions: Vec<BatchStream>) -> Result<RunReport> {
        let mut conn = self
            .connection
            .open()
            .await
            .map_err(TransferredError::in_destination)
            .map_err(|error| destination_context(error, &self.table.sql()))?;
        let report = self
            .transfer(&mut conn, stream::iter(partitions).flatten().boxed())
            .await;
        if let Err(error) = self.connection.wait(conn.disconnect()).await {
            warn!(target: "mysql.destination", "connection close failed: {error}");
        }
        report.map_err(|error| destination_context(error, &self.table.sql()))
    }
}
