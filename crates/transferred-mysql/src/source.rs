//! Streams one MySQL SELECT into bounded Arrow batches.

mod column_decoder;
mod decoder;

use async_stream::try_stream;
use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _};
use mysql_async::prelude::Queryable as _;
use mysql_async::{Conn, Row};
use transferred_core::{BatchStream, Result, Source, TransferredError};

use self::decoder::Decoder;
use crate::catalog;
use crate::column::Column;
use crate::connection::Connection;
use crate::error::source_context;
use crate::table::Table;

/// Reads a MySQL `InnoDB` table into Arrow batches.
pub struct MySqlSource {
    connection: Connection,
    table: Table,
}

impl MySqlSource {
    /// Validates connection and table syntax without connecting.
    ///
    /// # Errors
    /// Returns an error for invalid URLs, options or identifiers.
    pub fn new(dsn: &str, table: &str) -> Result<Self> {
        let connection = Connection::parse(dsn).map_err(TransferredError::in_source)?;
        let resolved =
            Table::parse(table, connection.database()).map_err(TransferredError::in_source)?;
        Ok(Self {
            connection,
            table: resolved,
        })
    }
}

#[async_trait]
impl Source for MySqlSource {
    async fn stream_partitions(mut self: Box<Self>) -> Result<Vec<BatchStream>> {
        let mut conn = self
            .connection
            .open()
            .await
            .map_err(TransferredError::in_source)
            .map_err(|error| source_context(error, &self.table.sql()))?;
        let columns = catalog::inspect(&mut conn, &self.connection, &mut self.table)
            .await
            .map_err(TransferredError::in_source)
            .map_err(|error| source_context(error, &self.table.sql()))?;
        Ok(vec![batches(conn, self.connection, self.table, columns)])
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keeps the streaming result borrowed until its trailing warnings are consumed"
)]
fn batches(
    mut conn: Conn,
    connection: Connection,
    table: Table,
    columns: Vec<Column>,
) -> BatchStream {
    let qualified = table.sql();
    try_stream! {
        let query = format!("SELECT {} FROM {}",
            columns.iter().map(Column::expression).collect::<Vec<_>>().join(","), table.sql());
        let mut response = connection.wait(conn.exec_iter(query, ())).await
            .map_err(TransferredError::in_source)?;
        check_columns(&columns, response.columns_ref())?;
        let mut rows = response.stream::<Row>().await.map_err(TransferredError::in_source)?
            .ok_or_else(|| TransferredError::in_source("SELECT returned no result set"))?;
        let mut decoder = Decoder::new(&columns);
        while let Some(row) = connection.wait(rows.try_next()).await.map_err(TransferredError::in_source)? {
            if let Some(batch) = decoder.append(&row).map_err(TransferredError::in_source)? {
                yield batch;
            }
        }
        drop(rows);
        drop(response);
        if conn.get_warnings() != 0 {
            Err(TransferredError::in_source(format!("SELECT from {} returned data warnings", table.sql())))?;
        }
        if let Some(batch) = decoder.finish().map_err(TransferredError::in_source)? {
            yield batch;
        }
    }.map_err(move |error| source_context(error, &qualified)).boxed()
}

fn check_columns(columns: &[Column], wire: &[mysql_async::Column]) -> Result<()> {
    if columns.len() != wire.len() {
        return Err(TransferredError::in_source(
            "SELECT columns changed after schema inspection",
        ));
    }
    for (column, metadata) in columns.iter().zip(wire) {
        column
            .check(metadata)
            .map_err(TransferredError::in_source)?;
    }
    Ok(())
}
