use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use transferred_core::{BatchStream, Result, Source, TransferredError};

use crate::connection::connect;
use crate::pg_to_arrow::Decoder;

/// Rows per Arrow batch, one of which the transfer holds in flight.
const BATCH_ROWS: usize = 10_000;

/// A `Source` that reads rows from a Postgres table or query.
pub struct PostgresSource {
    /// Postgres connection string.
    pub dsn: String,
    /// Table to transfer.
    pub table: String,
}

impl PostgresSource {
    /// Constructs a `PostgresSource`
    #[must_use]
    pub fn new(dsn: String, table: String) -> Self {
        Self { dsn, table }
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        let client = connect(&self.dsn).await.map_err(TransferredError::source)?;

        let verified_table: String = client
            .query_one("SELECT $1::text::regclass::text", &[&self.table])
            .await
            .map_err(TransferredError::source)?
            .get(0);

        let query = client
            .prepare(&format!("select * from {verified_table}"))
            .await
            .map_err(TransferredError::source)?;

        let types: Vec<_> = query
            .columns()
            .iter()
            .map(|column| column.type_().clone())
            .collect();
        let mut decoder = Decoder::derive(query.columns())?;

        let copy = client
            .copy_out(&format!(
                "copy (select * from {verified_table}) to stdout (format binary)"
            ))
            .await
            .map_err(TransferredError::source)?;

        let batches = BinaryCopyOutStream::new(copy, &types)
            .try_chunks(BATCH_ROWS)
            .map(move |rows| {
                for row in rows.map_err(|failed| TransferredError::source(failed.1))? {
                    decoder.append_row(&row)?;
                }

                decoder.finish()
            });

        Ok(vec![batches.boxed()])
    }
}
