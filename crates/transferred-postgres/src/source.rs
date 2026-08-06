use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use tokio_postgres::{self, NoTls, binary_copy::BinaryCopyOutStream};
use transferred_core::{BatchStream, Result, Source, TransferredError};

use crate::pg_to_arrow::PgToArrow;

const BATCH_ROWS: usize = 10_000;

/// A `Source` that reads rows from a Postgres table or query.
pub struct PostgresSource {
    /// Postgres connection string.
    pub dsn: String,
    /// Table to transfer.
    pub table: String,
}

impl PostgresSource {
    /// Construct a `PostgresSource`
    #[must_use]
    pub fn new(dsn: String, table: String) -> Self {
        Self { dsn, table }
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        let (client, connection) = tokio_postgres::connect(&self.dsn, NoTls)
            .await
            .map_err(TransferredError::source)?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("connection error: {error}");
            }
        });

        let verified_table: String = client
            .query_one("SELECT $1::text::regclass::text", &[&self.table])
            .await
            .map_err(TransferredError::source)?
            .get(0);

        let query = client
            .prepare(&format!("select * from {verified_table}"))
            .await
            .map_err(TransferredError::source)?;

        let columns = query.columns();
        let pg_types: Vec<_> = columns
            .iter()
            .map(|column| column.type_().clone())
            .collect();

        let pg_to_arrow = PgToArrow::derive(columns)?;

        let sql = format!("copy (select * from {verified_table}) to stdout (format binary)");
        let stream = BinaryCopyOutStream::new(
            client
                .copy_out(&sql)
                .await
                .map_err(TransferredError::source)?,
            &pg_types,
        );

        let batches = stream.try_chunks(BATCH_ROWS).map(move |chunk| {
            let chunk = chunk.map_err(TransferredError::source)?;
            pg_to_arrow.batch(&chunk)
        });

        Ok(vec![batches.boxed()])
    }
}
