//! Postgres source. Binary COPY OUT → Arrow `RecordBatch`.

mod pg_to_arrow;

use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _};
use tokio_postgres::Client;
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use transferred_core::{BatchStream, Result, Source, TransferredError};

use self::pg_to_arrow::Decoder;
use crate::connection::connect;

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
    /// Constructs a `PostgresSource`.
    #[must_use]
    pub const fn new(dsn: String, table: String) -> Self {
        Self { dsn, table }
    }
}

#[async_trait]
impl Source for PostgresSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        let client = connect(&self.dsn)
            .await
            .map_err(TransferredError::in_source)?;
        let table = verified_table(&client, &self.table).await?;

        Ok(vec![batches(&client, &table).await?])
    }
}

/// The table's name as Postgres resolves and quotes it, so anything but a table fails here.
async fn verified_table(client: &Client, table: &str) -> Result<String> {
    Ok(client
        .query_one("SELECT $1::text::regclass::text", &[&table])
        .await
        .map_err(TransferredError::in_source)?
        .get(0))
}

/// Streams `table` out over binary COPY, decoded into Arrow a batch at a time.
async fn batches(client: &Client, table: &str) -> Result<BatchStream> {
    let query = client
        .prepare(&format!("select * from {table}"))
        .await
        .map_err(TransferredError::in_source)?;

    let types: Vec<_> = query
        .columns()
        .iter()
        .map(|column| column.type_().clone())
        .collect();
    let decoder = Decoder::derive(query.columns())?;

    let copy = client
        .copy_out(&format!(
            "copy (select * from {table}) to stdout (format binary)"
        ))
        .await
        .map_err(TransferredError::in_source)?;

    Ok(BinaryCopyOutStream::new(copy, &types)
        .try_chunks(BATCH_ROWS)
        .map(move |chunk| {
            let rows = chunk.map_err(|failed| TransferredError::in_source(failed.1))?;
            decoder.decode(&rows)
        })
        .boxed())
}
