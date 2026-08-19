use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use tokio_postgres::binary_copy::{BinaryCopyOutRow, BinaryCopyOutStream};
use transferred_core::{BatchStream, Result, Source, TransferredError};

use crate::connection::connect;
use crate::pg_to_arrow::Decoder;

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

        let decoder = Decoder::derive(query.columns())?;
        let pg_types: Vec<_> = query
            .columns()
            .iter()
            .map(|column| column.type_().clone())
            .collect();

        let copy = client
            .copy_out(&format!(
                "copy (select * from {verified_table}) to stdout (format binary)"
            ))
            .await
            .map_err(TransferredError::source)?;
        // The stream is not fused: polled once more after its trailer, it reports the transport
        // closed rather than ending again.
        let rows = BinaryCopyOutStream::new(copy, &pg_types)
            .map_err(TransferredError::source)
            .fuse();

        // No rows means the stream ended; a last batch short of `BATCH_ROWS` still has some.
        let batches = stream::try_unfold(
            (Box::pin(rows), decoder),
            |(mut rows, mut decoder)| async move {
                match read_batch(&mut rows, &mut decoder).await? {
                    0 => Ok(None),
                    _ => Ok(Some((decoder.finish()?, (rows, decoder)))),
                }
            },
        );

        Ok(vec![batches.boxed()])
    }
}

/// Fills `decoder` with up to `BATCH_ROWS` rows. Returns how many arrived; zero means the end.
async fn read_batch(
    rows: &mut (impl Stream<Item = Result<BinaryCopyOutRow>> + Unpin),
    decoder: &mut Decoder,
) -> Result<usize> {
    let mut read = 0;

    while read < BATCH_ROWS {
        let Some(row) = rows.try_next().await? else {
            break;
        };
        decoder.append_row(&row)?;
        read += 1;
    }

    Ok(read)
}
