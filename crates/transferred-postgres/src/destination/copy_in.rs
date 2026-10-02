//! Postgres binary COPY: the wire format the destination pushes rows through.

use std::pin::Pin;

use arrow::array::RecordBatch;
use bytes::{BufMut as _, Bytes, BytesMut};
use futures::SinkExt as _;
use tokio_postgres::{Client, CopyInSink};
use transferred_core::{Result, TransferredError};

use super::arrow_to_pg::Encoder;

/// Bytes every binary COPY stream starts with, before the flags and header extension.
const COPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// Field count that ends the rows.
const COPY_TRAILER: i16 = -1;

/// Bytes buffered before a chunk goes out; 4 KB costs a third more client CPU, 64 KB is the plateau.
const CHUNK_BYTES: usize = 64 << 10;

/// Writes rows a chunk at a time, unlike `BinaryCopyInWriter`, which boxes every value.
pub(crate) struct CopyIn {
    sink: Pin<Box<CopyInSink<Bytes>>>,
    buf: BytesMut,
}

impl CopyIn {
    /// Opens a COPY into `table`. The header goes out with the first chunk.
    pub(crate) async fn open(client: &Client, table: &str) -> Result<Self> {
        let sink = client
            .copy_in(&format!("copy {table} from stdin (format binary)"))
            .await
            .map_err(TransferredError::in_destination)?;

        // `bytes` restores this capacity after every `split`, so a chunk is one allocation.
        let mut buf = BytesMut::with_capacity(CHUNK_BYTES);
        buf.put_slice(COPY_SIGNATURE);
        buf.put_i32(0); // no flags
        buf.put_i32(0); // no header extension

        Ok(Self {
            sink: Box::pin(sink),
            buf,
        })
    }

    /// Writes one COPY row per Arrow row, sending whenever the buffer fills.
    pub(crate) async fn write_batch(&mut self, encoder: &Encoder, batch: &RecordBatch) -> Result<()> {
        encoder.check(batch)?;

        for row_num in 0..batch.num_rows() {
            encoder.write_row(batch, row_num, &mut self.buf)?;

            if self.buf.len() >= CHUNK_BYTES {
                self.send().await?;
            }
        }

        Ok(())
    }

    /// Writes the trailer, closes the stream and returns the rows Postgres took.
    pub(crate) async fn finish(mut self) -> Result<u64> {
        self.buf.put_i16(COPY_TRAILER);
        self.send().await?;

        self.sink
            .as_mut()
            .finish()
            .await
            .map_err(TransferredError::in_destination)
    }

    /// Sends the buffered bytes and empties the buffer.
    async fn send(&mut self) -> Result<()> {
        self.sink
            .send(self.buf.split().freeze())
            .await
            .map_err(TransferredError::in_destination)
    }
}
