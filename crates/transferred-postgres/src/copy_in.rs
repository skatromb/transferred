//! Postgres binary COPY: the wire format the destination pushes rows through.

use std::pin::Pin;

use arrow::array::{Array, RecordBatch};
use bytes::{BufMut, Bytes, BytesMut};
use futures::SinkExt;
use tokio_postgres::types::IsNull;
use tokio_postgres::{Client, CopyInSink};
use transferred_core::{Result, TransferredError};

use crate::arrow_to_pg::{ColumnEncoder, Encoder};

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

/// Writes rows a chunk at a time, unlike `BinaryCopyInWriter`, which boxes every value.
pub struct CopyIn {
    sink: Pin<Box<CopyInSink<Bytes>>>,
    buf: BytesMut,
}

impl CopyIn {
    /// Opens a COPY into `table`. The header goes out with the first chunk.
    pub async fn open(client: &Client, table: &str) -> Result<Self> {
        let sink = client
            .copy_in(&format!("copy {table} from stdin (format binary)"))
            .await
            .map_err(TransferredError::destination)?;

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
    pub async fn write_batch(&mut self, encoder: &Encoder, batch: &RecordBatch) -> Result<()> {
        let columns = encoder.columns(batch)?;
        let fields_count = i16::try_from(columns.len()).map_err(|_| {
            TransferredError::destination(format!(
                "a COPY row holds at most {} columns, not {}",
                i16::MAX,
                columns.len()
            ))
        })?;

        for row_num in 0..batch.num_rows() {
            self.buf.put_i16(fields_count);
            for (encoder, array) in columns.iter().zip(batch.columns()) {
                self.push_field(encoder, array.as_ref(), row_num)?;
            }

            if self.buf.len() >= CHUNK_BYTES {
                self.send().await?;
            }
        }

        Ok(())
    }

    /// Appends one field: its length, then its bytes.
    fn push_field(
        &mut self,
        encoder: &ColumnEncoder,
        array: &dyn Array,
        row_num: usize,
    ) -> Result<()> {
        // The length is only known once the value is written, so leave a hole and come back.
        let start_at = self.buf.len();
        self.buf.put_i32(0);

        let len = match encoder.write(array, row_num, &mut self.buf)? {
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
    pub async fn finish(mut self) -> Result<u64> {
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
