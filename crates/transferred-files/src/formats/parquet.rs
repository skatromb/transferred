//! Parquet codec — `FormatRead` + `FormatWrite` over the arrow-rs `parquet` crate.

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use transferred_core::{BatchStream, TransferredError};
// Leading `::` selects the extern `parquet` crate, not this `formats::parquet` module.
use ::parquet::arrow::AsyncArrowWriter;
use ::parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use ::parquet::file::properties::WriterProperties;

use super::{FileReader, FileWriter, FormatRead, FormatWrite};
use parquet::basic::{Compression as ParquetCompression, ZstdLevel};

/// Compression codec for Parquet column chunks. Default = `Zstd`.
#[derive(Debug, Clone, Copy, Default)]
pub enum Compression {
    /// Zstandard, default level.
    #[default]
    Zstd,
    /// Snappy.
    Snappy,
    /// No compression.
    None,
}

impl From<Compression> for ParquetCompression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::Zstd => ParquetCompression::ZSTD(ZstdLevel::default()),
            Compression::Snappy => ParquetCompression::SNAPPY,
            Compression::None => ParquetCompression::UNCOMPRESSED,
        }
    }
}

/// Parquet file format. Carries encoder knobs; decoding needs none.
#[derive(Debug, Clone, Default)]
pub struct Parquet {
    /// Compression codec for column chunks.
    pub compression: Compression,
    /// Max rows per row group. `None` keeps the parquet-rs default (1,048,576).
    pub row_group_size: Option<usize>,
}

impl Parquet {
    /// Build a Parquet codec.
    #[must_use]
    pub fn new(compression: Compression, row_group_size: Option<usize>) -> Self {
        Self {
            compression,
            row_group_size,
        }
    }

    /// Writer properties. `row_group_size = None` leaves the parquet-rs default
    /// (never forwards `None` to `set_max_row_group_row_count`, which means *unlimited*).
    fn writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder().set_compression(self.compression.into());
        if let Some(rows) = self.row_group_size {
            builder = builder.set_max_row_group_row_count(Some(rows));
        }
        builder.build()
    }
}

#[async_trait]
impl FormatRead for Parquet {
    async fn read(&self, reader: Box<dyn FileReader>) -> Result<BatchStream, TransferredError> {
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| TransferredError::source(format!("parquet reader init: {e}")))?
            .build()
            .map_err(|e| TransferredError::source(format!("parquet reader build: {e}")))?
            .map(|result| {
                result.map_err(|e| TransferredError::source(format!("parquet read: {e}")))
            });
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl FormatWrite for Parquet {
    fn file_extension(&self) -> &'static str {
        "parquet"
    }

    async fn write(
        &self,
        writer: Box<dyn FileWriter>,
        mut batches: BatchStream,
    ) -> Result<u64, TransferredError> {
        let first = batches
            .try_next()
            .await?
            .ok_or_else(|| TransferredError::source("source produced no batches"))?;

        let mut arrow_writer =
            AsyncArrowWriter::try_new(writer, first.schema(), Some(self.writer_properties()))
                .map_err(|e| {
                    TransferredError::destination(format!("AsyncArrowWriter init: {e}"))
                })?;

        let mut rows = first.num_rows() as u64;
        arrow_writer
            .write(&first)
            .await
            .map_err(|e| TransferredError::destination(format!("AsyncArrowWriter::write: {e}")))?;

        while let Some(batch) = batches.try_next().await? {
            rows += batch.num_rows() as u64;
            arrow_writer.write(&batch).await.map_err(|e| {
                TransferredError::destination(format!("AsyncArrowWriter::write: {e}"))
            })?;
        }

        arrow_writer
            .close()
            .await
            .map_err(|e| TransferredError::destination(format!("AsyncArrowWriter::close: {e}")))?;
        Ok(rows)
    }
}
