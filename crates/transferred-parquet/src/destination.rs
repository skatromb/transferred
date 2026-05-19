use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio::fs::File;
use tracing::warn;
use transferred_core::{BatchStream, Destination, ElError, RunReport};

use crate::compression::Compression;

/// Local single-file Parquet destination. Writes via tmp + atomic rename.
#[derive(Debug, Clone)]
pub struct ParquetDestination {
    pub path: PathBuf,
    pub compression: Compression,
}

impl ParquetDestination {
    /// Build a destination. No I/O performed.
    #[must_use]
    pub fn new(path: PathBuf, compression: Compression) -> Self {
        Self { path, compression }
    }
}

#[async_trait]
impl Destination for ParquetDestination {
    async fn write_partitions(
        self: Box<Self>,
        batches: Vec<BatchStream>,
    ) -> Result<RunReport, ElError> {
        let start = Instant::now();
        let tmp = tmp_path(&self.path);

        let (rows, bytes_written) = match write_all(&tmp, self.compression, batches).await {
            Ok(stats) => stats,
            Err(err) => {
                cleanup_tmp(&tmp).await;
                return Err(err);
            }
        };

        if let Err(err) = tokio::fs::rename(&tmp, &self.path).await {
            cleanup_tmp(&tmp).await;
            return Err(ElError::from(err));
        }

        Ok(RunReport {
            rows,
            bytes_written,
            duration: start.elapsed(),
            coercions: vec![],
        })
    }
}

/// Currently supports only sequential partitions. Writer schema is taken from
/// the first batch; an empty source errors.
async fn write_all(
    tmp: &Path,
    compression: Compression,
    batches: Vec<BatchStream>,
) -> Result<(u64, u64), ElError> {
    let mut stream = futures::stream::iter(batches).flatten();

    let first = stream
        .try_next()
        .await?
        .ok_or_else(|| ElError::source("source produced no batches"))?;

    let file = File::create(tmp).await?;
    let props = WriterProperties::builder()
        .set_compression(compression.into())
        .build();
    let mut writer = AsyncArrowWriter::try_new(file, first.schema(), Some(props))
        .map_err(|e| ElError::destination(format!("AsyncArrowWriter init: {e}")))?;

    let mut rows = first.num_rows() as u64;
    writer
        .write(&first)
        .await
        .map_err(|e| ElError::destination(format!("AsyncArrowWriter::write: {e}")))?;

    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows() as u64;
        writer
            .write(&batch)
            .await
            .map_err(|e| ElError::destination(format!("AsyncArrowWriter::write: {e}")))?;
    }

    writer
        .close()
        .await
        .map_err(|e| ElError::destination(format!("AsyncArrowWriter::close: {e}")))?;
    let bytes = tokio::fs::metadata(tmp).await?.len();

    Ok((rows, bytes))
}

async fn cleanup_tmp(tmp: &Path) {
    if let Err(err) = tokio::fs::remove_file(tmp).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %tmp.display(), error = %err, "failed to remove tmp parquet file");
    }
}

fn tmp_path(final_path: &Path) -> PathBuf {
    let mut name = final_path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(".tmp");
    final_path.with_file_name(name)
}
