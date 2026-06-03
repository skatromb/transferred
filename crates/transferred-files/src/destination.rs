use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::fs::File;
use tracing::warn;
use transferred_core::{BatchStream, Destination, RunReport, TransferredError};

use crate::compression::Compression;
use crate::format::FormatWrite;
use crate::parquet_codec::Parquet;

/// Local single-file Parquet destination. Writes via tmp + atomic rename.
#[derive(Debug, Clone)]
pub struct ParquetDestination {
    /// Output file path.
    pub path: PathBuf,
    /// Compression codec applied to column chunks.
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
    ) -> Result<RunReport, TransferredError> {
        let start = Instant::now();
        let tmp = tmp_path(&self.path);

        let rows = match write_via_codec(&tmp, self.compression, batches).await {
            Ok(rows) => rows,
            Err(err) => {
                cleanup_tmp(&tmp).await;
                return Err(err);
            }
        };

        if let Err(err) = tokio::fs::rename(&tmp, &self.path).await {
            cleanup_tmp(&tmp).await;
            return Err(TransferredError::from(err));
        }

        let bytes_written = tokio::fs::metadata(&self.path).await?.len();

        Ok(RunReport {
            rows,
            bytes_written,
            duration: start.elapsed(),
            coercions: vec![],
        })
    }
}

/// Flatten the partitions into one stream and hand them to the Parquet codec.
/// Schema is taken from the first batch; an empty source errors.
async fn write_via_codec(
    tmp: &Path,
    compression: Compression,
    batches: Vec<BatchStream>,
) -> Result<u64, TransferredError> {
    let stream: BatchStream = Box::pin(futures::stream::iter(batches).flatten());
    let file = File::create(tmp).await?;
    Parquet::new(compression, None)
        .write(Box::new(file), stream)
        .await
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
