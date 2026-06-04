use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::fs::File;
use tracing::warn;
use transferred_core::{BatchStream, Destination, RunReport, TransferredError};

use crate::formats::FormatWrite;

/// Local single-file destination. Writes via tmp + atomic rename.
/// Bytes are encoded by the supplied [`FormatWrite`] codec.
#[derive(Clone)]
pub struct FilesDestination {
    /// Output file path.
    pub path: PathBuf,
    format: Arc<dyn FormatWrite>,
}

impl FilesDestination {
    /// Build a destination. No I/O performed.
    #[must_use]
    pub fn new(path: PathBuf, format: Arc<dyn FormatWrite>) -> Self {
        Self { path, format }
    }
}

#[async_trait]
impl Destination for FilesDestination {
    async fn write_partitions(
        self: Box<Self>,
        batches: Vec<BatchStream>,
    ) -> Result<RunReport, TransferredError> {
        let start = Instant::now();
        let tmp = tmp_path(&self.path);

        let rows = match write_via_codec(&tmp, &*self.format, batches).await {
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

/// Flatten the partitions into one stream and hand them to the format codec.
/// Schema is taken from the first batch; an empty source errors.
async fn write_via_codec(
    tmp: &Path,
    format: &dyn FormatWrite,
    batches: Vec<BatchStream>,
) -> Result<u64, TransferredError> {
    let stream: BatchStream = Box::pin(futures::stream::iter(batches).flatten());
    let file = File::create(tmp).await?;
    format.write(Box::new(file), stream).await
}

async fn cleanup_tmp(tmp: &Path) {
    if let Err(err) = tokio::fs::remove_file(tmp).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %tmp.display(), error = %err, "failed to remove tmp file");
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
