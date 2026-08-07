use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt;
use tokio::fs::File;
use tracing::warn;
use transferred_core::{BatchStream, Destination, Result, RunReport, TransferredError};

use crate::formats::FormatWrite;

/// Local files destination. Writes `part-NNNNN.{extension}` files to a directory,
/// or one `{dir}.{extension}` when `single_file`.
/// Write is atomic via tmp dir + rename.
/// Written paths land in `RunReport.written_objects`.
#[derive(Clone)]
pub struct FilesDestination {
    path: PathBuf,
    format: Arc<dyn FormatWrite>,
    single_file: bool,
}

#[async_trait]
impl Destination for FilesDestination {
    async fn write_partitions(self: Box<Self>, partitions: Vec<BatchStream>) -> Result<RunReport> {
        let start = Instant::now();
        let tmp_dir = make_tmp(&self.path);
        tokio::fs::create_dir_all(&tmp_dir).await?;

        let writtens = match self.write_files(&tmp_dir, partitions).await {
            Ok(written) => written,
            Err(err) => {
                cleanup(&tmp_dir).await;
                return Err(err);
            }
        };

        if let Err(err) = self.atomic_replace(&tmp_dir).await {
            cleanup(&tmp_dir).await;
            return Err(err);
        }

        let mut bytes_written = 0;
        for written in &writtens {
            bytes_written += tokio::fs::metadata(&written.path).await?.len();
        }

        Ok(RunReport {
            rows: writtens.iter().map(|w| w.rows).sum(),
            bytes_written,
            written_objects: writtens
                .iter()
                .map(|w| w.path.display().to_string())
                .collect(),
            duration: start.elapsed(),
            coercions: vec![],
        })
    }
}

impl FilesDestination {
    /// Build a destination. No I/O performed.
    #[must_use]
    pub fn new(path: PathBuf, format: Arc<dyn FormatWrite>, single_file: bool) -> Self {
        Self {
            path,
            format,
            single_file,
        }
    }

    /// Pick a filename: `{dir}.{ext}` if `single_file`, else `part-NNNNN.{ext}`.
    fn output_filename(&self, part: usize) -> String {
        let ext = self.format.file_extension();
        let base_name = self
            .path
            .file_name()
            .map_or("data".to_string(), |n| n.to_string_lossy().to_string());

        if self.single_file {
            format!("{base_name}.{ext}")
        } else {
            format!("part-{part:05}.{ext}")
        }
    }

    /// Create and write files into `tmp_dir`; returns the final paths and row counts.
    async fn write_files(
        &self,
        tmp_dir: &Path,
        partitions: Vec<BatchStream>,
    ) -> Result<Vec<Written>> {
        let streams: Vec<BatchStream> = if self.single_file {
            vec![Box::pin(futures::stream::iter(partitions).flatten())]
        } else {
            partitions
        };

        let mut written = Vec::new();
        for stream in streams {
            let mut stream = stream.peekable();
            if Pin::new(&mut stream).peek().await.is_none() {
                continue; // skip empty partitions — no stray part file
            }

            let name = self.output_filename(written.len() + 1);
            let file = File::create(tmp_dir.join(&name)).await?;

            let rows = self.format.write(Box::new(file), Box::pin(stream)).await?;
            written.push(Written {
                path: self.path.join(&name),
                rows,
            });
        }

        if written.is_empty() {
            return Err(TransferredError::EmptySource);
        }

        Ok(written)
    }

    /// Atomically overwrite `path` dir with `tmp_dir`, removing any existing output first.
    async fn atomic_replace(&self, tmp_dir: &Path) -> Result<()> {
        match tokio::fs::metadata(&self.path).await {
            Ok(meta) if meta.is_dir() => tokio::fs::remove_dir_all(&self.path).await?,
            Ok(_) => tokio::fs::remove_file(&self.path).await?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }

        tokio::fs::rename(tmp_dir, &self.path).await?;

        Ok(())
    }
}

/// A file the destination produced.
struct Written {
    path: PathBuf,
    rows: u64,
}

/// Remove a leftover tmp directory, logging non-`NotFound` failures.
async fn cleanup(tmp_dir: &Path) {
    if let Err(err) = tokio::fs::remove_dir_all(tmp_dir).await
        && err.kind() != std::io::ErrorKind::NotFound
    {
        warn!(target: "files::destination", path = %tmp_dir.display(), error = %err, "failed to remove tmp dir");
    }
}

/// Create `{name}.tmp` near the `final_path`, for staging files.
fn make_tmp(final_path: &Path) -> PathBuf {
    let mut name = final_path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(".tmp");

    final_path.with_file_name(name)
}
