use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{TryStreamExt, stream};
use tokio::fs::File;
use transferred_core::{BatchStream, Source, TransferredError};

use crate::formats::FormatRead;

/// Local file source. One or many files, via glob pattern or explicit paths.
/// Bytes are decoded by the supplied [`FormatRead`] codec.
#[derive(Clone)]
pub struct FilesSource {
    source: GlobOrPaths,
    format: Arc<dyn FormatRead>,
}

/// How the source enumerates files: glob or single path, or list of paths.
#[derive(Debug, Clone)]
pub enum GlobOrPaths {
    /// Pattern (e.g. `data/*.parquet`). Expanded at `stream_partitions` time;
    /// A pattern with no wildcards matches the literal path.
    Glob(String),
    /// Explicit paths: one or multiple. No per-item glob expansion.
    Paths(Vec<PathBuf>),
}

impl FilesSource {
    /// Build a source. No I/O performed.
    #[must_use]
    pub fn new(source: GlobOrPaths, format: Arc<dyn FormatRead>) -> Self {
        Self { source, format }
    }
}

#[async_trait]
impl Source for FilesSource {
    /// One stream per file. Glob patterns expanded here; empty matches error.
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>, TransferredError> {
        let paths = match self.source {
            GlobOrPaths::Paths(paths) => paths,
            GlobOrPaths::Glob(pattern) => expand_glob(&pattern)?,
        };

        let format = self.format;
        Ok(paths
            .into_iter()
            .map(|path| lazy_open_file(path, format.clone()))
            .collect())
    }
}

/// Keep files opening lazy so that only opened files has file descriptors.
fn lazy_open_file(path: PathBuf, format: Arc<dyn FormatRead>) -> BatchStream {
    Box::pin(stream::once(open_file_stream(path, format)).try_flatten())
}

fn expand_glob(pattern: &str) -> Result<Vec<PathBuf>, TransferredError> {
    let paths: Vec<PathBuf> = glob::glob(pattern)
        .map_err(|err| {
            TransferredError::source(format!("invalid glob pattern '{pattern}': {err}"))
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| TransferredError::source(format!("glob walk error: {err}")))?;

    if paths.is_empty() {
        return Err(TransferredError::source(format!(
            "glob '{pattern}' matched no files"
        )));
    }
    Ok(paths)
}

async fn open_file_stream(
    path: PathBuf,
    format: Arc<dyn FormatRead>,
) -> Result<BatchStream, TransferredError> {
    let file = File::open(&path).await?;
    format.read(Box::new(file)).await
}
