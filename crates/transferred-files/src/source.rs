use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{TryStreamExt, stream};
use tokio::fs::File;
use transferred_core::{BatchStream, Source, TransferredError};

use crate::formats::FormatRead;

/// Local file source. One or many files, decoded by the supplied `FormatRead`.
#[derive(Clone)]
pub struct FilesSource {
    paths: GlobOrPaths,
    format: Arc<dyn FormatRead>,
}

impl FilesSource {
    /// Build a source. No I/O performed.
    #[must_use]
    pub fn new(paths: GlobOrPaths, format: Arc<dyn FormatRead>) -> Self {
        Self { paths, format }
    }
}

#[async_trait]
impl Source for FilesSource {
    /// One stream per file. Globs expanded here; empty results error.
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>, TransferredError> {
        let paths = self.paths.resolve()?;
        let format = self.format;
        Ok(paths
            .into_iter()
            .map(|path| lazy_open_file(path, format.clone()))
            .collect())
    }
}

/// How the source enumerates files: glob or single path, or list of paths.
#[derive(Debug, Clone)]
pub enum GlobOrPaths {
    /// Pattern (e.g. `data/*.parquet`). Expanded at `stream_partitions` time.
    Glob(String),
    /// Explicit paths. No per-item glob expansion.
    Paths(Vec<PathBuf>),
}

impl GlobOrPaths {
    /// Resolve to concrete paths. Glob walks the filesystem; empty results error.
    fn resolve(self) -> Result<Vec<PathBuf>, TransferredError> {
        match self {
            GlobOrPaths::Glob(pattern) => expand_glob(&pattern),
            GlobOrPaths::Paths(paths) if paths.is_empty() => {
                Err(TransferredError::source("no input paths provided"))
            }
            GlobOrPaths::Paths(paths) => Ok(paths),
        }
    }
}

/// Expand a glob pattern to matching paths. Empty matches error.
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

/// Keep files opening lazy so that only opened files has file descriptors.
fn lazy_open_file(path: PathBuf, format: Arc<dyn FormatRead>) -> BatchStream {
    Box::pin(stream::once(open_file_stream(path, format)).try_flatten())
}

async fn open_file_stream(
    path: PathBuf,
    format: Arc<dyn FormatRead>,
) -> Result<BatchStream, TransferredError> {
    let file = File::open(&path).await?;
    format.read(Box::new(file)).await
}
