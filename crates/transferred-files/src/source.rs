use std::path::PathBuf;
use std::result;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{TryStreamExt as _, stream};
use tokio::fs::File;
use transferred_core::{BatchStream, Result, Source, TransferredError};

use crate::formats::FormatRead;

/// Local file source. One or many files, decoded by the supplied `FormatRead`.
#[derive(Clone)]
pub struct FilesSource {
    paths: GlobOrPaths,
    format: Arc<dyn FormatRead>,
}

impl FilesSource {
    /// Builds a source. No I/O performed.
    #[must_use]
    pub fn new(paths: GlobOrPaths, format: Arc<dyn FormatRead>) -> Self {
        Self { paths, format }
    }
}

#[async_trait]
impl Source for FilesSource {
    /// One stream per file. Globs expanded here; empty results error.
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
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
    /// Resolves to concrete paths. Glob walks the filesystem; empty results error.
    fn resolve(self) -> Result<Vec<PathBuf>> {
        let paths = match self {
            Self::Glob(pattern) => expand_glob(&pattern)?,
            Self::Paths(paths) if paths.is_empty() => {
                return Err(TransferredError::in_source("no input paths provided"));
            }
            Self::Paths(paths) => paths,
        };
        if let Some(dir) = paths.iter().find(|path| path.is_dir()) {
            return Err(TransferredError::in_source(format!(
                concat!(
                    "{} is a directory, not a file. ",
                    r#"Pass a list of filenames or glob pattern ("directory/*.parquet")"#
                ),
                dir.display()
            )));
        }
        Ok(paths)
    }
}

/// Expands a glob pattern to matching paths. Empty matches error.
fn expand_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    let paths: Vec<PathBuf> = glob::glob(pattern)
        .map_err(|err| {
            TransferredError::in_source(format!("invalid glob pattern '{pattern}': {err}"))
        })?
        .collect::<result::Result<Vec<_>, _>>()
        .map_err(|err| TransferredError::in_source(format!("glob walk error: {err}")))?;

    if paths.is_empty() {
        return Err(TransferredError::in_source(format!(
            "glob '{pattern}' matched no files"
        )));
    }

    Ok(paths)
}

/// Keeps file opening lazy, so that only opened files hold file descriptors.
fn lazy_open_file(path: PathBuf, format: Arc<dyn FormatRead>) -> BatchStream {
    Box::pin(stream::once(open_file_stream(path, format)).try_flatten())
}

async fn open_file_stream(path: PathBuf, format: Arc<dyn FormatRead>) -> Result<BatchStream> {
    let file = File::open(&path).await?;
    format.read(Box::new(file)).await
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use crate::Parquet;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn directory_path_errors_with_clear_message() {
        let dir = tempdir().unwrap();
        let glob_dir = GlobOrPaths::Glob(dir.path().to_string_lossy().into_owned());
        let paths_dir = GlobOrPaths::Paths(vec![dir.path().to_path_buf()]);

        for paths in [glob_dir, paths_dir] {
            let source = FilesSource::new(paths, Arc::new(Parquet::default()));
            let err = Box::new(source).stream_partitions().await.err().unwrap();
            let cause = err.source().unwrap().to_string();
            assert!(cause.contains("is a directory, not a file"));
        }
    }
}
