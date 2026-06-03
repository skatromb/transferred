use std::path::PathBuf;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;
use transferred_core::{BatchStream, Source, TransferredError};

/// Local Parquet source. One or many files, via glob pattern or explicit paths.
#[derive(Debug, Clone)]
pub struct ParquetSource {
    source: GlobOrPaths,
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

impl ParquetSource {
    /// Build a source. No I/O performed.
    #[must_use]
    pub fn new(source: GlobOrPaths) -> Self {
        Self { source }
    }
}

#[async_trait]
impl Source for ParquetSource {
    /// One stream per file. Glob patterns expanded here; empty matches error.
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>, TransferredError> {
        let paths = match self.source {
            GlobOrPaths::Paths(paths) => paths,
            GlobOrPaths::Glob(pattern) => expand_glob(&pattern)?,
        };

        Ok(paths.into_iter().map(lazy_open_file).collect())
    }
}

/// Keep files opening lazy so that only opened files has file descriptors.
fn lazy_open_file(path: PathBuf) -> BatchStream {
    Box::pin(stream::once(open_file_stream(path)).try_flatten())
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

async fn open_file_stream(path: PathBuf) -> Result<BatchStream, TransferredError> {
    let file = File::open(&path).await?;
    let display = path.display().to_string();
    let stream = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .map_err(|err| TransferredError::source(format!("parquet reader init ({display}): {err}")))?
        .build()
        .map_err(|err| {
            TransferredError::source(format!("parquet reader build ({display}): {err}"))
        })?
        .map(move |result| {
            result.map_err(|e| TransferredError::source(format!("parquet read ({display}): {e}")))
        });

    Ok(Box::pin(stream))
}
