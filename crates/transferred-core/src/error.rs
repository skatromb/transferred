use std::error::Error as StdError;
use std::{io, result};

use arrow::error::ArrowError;
use thiserror::Error;

type AnyError = Box<dyn StdError + Send + Sync>;

/// Convenience alias for results returned by `transferred` operations.
pub type Result<T> = result::Result<T, TransferredError>;

/// Root error type. Every fallible operation in `transferred` returns `Result<T, TransferredError>`.
/// Maps to Python `transferred.TransferredError` at the FFI boundary.
#[derive(Debug, Error)]
pub enum TransferredError {
    /// A source connector failed to read or produce data.
    #[error("source error")]
    Source(#[source] AnyError),

    /// A destination connector failed to write or finalize output.
    #[error("destination error")]
    Destination(#[source] AnyError),

    /// Source produced zero batches (Python `EmptySourceError`).
    #[error("empty source: produced no batches")]
    EmptySource,

    /// Underlying I/O failure (filesystem, network).
    #[error("io error")]
    Io(#[from] io::Error),

    /// Arrow compute or schema error surfaced from the data layer.
    #[error("arrow error")]
    Arrow(#[from] ArrowError),
}

impl TransferredError {
    /// Constructs a [`TransferredError::Source`] from any error.
    pub fn in_source(err: impl Into<AnyError>) -> Self {
        Self::Source(err.into())
    }

    /// Constructs a [`TransferredError::Destination`] from any message.
    pub fn in_destination(err: impl Into<AnyError>) -> Self {
        Self::Destination(err.into())
    }
}
