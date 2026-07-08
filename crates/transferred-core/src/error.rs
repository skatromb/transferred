use thiserror::Error;

type AnyError = Box<dyn std::error::Error + Send + Sync>;

/// Convenience alias for results returned by `transferred` operations.
pub type Result<T> = std::result::Result<T, TransferredError>;

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
    Io(#[from] std::io::Error),

    /// Arrow compute or schema error surfaced from the data layer.
    #[error("arrow error")]
    Arrow(#[from] arrow::error::ArrowError),
}

impl TransferredError {
    /// Construct a [`TransferredError::Source`] from any error.
    pub fn source(err: impl Into<AnyError>) -> Self {
        Self::Source(err.into())
    }

    /// Construct a [`TransferredError::Destination`] from any message.
    pub fn destination(err: impl Into<AnyError>) -> Self {
        Self::Destination(err.into())
    }
}
