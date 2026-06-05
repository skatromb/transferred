use thiserror::Error;

/// Root error type. Every fallible operation in `transferred` returns `Result<T, TransferredError>`.
/// Maps to Python `transferred.TransferredError` at the FFI boundary.
#[derive(Debug, Error)]
pub enum TransferredError {
    /// A source connector failed to read or produce data.
    #[error("source error: {0}")]
    Source(String),

    /// Source produced zero batches (Python `EmptySourceError`).
    #[error("empty source: produced no batches")]
    EmptySource,

    /// A destination connector failed to write or finalize output.
    #[error("destination error: {0}")]
    Destination(String),

    /// Underlying I/O failure (filesystem, network).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Arrow compute or schema error surfaced from the data layer.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Any error that does not fit the other variants.
    #[error("{0}")]
    Other(String),
}

impl TransferredError {
    /// Construct a [`TransferredError::Source`] from any message.
    pub fn source<S: Into<String>>(msg: S) -> Self {
        Self::Source(msg.into())
    }

    /// Construct a [`TransferredError::Destination`] from any message.
    pub fn destination<S: Into<String>>(msg: S) -> Self {
        Self::Destination(msg.into())
    }

    /// Construct a [`TransferredError::Other`] from any message.
    pub fn other<S: Into<String>>(msg: S) -> Self {
        Self::Other(msg.into())
    }
}
