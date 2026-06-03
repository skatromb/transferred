//! File-format codec seam. A format converts between a file's byte stream and
//! Arrow batches; `Files` owns opening the file and hands over the byte handle.

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use transferred_core::{BatchStream, TransferredError};

/// A readable file handle — random-access bytes. `Files` opens it; a format decodes it.
/// Seekability is part of what a file *is* (footers, metadata); sequential formats simply
/// don't exercise it.
pub trait FileReader: AsyncRead + AsyncSeek + Send + Unpin {}
impl<T: AsyncRead + AsyncSeek + Send + Unpin> FileReader for T {}

/// A writable file sink — forward-only bytes. `Files` opens it; a format encodes into it.
pub trait FileWriter: AsyncWrite + Send + Unpin {}
impl<T: AsyncWrite + Send + Unpin> FileWriter for T {}

/// Decodes a file's bytes into Arrow batches.
#[async_trait]
pub trait FormatRead: Send + Sync {
    /// Read one open file handle into a stream of Arrow batches.
    async fn read(&self, reader: Box<dyn FileReader>) -> Result<BatchStream, TransferredError>;
}

/// Encodes Arrow batches into a file's bytes.
#[async_trait]
pub trait FormatWrite: Send + Sync {
    /// Write all batches into one open sink. Returns the row count written.
    async fn write(
        &self,
        writer: Box<dyn FileWriter>,
        batches: BatchStream,
    ) -> Result<u64, TransferredError>;
}
