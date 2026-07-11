//! File-format. A format converts between a file's byte stream and Arrow batches.
//! `Files` owns opening the file and hands over the byte handle.
//! One module per codec (`parquet`, `csv`, `avro`, etc.).

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use transferred_core::{BatchStream, Result};

pub mod parquet;

pub use parquet::Parquet;

/// A readable file handle trait marker for random-access bytes.
pub trait FileReader: AsyncRead + AsyncSeek + Send + Unpin {}
impl<T: AsyncRead + AsyncSeek + Send + Unpin> FileReader for T {}

/// A writable file handle trait.
pub trait FileWriter: AsyncWrite + Send + Unpin {}
impl<T: AsyncWrite + Send + Unpin> FileWriter for T {}

/// Decodes a file's bytes into Arrow batches.
#[async_trait]
pub trait FormatRead: Send + Sync {
    /// Read one open file handle into a stream of Arrow batches.
    async fn read(&self, reader: Box<dyn FileReader>) -> Result<BatchStream>;
}

/// Encodes Arrow batches into a file's bytes.
#[async_trait]
pub trait FormatWrite: Send + Sync {
    /// File extension for written parts, no dot (e.g. `"parquet"`).
    fn file_extension(&self) -> &'static str;

    /// Write all batches into one open sink. Returns the row count written.
    async fn write(&self, writer: Box<dyn FileWriter>, batches: BatchStream) -> Result<u64>;
}
