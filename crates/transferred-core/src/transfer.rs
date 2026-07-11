use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::{RunReport, error::Result};

/// Boxed `Stream` of Arrow batches — one partition's data.
pub type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// A data source. Yields one or more partitions of Arrow batches.
#[async_trait]
pub trait Source: Send {
    /// Consume the source and produce its partitions. Single-shot.
    /// Non-partitionable sources return a single-element `Vec`.
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>>;
}

/// A destination. Writes batch partitions atomically and reports stats.
#[async_trait]
pub trait Destination: Send {
    /// Consume the destination and write the partitions. Single-shot.
    /// Schema is taken from the first batch each partition emits.
    async fn write_partitions(self: Box<Self>, partitions: Vec<BatchStream>) -> Result<RunReport>;
}

/// Orchestrates a single end-to-end run from a `Source` to a `Destination`.
pub struct Transfer {
    source: Box<dyn Source>,
    destination: Box<dyn Destination>,
}

impl Transfer {
    /// Build a transfer.
    #[must_use]
    pub fn new(source: Box<dyn Source>, destination: Box<dyn Destination>) -> Self {
        Self {
            source,
            destination,
        }
    }

    /// Fetch partitions, hand them to the destination.
    ///
    /// # Errors
    /// Propagates any error from partition setup or write.
    pub async fn run(self) -> Result<RunReport> {
        let partitions = self.source.stream_partitions().await?;
        self.destination.write_partitions(partitions).await
    }
}
