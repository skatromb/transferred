use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::{ElError, RunReport};

/// Boxed `Stream` of Arrow batches — one partition's data.
pub type BatchStream = BoxStream<'static, Result<RecordBatch, ElError>>;

/// A data source. Yields one or more partitions of Arrow batches.
#[async_trait]
pub trait Source: Send {
    /// Consume the source and produce its partitions. Single-shot.
    /// Non-partitionable sources return a single-element `Vec`.
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>, ElError>;
}

/// A destination. Writes batch partitions atomically and reports stats.
#[async_trait]
pub trait Destination: Send {
    /// Consume the destination and write the partitions. Single-shot.
    /// Schema is taken from the first batch each partition emits.
    async fn write_partitins(
        self: Box<Self>,
        partitions: Vec<BatchStream>,
    ) -> Result<RunReport, ElError>;
}
