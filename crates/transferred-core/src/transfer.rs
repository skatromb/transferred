use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::{RunReport, error::Result};

/// Boxed `Stream` of Arrow batches — one partition's data.
pub type BatchStream = BoxStream<'static, Result<RecordBatch>>;

/// A data source. Yields one or more partitions of Arrow batches.
#[async_trait]
pub trait Source: Send {
    /// Consumes the source and produces its partitions. Single-shot.
    /// Non-partitionable sources return a single-element `Vec`.
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>>;
}

/// A destination. Writes batch partitions atomically and reports stats.
#[async_trait]
pub trait Destination: Send {
    /// Consumes the destination and writes the partitions. Single-shot.
    /// Schema is taken from the first batch each partition emits.
    async fn write_partitions(self: Box<Self>, partitions: Vec<BatchStream>) -> Result<RunReport>;
}

/// Any `Source`, boxed so a `Transfer` can take it without knowing its type.
pub type BoxedSource = Box<dyn Source>;

/// Any `Destination`, boxed so a `Transfer` can take it without knowing its type.
pub type BoxedDestination = Box<dyn Destination>;

/// Orchestrates a single end-to-end run from a `Source` to a `Destination`.
pub struct Transfer {
    source: BoxedSource,
    destination: BoxedDestination,
}

impl Transfer {
    /// Builds a transfer.
    #[must_use]
    pub fn new(source: BoxedSource, destination: BoxedDestination) -> Self {
        Self {
            source,
            destination,
        }
    }

    /// Fetches partitions and hands them to the destination.
    ///
    /// # Errors
    /// Propagates any error from partition setup or write.
    pub async fn run(self) -> Result<RunReport> {
        let partitions = self.source.stream_partitions().await?;
        self.destination.write_partitions(partitions).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use futures::executor::block_on;
    use futures::{StreamExt as _, TryStreamExt as _, stream};

    use super::*;
    use crate::TransferredError;

    /// One single-batch partition per entry, each batch that many rows long.
    struct Partitions(Vec<i32>);

    #[async_trait]
    impl Source for Partitions {
        async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
            Ok(self
                .0
                .into_iter()
                .map(|rows| {
                    let column: ArrayRef = Arc::new(Int32Array::from_iter_values(0..rows));
                    let batch = RecordBatch::try_from_iter([("n", column)]).map_err(Into::into);
                    stream::iter([batch]).boxed()
                })
                .collect())
        }
    }

    /// Fails before yielding any partition.
    struct Failing;

    #[async_trait]
    impl Source for Failing {
        async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
            Err(TransferredError::EmptySource)
        }
    }

    /// Drains every partition and reports the rows it saw.
    struct RowCounter;

    #[async_trait]
    impl Destination for RowCounter {
        async fn write_partitions(
            self: Box<Self>,
            partitions: Vec<BatchStream>,
        ) -> Result<RunReport> {
            let batches: Vec<RecordBatch> =
                stream::iter(partitions).flatten().try_collect().await?;
            let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            Ok(RunReport {
                rows: rows.try_into().unwrap(),
                ..RunReport::default()
            })
        }
    }

    #[test]
    fn run_hands_every_partition_to_the_destination() {
        let transfer = Transfer::new(Box::new(Partitions(vec![3, 5])), Box::new(RowCounter));

        let report = block_on(transfer.run()).unwrap();

        assert_eq!(report.rows, 8);
    }

    #[test]
    fn run_stops_at_a_failing_source() {
        let transfer = Transfer::new(Box::new(Failing), Box::new(RowCounter));

        let outcome = block_on(transfer.run());

        assert!(matches!(outcome, Err(TransferredError::EmptySource)));
    }
}
