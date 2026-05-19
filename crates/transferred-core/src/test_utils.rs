//! In-memory `Source` and `Destination` for tests in this and downstream crates.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{StreamExt, stream};

use crate::{BatchStream, Destination, ElError, RunReport, Source};

/// In-memory `Source` that yields a fixed `Vec<RecordBatch>` as a single partition.
pub struct TestSource {
    batches: Vec<RecordBatch>,
}

impl TestSource {
    /// Build from pre-built batches.
    #[must_use]
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }
}

#[async_trait]
impl Source for TestSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>, ElError> {
        let stream = stream::iter(self.batches.into_iter().map(Ok));
        Ok(vec![Box::pin(stream)])
    }
}

/// In-memory `Destination` that collects batches into a shared `Vec`.
/// Clone the `batches` `Arc` before moving the destination into a `Transfer`.
pub struct TestDestination {
    pub batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl TestDestination {
    /// Build an empty collector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Default for TestDestination {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Destination for TestDestination {
    async fn write_partitions(
        self: Box<Self>,
        partitions: Vec<BatchStream>,
    ) -> Result<RunReport, ElError> {
        let mut rows: u64 = 0;
        for mut partition in partitions {
            while let Some(batch) = partition.next().await {
                let batch = batch?;
                rows += batch.num_rows() as u64;
                self.batches
                    .lock()
                    .expect("TestDestination mutex")
                    .push(batch);
            }
        }
        Ok(RunReport {
            rows,
            bytes_written: 0,
            duration: Duration::ZERO,
            coercions: vec![],
        })
    }
}
