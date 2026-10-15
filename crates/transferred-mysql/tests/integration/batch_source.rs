//! Supplies typed batches and upstream failures to destination tests.

use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::{StreamExt as _, stream};
use transferred_core::{BatchStream, Result, Source};

pub(crate) struct BatchSource(pub Vec<Result<RecordBatch>>);

#[async_trait]
impl Source for BatchSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        Ok(vec![stream::iter(self.0).boxed()])
    }
}
