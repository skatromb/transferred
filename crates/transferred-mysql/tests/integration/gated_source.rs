//! Pauses a source after its first batch has reached the destination.

use arrow::array::RecordBatch;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt as _;
use tokio::sync::oneshot::{Receiver, Sender};
use transferred_core::{BatchStream, Result, Source};

pub(crate) struct GatedSource {
    pub first: RecordBatch,
    pub arrived: Sender<()>,
    pub release: Receiver<()>,
}

#[async_trait]
impl Source for GatedSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        let Self {
            first,
            arrived,
            release,
        } = *self;
        let batches: BatchStream = try_stream! {
            yield first;
            arrived.send(()).expect("test is waiting for the first batch");
            release.await.expect("test releases the source");
        }
        .boxed();
        Ok(vec![batches])
    }
}
