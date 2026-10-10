//! Internal bridge: wraps an Arrow C stream as a `transferred-core` `Source`.

#![expect(
    clippy::multiple_inherent_impl,
    reason = "Rust-only methods stay out of `#[pymethods]`"
)]

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow as _;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::Stream;
use pyo3::prelude::*;
use transferred_core::{BatchStream, BoxedSource, Result, Source, TransferredError};

/// Internal `PyO3` wrapper around an Arrow C stream, built by `Transfer` from a `DataFrame` or rows.
#[pyclass(name = "_ArrowSource", module = "transferred._native", unsendable)]
pub(crate) struct PyArrowSource {
    inner: Option<ArrowSource>,
}

#[pymethods]
impl PyArrowSource {
    #[new]
    fn new(reader: &Bound<PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: Some(ArrowSource {
                reader: ArrowArrayStreamReader::from_pyarrow_bound(reader)?,
            }),
        })
    }
}

impl PyArrowSource {
    /// Takes the wrapped source, leaving `None` behind.
    pub(crate) fn take(&mut self) -> Option<BoxedSource> {
        Some(Box::new(self.inner.take()?))
    }
}

/// Rust-side source over a pyarrow `RecordBatchReader` that gives us `Send`.
pub(crate) struct ArrowSource {
    reader: ArrowArrayStreamReader,
}

#[async_trait]
impl Source for ArrowSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>> {
        let stream = ArrowReaderStream {
            reader: self.reader,
        };
        Ok(vec![Box::pin(stream)])
    }
}

/// Adapts the reader to an async `Stream`.
struct ArrowReaderStream {
    reader: ArrowArrayStreamReader,
}

impl Stream for ArrowReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        let next = Python::attach(|_py| self.reader.next());
        Poll::Ready(match next {
            None => None,
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(err)) => Some(Err(TransferredError::Arrow(err))),
        })
    }
}
