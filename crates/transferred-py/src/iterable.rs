//! Internal bridge: wraps a pyarrow `RecordBatchReader` as a `transferred-core` `Source`.
//!
//! Not user-facing on its own — the public Python class `PyIterableSource` constructs
//! a pyarrow reader from a Python iterable and feeds it through here.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::Stream;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use transferred_core::{BatchStream, ElError, Source};

/// Internal `PyO3` wrapper around a pyarrow `RecordBatchReader`. Constructed by
/// the user-facing Python `PyIterableSource`; not intended to be used directly.
#[gen_stub_pyclass]
#[pyclass(
    name = "_RecordBatchReaderSource",
    module = "transferred._native",
    unsendable
)]
pub struct PyRecordBatchReaderSource {
    pub(crate) inner: Option<ArrowReaderSource>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRecordBatchReaderSource {
    #[new]
    fn new(reader: &Bound<'_, PyAny>) -> PyResult<Self> {
        let reader = ArrowArrayStreamReader::from_pyarrow_bound(reader)?;
        Ok(Self {
            inner: Some(ArrowReaderSource { reader }),
        })
    }
}

/// Rust-side source over a pyarrow `RecordBatchReader`.
pub struct ArrowReaderSource {
    reader: ArrowArrayStreamReader,
}

#[async_trait]
impl Source for ArrowReaderSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>, ElError> {
        let stream = PyArrowReaderStream {
            reader: self.reader,
        };
        Ok(vec![Box::pin(stream)])
    }
}

struct PyArrowReaderStream {
    reader: ArrowArrayStreamReader,
}

impl Stream for PyArrowReaderStream {
    type Item = Result<RecordBatch, ElError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = Python::attach(|_py| self.reader.next());
        Poll::Ready(match next {
            None => None,
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(e)) => Some(Err(ElError::Arrow(e))),
        })
    }
}
