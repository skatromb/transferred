//! Internal bridge: wraps a pyarrow `RecordBatchReader` as a `transferred-core` `Source`.
//!
//! Not user-facing on its own — the public Python class `ArrowSource` (or any
//! Python wrapper exposing `_native_source`) constructs a pyarrow reader and
//! feeds it through here.

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
/// the user-facing Python `ArrowSource`; not intended to be used directly.
#[gen_stub_pyclass]
#[pyclass(name = "_ArrowSource", module = "transferred._native", unsendable)]
pub struct PyArrowSource {
    pub(crate) inner: Option<ArrowSource>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyArrowSource {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    fn new(reader: &Bound<'_, PyAny>) -> PyResult<Self> {
        let reader = ArrowArrayStreamReader::from_pyarrow_bound(reader)?;
        Ok(Self {
            inner: Some(ArrowSource { reader }),
        })
    }
}

/// Rust-side source over a pyarrow `RecordBatchReader` that gives us `Send`
pub struct ArrowSource {
    reader: ArrowArrayStreamReader,
}

#[async_trait]
impl Source for ArrowSource {
    async fn stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>, ElError> {
        let stream = ArrowReaderStream {
            reader: self.reader,
        };
        Ok(vec![Box::pin(stream)])
    }
}

/// Struct for implementing `async Stream`
struct ArrowReaderStream {
    reader: ArrowArrayStreamReader,
}

impl Stream for ArrowReaderStream {
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
