//! `Transfer` Python class. Single-shot: consumes source + destination on `run()`.

use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use tokio::runtime::Builder;
use transferred_core::{BoxedDestination, BoxedSource, Transfer};

use crate::arrow::PyArrowSource;
use crate::error::to_pyerr;
use crate::files::{PyFilesDestination, PyFilesSource};
use crate::postgres::{PyPostgresDestination, PyPostgresSource};
use crate::report::PyRunReport;

/// Internal `PyO3` wrapper around `transferred_core::Transfer`. Subclassed by the
/// user-facing Python `Transfer`; not used directly.
#[gen_stub_pyclass]
#[pyclass(
    name = "_Transfer",
    module = "transferred._native",
    unsendable,
    subclass
)]
pub(crate) struct PyTransfer {
    source: Option<BoxedSource>,
    destination: Option<BoxedDestination>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTransfer {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    fn new(source: &Bound<'_, PyAny>, destination: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            source: Some(extract_source(source)?),
            destination: Some(extract_destination(destination)?),
        })
    }

    fn run(&mut self, py: Python<'_>) -> PyResult<PyRunReport> {
        let source = self
            .source
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Transfer already consumed"))?;
        let destination = self
            .destination
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Transfer already consumed"))?;

        let report = py.detach(|| {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| PyRuntimeError::new_err(format!("tokio runtime: {err}")))?;
            runtime
                .block_on(Transfer::new(source, destination).run())
                .map_err(to_pyerr)
        })?;

        Ok(PyRunReport::new(report))
    }
}

/// Downcasts `$obj` to each pyclass in turn; on match, takes its inner value and returns it boxed.
macro_rules! try_take_inner {
    ($obj:expr, $($py_class:ty),+) => {
        $(if let Ok(cell) = $obj.cast::<$py_class>() {
            let inner = cell
                .try_borrow_mut()?
                .take()
                .ok_or_else(already_consumed)?;
            return Ok(Box::new(inner));
        })+
    };
}

fn extract_source(source: &Bound<'_, PyAny>) -> PyResult<BoxedSource> {
    try_take_inner!(source, PyFilesSource, PyArrowSource, PyPostgresSource);
    // PyO3 convention: Python wrappers expose a `_native_source` attr holding a native source.
    if let Ok(inner) = source.getattr("_native_source") {
        return extract_source(&inner);
    }
    Err(PyTypeError::new_err(
        "source must be a transferred source object",
    ))
}

fn extract_destination(destination: &Bound<'_, PyAny>) -> PyResult<BoxedDestination> {
    try_take_inner!(destination, PyFilesDestination, PyPostgresDestination);
    // PyO3 convention: Python wrappers expose a `_native_destination` attr holding a native destination.
    if let Ok(inner) = destination.getattr("_native_destination") {
        return extract_destination(&inner);
    }
    Err(PyTypeError::new_err(
        "destination must be a transferred destination object",
    ))
}

fn already_consumed() -> PyErr {
    PyRuntimeError::new_err("source or destination already consumed by another Transfer")
}
