//! `Transfer` Python class. Single-shot: consumes source + destination on `run()`.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use tokio::runtime::Builder;
use transferred_core::{Destination, Source, Transfer};

use crate::arrow::PyArrowSource;
use crate::error::to_pyerr;
use crate::files::{PyFilesDestination, PyFilesSource};
use crate::postgres::PyPostgresSource;
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
pub struct PyTransfer {
    source: Option<Box<dyn Source + Send>>,
    destination: Option<Box<dyn Destination + Send>>,
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
        let source = extract_source(source)?;
        let destination = extract_destination(destination)?;
        Ok(Self {
            source: Some(source),
            destination: Some(destination),
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
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime: {e}")))?;
            rt.block_on(Transfer::new(source, destination).run())
                .map_err(to_pyerr)
        })?;

        Ok(PyRunReport::new(report))
    }
}

fn extract_source(obj: &Bound<'_, PyAny>) -> PyResult<Box<dyn Source + Send>> {
    if let Ok(cell) = obj.cast::<PyFilesSource>() {
        let inner = cell
            .try_borrow_mut()?
            .inner
            .take()
            .ok_or_else(already_consumed)?;
        return Ok(Box::new(inner));
    }
    if let Ok(cell) = obj.cast::<PyArrowSource>() {
        let inner = cell
            .try_borrow_mut()?
            .inner
            .take()
            .ok_or_else(already_consumed)?;
        return Ok(Box::new(inner));
    }
    if let Ok(cell) = obj.cast::<PyPostgresSource>() {
        let inner = cell
            .try_borrow_mut()?
            .inner
            .take()
            .ok_or_else(already_consumed)?;
        return Ok(Box::new(inner));
    }
    // PyO3 convention: Python wrappers expose a `_native_source` attr holding a native source.
    if let Ok(inner) = obj.getattr("_native_source") {
        return extract_source(&inner);
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "source must be a transferred source object",
    ))
}

fn extract_destination(obj: &Bound<'_, PyAny>) -> PyResult<Box<dyn Destination + Send>> {
    if let Ok(cell) = obj.cast::<PyFilesDestination>() {
        let inner = cell
            .try_borrow_mut()?
            .inner
            .take()
            .ok_or_else(already_consumed)?;
        return Ok(Box::new(inner));
    }
    // PyO3 convention: Python wrappers expose a `_native_destination` attr holding a native destination.
    if let Ok(inner) = obj.getattr("_native_destination") {
        return extract_destination(&inner);
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "destination must be a transferred destination object",
    ))
}

fn already_consumed() -> PyErr {
    PyRuntimeError::new_err("source or destination already consumed by another Transfer")
}
