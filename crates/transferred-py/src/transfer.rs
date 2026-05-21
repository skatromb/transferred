//! `Transfer` Python class. Single-shot: consumes source + destination on `run()`.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use tokio::runtime::Builder;
use transferred_core::{Destination, Source, Transfer};

use crate::error::to_pyerr;
use crate::iterable::PyRecordBatchReaderSource;
use crate::parquet::{PyParquetDestination, PyParquetSource};
use crate::report::PyRunReport;

/// Orchestrates a single source → destination run. Single-shot: each instance can run once.
///
/// Args:
///     source: A transferred source (e.g. `ParquetSource`).
///     destination: A transferred destination (e.g. `ParquetDestination`).
///
/// Example:
///     ```py
///     >>> from transferred import ParquetSource, ParquetDestination, Transfer
///     >>> report = Transfer(
///     ...     source=ParquetSource("in.parquet"),
///     ...     destination=ParquetDestination("out.parquet", compression="zstd"),
///     ... ).run()
///     >>> report.rows
///     12481902
///     ```
#[gen_stub_pyclass]
#[pyclass(name = "Transfer", module = "transferred._native", unsendable)]
pub struct PyTransfer {
    source: Option<Box<dyn Source + Send>>,
    destination: Option<Box<dyn Destination + Send>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTransfer {
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
    if let Ok(cell) = obj.cast::<PyParquetSource>() {
        let inner = cell
            .try_borrow_mut()?
            .inner
            .take()
            .ok_or_else(already_consumed)?;
        return Ok(Box::new(inner));
    }
    if let Ok(cell) = obj.cast::<PyRecordBatchReaderSource>() {
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
    if let Ok(cell) = obj.cast::<PyParquetDestination>() {
        let inner = cell
            .try_borrow_mut()?
            .inner
            .take()
            .ok_or_else(already_consumed)?;
        return Ok(Box::new(inner));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "destination must be a transferred destination object",
    ))
}

fn already_consumed() -> PyErr {
    PyRuntimeError::new_err("source or destination already consumed by another Transfer")
}
