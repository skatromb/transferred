//! `Transfer` Python class. Single-shot: consumes source + destination on `run()`.

use pyo3::PyClass;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Builder;
use transferred_core::{BoxedDestination, BoxedSource, Destination, Source, Transfer};

use crate::error::to_pyerr;
use crate::report::PyRunReport;

/// A `transferred` data source. Subclasses are passed to `Transfer(source=...)`.
#[pyclass(name = "Source", module = "transferred._native", unsendable, subclass)]
pub(crate) struct PySource {
    inner: Option<BoxedSource>,
}

impl PySource {
    pub(crate) fn init<Subclass: PyClass<BaseType = Self>>(
        subclass: Subclass,
        source: impl Source + 'static,
    ) -> PyClassInitializer<Subclass> {
        PyClassInitializer::from(Self {
            inner: Some(Box::new(source)),
        })
        .add_subclass(subclass)
    }
}

/// A `transferred` data destination. Subclasses are passed to `Transfer(destination=...)`.
#[pyclass(
    name = "Destination",
    module = "transferred._native",
    unsendable,
    subclass
)]
pub(crate) struct PyDestination {
    inner: Option<BoxedDestination>,
}

impl PyDestination {
    pub(crate) fn init<Subclass: PyClass<BaseType = Self>>(
        subclass: Subclass,
        destination: impl Destination + 'static,
    ) -> PyClassInitializer<Subclass> {
        PyClassInitializer::from(Self {
            inner: Some(Box::new(destination)),
        })
        .add_subclass(subclass)
    }
}

/// Internal `PyO3` wrapper around `transferred_core::Transfer`. Subclassed by the
/// user-facing Python `Transfer`; not used directly.
#[pyclass(
    name = "_Transfer",
    module = "transferred._native",
    unsendable,
    subclass
)]
pub(crate) struct PyTransfer {
    inner: Option<Transfer>,
}

#[pymethods]
impl PyTransfer {
    #[new]
    fn new(source: &Bound<PySource>, destination: &Bound<PyDestination>) -> PyResult<Self> {
        let taken_source = source.try_borrow_mut()?.inner.take();
        let taken_destination = destination.try_borrow_mut()?.inner.take();
        let (Some(native_source), Some(native_destination)) = (taken_source, taken_destination)
        else {
            return Err(PyRuntimeError::new_err(
                "source or destination already consumed by another Transfer",
            ));
        };
        Ok(Self {
            inner: Some(Transfer::new(native_source, native_destination)),
        })
    }

    fn run(&mut self, py: Python) -> PyResult<PyRunReport> {
        let transfer = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Transfer already consumed"))?;

        let report = py.detach(|| {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| PyRuntimeError::new_err(format!("tokio runtime: {err}")))?;
            runtime.block_on(transfer.run()).map_err(to_pyerr)
        })?;

        Ok(PyRunReport::new(report))
    }
}
