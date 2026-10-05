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
    inner: Option<Transfer>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTransfer {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    fn new(source: &Bound<PyAny>, destination: &Bound<PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: Some(Transfer::new(
                extract_source(source)?,
                extract_destination(destination)?,
            )),
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

/// Any object `Transfer` accepts as its source.
#[derive(FromPyObject)]
enum AnySource<'py> {
    Files(Bound<'py, PyFilesSource>),
    Arrow(Bound<'py, PyArrowSource>),
    Postgres(Bound<'py, PyPostgresSource>),
    // PyO3 convention: Python wrappers expose a `_native_source` attr holding a native source.
    Native {
        #[pyo3(attribute("_native_source"))]
        native: Bound<'py, PyAny>,
    },
}

/// Any object `Transfer` accepts as its destination.
#[derive(FromPyObject)]
enum AnyDestination<'py> {
    Files(Bound<'py, PyFilesDestination>),
    Postgres(Bound<'py, PyPostgresDestination>),
    // PyO3 convention: Python wrappers expose a `_native_destination` attr holding a native destination.
    Native {
        #[pyo3(attribute("_native_destination"))]
        native: Bound<'py, PyAny>,
    },
}

fn extract_source(source: &Bound<PyAny>) -> PyResult<BoxedSource> {
    let any_source = source
        .extract()
        .ok()
        .ok_or_else(|| PyTypeError::new_err("`source` must be a `transferred.Source`"))?;
    let taken = match any_source {
        AnySource::Files(files) => files.try_borrow_mut()?.take(),
        AnySource::Arrow(arrow) => arrow.try_borrow_mut()?.take(),
        AnySource::Postgres(postgres) => postgres.try_borrow_mut()?.take(),
        AnySource::Native { native } => return extract_source(&native),
    };
    taken.ok_or_else(already_consumed)
}

fn extract_destination(destination: &Bound<PyAny>) -> PyResult<BoxedDestination> {
    let any_destination = destination
        .extract()
        .ok()
        .ok_or_else(|| PyTypeError::new_err("`destination` must be a `transferred.Destination`"))?;
    let taken = match any_destination {
        AnyDestination::Files(files) => files.try_borrow_mut()?.take(),
        AnyDestination::Postgres(postgres) => postgres.try_borrow_mut()?.take(),
        AnyDestination::Native { native } => return extract_destination(&native),
    };
    taken.ok_or_else(already_consumed)
}

fn already_consumed() -> PyErr {
    PyRuntimeError::new_err("source or destination already consumed by another Transfer")
}
