//! Postgres source + destination Python wrappers.

#![expect(
    clippy::multiple_inherent_impl,
    reason = "Rust-only methods stay out of `#[pymethods]`"
)]

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use transferred_postgres::{PostgresDestination, PostgresSource};

/// Internal `PyO3` wrapper around `transferred_postgres::PostgresSource`.
#[gen_stub_pyclass]
#[pyclass(name = "_PostgresSource", module = "transferred._native", unsendable)]
pub struct PyPostgresSource {
    inner: Option<PostgresSource>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPostgresSource {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    #[pyo3(signature = (dsn, table))]
    const fn new(dsn: String, table: String) -> Self {
        Self {
            inner: Some(PostgresSource::new(dsn, table)),
        }
    }
}

impl PyPostgresSource {
    /// Takes the wrapped source, leaving `None` behind.
    pub(crate) const fn take(&mut self) -> Option<PostgresSource> {
        self.inner.take()
    }
}

/// Internal `PyO3` wrapper around `transferred_postgres::PostgresDestination`.
#[gen_stub_pyclass]
#[pyclass(
    name = "_PostgresDestination",
    module = "transferred._native",
    unsendable
)]
pub struct PyPostgresDestination {
    inner: Option<PostgresDestination>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPostgresDestination {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    #[pyo3(signature = (dsn, table))]
    const fn new(dsn: String, table: String) -> Self {
        Self {
            inner: Some(PostgresDestination::new(dsn, table)),
        }
    }
}

impl PyPostgresDestination {
    /// Takes the wrapped destination, leaving `None` behind.
    pub(crate) const fn take(&mut self) -> Option<PostgresDestination> {
        self.inner.take()
    }
}
