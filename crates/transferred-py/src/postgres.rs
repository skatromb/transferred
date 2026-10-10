//! Postgres source + destination Python wrappers.

#![expect(
    clippy::multiple_inherent_impl,
    reason = "Rust-only methods stay out of `#[pymethods]`"
)]

use pyo3::prelude::*;
use transferred_core::{BoxedDestination, BoxedSource};
use transferred_postgres::{PostgresDestination, PostgresSource};

/// Internal `PyO3` wrapper around `transferred_postgres::PostgresSource`.
#[pyclass(name = "_PostgresSource", module = "transferred._native", unsendable)]
pub(crate) struct PyPostgresSource {
    inner: Option<PostgresSource>,
}

#[pymethods]
impl PyPostgresSource {
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
    pub(crate) fn take(&mut self) -> Option<BoxedSource> {
        Some(Box::new(self.inner.take()?))
    }
}

/// Internal `PyO3` wrapper around `transferred_postgres::PostgresDestination`.
#[pyclass(
    name = "_PostgresDestination",
    module = "transferred._native",
    unsendable
)]
pub(crate) struct PyPostgresDestination {
    inner: Option<PostgresDestination>,
}

#[pymethods]
impl PyPostgresDestination {
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
    pub(crate) fn take(&mut self) -> Option<BoxedDestination> {
        Some(Box::new(self.inner.take()?))
    }
}
