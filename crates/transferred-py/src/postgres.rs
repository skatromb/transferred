//! Postgres source + destination Python wrappers.

use pyo3::prelude::*;
use transferred_postgres::{PostgresDestination, PostgresSource};

use crate::transfer::{PyDestination, PySource};

/// Internal `PyO3` wrapper around `transferred_postgres::PostgresSource`.
#[pyclass(
    name = "_PostgresSource",
    module = "transferred._native",
    extends = PySource,
    subclass,
    unsendable
)]
pub(crate) struct PyPostgresSource;

#[pymethods]
impl PyPostgresSource {
    #[new]
    #[pyo3(signature = (dsn, table))]
    fn new(dsn: String, table: String) -> PyClassInitializer<Self> {
        PySource::init(Self, PostgresSource::new(dsn, table))
    }
}

/// Internal `PyO3` wrapper around `transferred_postgres::PostgresDestination`.
#[pyclass(
    name = "_PostgresDestination",
    module = "transferred._native",
    extends = PyDestination,
    subclass,
    unsendable
)]
pub(crate) struct PyPostgresDestination;

#[pymethods]
impl PyPostgresDestination {
    #[new]
    #[pyo3(signature = (dsn, table))]
    fn new(dsn: String, table: String) -> PyClassInitializer<Self> {
        PyDestination::init(Self, PostgresDestination::new(dsn, table))
    }
}
