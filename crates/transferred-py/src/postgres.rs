//! Postgres source Python wrapper.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use transferred_postgres::PostgresSource;

/// Internal `PyO3` wrapper around `transferred_postgres::PostgresSource`.
#[gen_stub_pyclass]
#[pyclass(name = "_PostgresSource", module = "transferred._native", unsendable)]
pub struct PyPostgresSource {
    pub(crate) inner: Option<PostgresSource>,
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
    fn new(dsn: String, table: String) -> Self {
        Self {
            inner: Some(PostgresSource::new(dsn, table)),
        }
    }
}
