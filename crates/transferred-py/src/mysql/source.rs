use pyo3::prelude::*;
use transferred_mysql::MySqlSource;

use crate::error::invalid_arguments;
use crate::transfer::PySource;

/// Internal `PyO3` wrapper around `transferred_mysql::MySqlSource`.
#[pyclass(name = "_MySqlSource", module = "transferred._native", extends = PySource, subclass, unsendable)]
pub(crate) struct PyMySqlSource;

#[pymethods]
impl PyMySqlSource {
    #[new]
    #[pyo3(signature = (dsn, table))]
    fn new(dsn: &str, table: &str) -> PyResult<PyClassInitializer<Self>> {
        Ok(PySource::init(
            Self,
            MySqlSource::new(dsn, table).map_err(|error| invalid_arguments(&error))?,
        ))
    }
}
