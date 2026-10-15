use pyo3::prelude::*;
use transferred_mysql::MySqlDestination;

use crate::error::invalid_arguments;
use crate::transfer::PyDestination;

/// Internal `PyO3` wrapper around `transferred_mysql::MySqlDestination`.
#[pyclass(name = "_MySqlDestination", module = "transferred._native", extends = PyDestination, subclass, unsendable)]
pub(crate) struct PyMySqlDestination;

#[pymethods]
impl PyMySqlDestination {
    #[new]
    #[pyo3(signature = (dsn, table))]
    fn new(dsn: &str, table: &str) -> PyResult<PyClassInitializer<Self>> {
        Ok(PyDestination::init(
            Self,
            MySqlDestination::new(dsn, table).map_err(|error| invalid_arguments(&error))?,
        ))
    }
}
