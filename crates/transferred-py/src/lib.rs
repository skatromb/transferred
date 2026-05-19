//! Python bindings for `transferred`. Exposes `_native` extension module.

mod error;
mod parquet;
mod report;
mod transfer;

use pyo3::prelude::*;
use pyo3_stub_gen::define_stub_info_gatherer;

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    error::register(py, m)?;
    m.add_class::<report::PyRunReport>()?;
    m.add_class::<parquet::PyParquetSource>()?;
    m.add_class::<parquet::PyParquetDestination>()?;
    m.add_class::<transfer::PyTransfer>()?;
    Ok(())
}

define_stub_info_gatherer!(stub_info);
