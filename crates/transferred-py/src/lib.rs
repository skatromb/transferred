//! Python bindings for `transferred`. Exposes `_native` extension module.
#![doc(html_logo_url = "https://raw.githubusercontent.com/skatromb/transferred/main/logo.png")]

mod arrow;
mod error;
mod files;
mod postgres;
mod report;
mod transfer;

use pyo3::prelude::*;
use pyo3_stub_gen::define_stub_info_gatherer;

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    error::register(py, m)?;
    m.add_class::<report::PyRunReport>()?;
    m.add_class::<files::PyParquet>()?;
    m.add_class::<files::PyFilesSource>()?;
    m.add_class::<files::PyFilesDestination>()?;
    m.add_class::<arrow::PyArrowSource>()?;
    m.add_class::<postgres::PyPostgresSource>()?;
    m.add_class::<transfer::PyTransfer>()?;
    Ok(())
}

define_stub_info_gatherer!(stub_info);
