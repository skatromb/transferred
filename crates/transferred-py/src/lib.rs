//! Python bindings for `transferred`. Exposes `_native` extension module.
#![doc(html_logo_url = "https://raw.githubusercontent.com/skatromb/transferred/main/logo.png")]

mod arrow;
mod error;
mod files;
mod postgres;
mod report;
mod transfer;

use pyo3::prelude::*;
use pyo3_log::{Caching, Logger};
use pyo3_stub_gen::define_stub_info_gatherer;

/// Routes Rust `tracing` events into Python `logging` under the `transferred` logger.
fn install_logging(py: Python<'_>) -> PyResult<()> {
    // Not the default `LoggersAndLevels`: caching levels would freeze `setLevel` calls made later.
    let logger = Logger::new(py, Caching::Loggers)?.set_prefix("transferred");
    // Already installed means an earlier import wired this up.
    let _ = logger.install();
    Ok(())
}

#[pymodule]
fn _native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    install_logging(py)?;
    error::register(py, m)?;
    m.add_class::<report::PyRunReport>()?;
    m.add_class::<files::PyParquet>()?;
    m.add_class::<files::PyFilesSource>()?;
    m.add_class::<files::PyFilesDestination>()?;
    m.add_class::<arrow::PyArrowSource>()?;
    m.add_class::<postgres::PyPostgresSource>()?;
    m.add_class::<postgres::PyPostgresDestination>()?;
    m.add_class::<transfer::PyTransfer>()?;
    Ok(())
}

define_stub_info_gatherer!(stub_info);
