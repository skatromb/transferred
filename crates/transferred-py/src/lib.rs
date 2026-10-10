//! Python bindings for `transferred`. Exposes `_native` extension module.
#![doc(html_logo_url = "https://raw.githubusercontent.com/skatromb/transferred/main/logo.png")]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod arrow;
mod error;
mod files;
mod postgres;
mod report;
mod transfer;

use pyo3::prelude::*;
use pyo3_log::{Caching, Logger};

/// Routes Rust `tracing` events into Python `logging` under the `transferred` logger.
fn install_logging(py: Python) -> PyResult<()> {
    // Not the default `LoggersAndLevels`: caching levels would freeze `setLevel` calls made later.
    let logger = Logger::new(py, Caching::Loggers)?.set_prefix("transferred");
    // Already installed means an earlier import wired this up.
    drop(logger.install());
    Ok(())
}

#[pymodule]
mod _native {
    #[pymodule_export]
    use crate::arrow::PyArrowSource;
    #[pymodule_export]
    use crate::error::{
        ArrowError, DestinationError, EmptySourceError, IoError, SourceError, TransferredError,
    };
    #[pymodule_export]
    use crate::files::{PyFilesDestination, PyFilesSource, PyFormat, PyParquet};
    #[pymodule_export]
    use crate::postgres::{PyPostgresDestination, PyPostgresSource};
    #[pymodule_export]
    use crate::report::PyRunReport;
    #[pymodule_export]
    use crate::transfer::{PyDestination, PySource, PyTransfer};
    use pyo3::prelude::*;

    #[pymodule_init]
    fn init(module: &Bound<PyModule>) -> PyResult<()> {
        super::install_logging(module.py())
    }
}
