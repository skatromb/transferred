//! Parquet source and destination Python wrappers.

use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use transferred_parquet::{Compression, ParquetDestination, ParquetSource};

/// Internal `PyO3` wrapper around `transferred_parquet::ParquetSource`.
/// Constructed by the user-facing Python `ParquetSource`; not used directly.
#[gen_stub_pyclass]
#[pyclass(name = "_ParquetSource", module = "transferred._native", unsendable)]
pub struct PyParquetSource {
    pub(crate) inner: Option<ParquetSource>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyParquetSource {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    fn new(path: PathBuf) -> Self {
        Self {
            inner: Some(ParquetSource::new(path)),
        }
    }
}

/// Internal `PyO3` wrapper around `transferred_parquet::ParquetDestination`.
/// Constructed by the user-facing Python `ParquetDestination`; not used directly.
#[gen_stub_pyclass]
#[pyclass(
    name = "_ParquetDestination",
    module = "transferred._native",
    unsendable
)]
pub struct PyParquetDestination {
    pub(crate) inner: Option<ParquetDestination>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyParquetDestination {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    #[pyo3(signature = (path, compression = "zstd"))]
    fn new(path: PathBuf, compression: &str) -> PyResult<Self> {
        let compression = parse_compression(compression)?;
        Ok(Self {
            inner: Some(ParquetDestination::new(path, compression)),
        })
    }
}

fn parse_compression(s: &str) -> PyResult<Compression> {
    match s.to_ascii_lowercase().as_str() {
        "zstd" => Ok(Compression::Zstd),
        "snappy" => Ok(Compression::Snappy),
        "uncompressed" | "none" => Ok(Compression::None),
        other => Err(PyValueError::new_err(format!(
            "unknown compression: {other}. expected one of: zstd, snappy, uncompressed"
        ))),
    }
}
