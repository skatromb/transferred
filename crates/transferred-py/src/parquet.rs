//! Parquet source and destination Python wrappers.

use std::path::PathBuf;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use transferred_parquet::{Compression, ParquetDestination, ParquetSource};

/// Local single-file Parquet source. No I/O performed at construction.
#[gen_stub_pyclass]
#[pyclass(name = "ParquetSource", module = "transferred._native", unsendable)]
pub struct PyParquetSource {
    pub(crate) inner: Option<ParquetSource>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyParquetSource {
    #[new]
    fn new(path: PathBuf) -> Self {
        Self {
            inner: Some(ParquetSource::new(path)),
        }
    }
}

/// Local single-file Parquet destination. Writes via tmp file + atomic rename.
#[gen_stub_pyclass]
#[pyclass(
    name = "ParquetDestination",
    module = "transferred._native",
    unsendable
)]
pub struct PyParquetDestination {
    pub(crate) inner: Option<ParquetDestination>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyParquetDestination {
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
