//! Files source/destination + Parquet format Python wrappers.

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use transferred_files::{Compression, FilesDestination, FilesSource, GlobOrPaths, Parquet};

use crate::transfer::{PyDestination, PySource};

/// Internal `PyO3` wrapper around `transferred_files::Parquet`.
#[pyclass(
    name = "_Parquet",
    module = "transferred._native",
    unsendable,
    subclass,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyParquet {
    inner: Parquet,
}

#[pymethods]
impl PyParquet {
    #[new]
    #[pyo3(signature = (compression))]
    fn new(compression: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            inner: Parquet::new(parse_compression(compression)?),
        })
    }
}

/// Internal `PyO3` wrapper around `transferred_files::FilesSource`.
#[pyclass(
    name = "_FilesSource",
    module = "transferred._native",
    extends = PySource,
    subclass,
    unsendable
)]
pub(crate) struct PyFilesSource;

#[pymethods]
impl PyFilesSource {
    #[new]
    #[pyo3(signature = (path, format))]
    fn new(path: PathArg, format: &PyParquet) -> PyClassInitializer<Self> {
        let source = match path {
            PathArg::Many(paths) => GlobOrPaths::Paths(paths),
            PathArg::One(single) => GlobOrPaths::Glob(single.to_string_lossy().into_owned()),
        };
        PySource::init(Self, FilesSource::new(source, Arc::new(format.inner)))
    }
}

/// `path=` of `FilesSource`: a list of paths, or one path or glob.
#[derive(FromPyObject)]
enum PathArg {
    Many(Vec<PathBuf>),
    One(PathBuf),
}

/// Internal `PyO3` wrapper around `transferred_files::FilesDestination`.
#[pyclass(
    name = "_FilesDestination",
    module = "transferred._native",
    extends = PyDestination,
    subclass,
    unsendable
)]
pub(crate) struct PyFilesDestination;

#[pymethods]
impl PyFilesDestination {
    #[new]
    #[pyo3(signature = (path, format, single_file = false))]
    fn new(path: PathBuf, format: &PyParquet, single_file: bool) -> PyClassInitializer<Self> {
        PyDestination::init(
            Self,
            FilesDestination::new(path, Arc::new(format.inner), single_file),
        )
    }
}

fn parse_compression(compression: Option<&str>) -> PyResult<Compression> {
    match compression {
        None => Ok(Compression::Uncompressed),
        Some("zstd") => Ok(Compression::Zstd),
        Some("snappy") => Ok(Compression::Snappy),
        Some(other) => Err(PyValueError::new_err(format!(
            "unknown compression: {other}. expected one of: zstd, snappy, None"
        ))),
    }
}
