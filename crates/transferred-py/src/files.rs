//! Files source/destination + Parquet format Python wrappers.

#![expect(
    clippy::multiple_inherent_impl,
    reason = "Rust-only methods stay out of `#[pymethods]`"
)]

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use transferred_core::{BoxedDestination, BoxedSource};
use transferred_files::{Compression, FilesDestination, FilesSource, GlobOrPaths, Parquet};

/// Internal `PyO3` wrapper around `transferred_files::Parquet`.
#[pyclass(
    name = "_Parquet",
    module = "transferred._native",
    unsendable,
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
#[pyclass(name = "_FilesSource", module = "transferred._native", unsendable)]
pub(crate) struct PyFilesSource {
    inner: Option<FilesSource>,
}

#[pymethods]
impl PyFilesSource {
    #[new]
    #[pyo3(signature = (path, format))]
    fn new(path: PathArg, format: &PyParquet) -> Self {
        let source = match path {
            PathArg::Many(paths) => GlobOrPaths::Paths(paths),
            PathArg::One(single) => GlobOrPaths::Glob(single.to_string_lossy().into_owned()),
        };
        Self {
            inner: Some(FilesSource::new(source, Arc::new(format.inner))),
        }
    }
}

/// `path=` of `FilesSource`: a list of paths, or one path or glob.
#[derive(FromPyObject)]
enum PathArg {
    Many(Vec<PathBuf>),
    One(PathBuf),
}

impl PyFilesSource {
    /// Takes the wrapped source, leaving `None` behind.
    pub(crate) fn take(&mut self) -> Option<BoxedSource> {
        Some(Box::new(self.inner.take()?))
    }
}

/// Internal `PyO3` wrapper around `transferred_files::FilesDestination`.
#[pyclass(name = "_FilesDestination", module = "transferred._native", unsendable)]
pub(crate) struct PyFilesDestination {
    inner: Option<FilesDestination>,
}

#[pymethods]
impl PyFilesDestination {
    #[new]
    #[pyo3(signature = (path, format, single_file = false))]
    fn new(path: PathBuf, format: &PyParquet, single_file: bool) -> Self {
        Self {
            inner: Some(FilesDestination::new(
                path,
                Arc::new(format.inner),
                single_file,
            )),
        }
    }
}

impl PyFilesDestination {
    /// Takes the wrapped destination, leaving `None` behind.
    pub(crate) fn take(&mut self) -> Option<BoxedDestination> {
        Some(Box::new(self.inner.take()?))
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
