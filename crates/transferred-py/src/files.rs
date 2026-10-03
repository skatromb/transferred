//! Files source/destination + Parquet format Python wrappers.

#![expect(
    clippy::multiple_inherent_impl,
    reason = "Rust-only methods stay out of `#[pymethods]`"
)]

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use transferred_core::{BoxedDestination, BoxedSource};
use transferred_files::{Compression, FilesDestination, FilesSource, GlobOrPaths, Parquet};

/// Internal `PyO3` wrapper around `transferred_files::Parquet`.
#[gen_stub_pyclass]
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

#[gen_stub_pymethods]
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
#[gen_stub_pyclass]
#[pyclass(name = "_FilesSource", module = "transferred._native", unsendable)]
pub(crate) struct PyFilesSource {
    inner: Option<FilesSource>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilesSource {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    #[pyo3(signature = (path, format))]
    fn new(
        #[gen_stub(override_type(
            type_repr = "str | os.PathLike | list[str | os.PathLike]",
            imports = ("os",)
        ))]
        path: &Bound<'_, PyAny>,
        format: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let source = if path.cast::<PyList>().is_ok() {
            let paths: Vec<PathBuf> = path.extract()?;
            GlobOrPaths::Paths(paths)
        } else {
            let single: PathBuf = path.extract()?;
            GlobOrPaths::Glob(single.to_string_lossy().into_owned())
        };
        Ok(Self {
            inner: Some(FilesSource::new(source, Arc::new(parquet_arg(format)?))),
        })
    }
}

impl PyFilesSource {
    /// Takes the wrapped source, leaving `None` behind.
    pub(crate) fn take(&mut self) -> Option<BoxedSource> {
        Some(Box::new(self.inner.take()?))
    }
}

/// Internal `PyO3` wrapper around `transferred_files::FilesDestination`.
#[gen_stub_pyclass]
#[pyclass(name = "_FilesDestination", module = "transferred._native", unsendable)]
pub(crate) struct PyFilesDestination {
    inner: Option<FilesDestination>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilesDestination {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    #[pyo3(signature = (path, format, single_file = false))]
    fn new(path: PathBuf, format: &Bound<'_, PyAny>, single_file: bool) -> PyResult<Self> {
        Ok(Self {
            inner: Some(FilesDestination::new(
                path,
                Arc::new(parquet_arg(format)?),
                single_file,
            )),
        })
    }
}

impl PyFilesDestination {
    /// Takes the wrapped destination, leaving `None` behind.
    pub(crate) fn take(&mut self) -> Option<BoxedDestination> {
        Some(Box::new(self.inner.take()?))
    }
}

/// Extracts a `Parquet` codec from the `format=` argument. Parquet is the only
/// format today, so any `Parquet` instance resolves here.
fn parquet_arg(format: &Bound<'_, PyAny>) -> PyResult<Parquet> {
    Ok(format.extract::<PyRef<'_, PyParquet>>()?.inner)
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
