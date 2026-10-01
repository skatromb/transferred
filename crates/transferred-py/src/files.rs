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
use transferred_files::{
    Compression, FilesDestination, FilesSource, FormatRead, FormatWrite, GlobOrPaths, Parquet,
};

/// Internal `PyO3` wrapper around `transferred_files::Parquet`.
#[gen_stub_pyclass]
#[pyclass(name = "_Parquet", module = "transferred._native", unsendable)]
pub struct PyParquet {
    inner: Parquet,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyParquet {
    #[new]
    #[pyo3(signature = (compression))]
    fn new(compression: Option<&str>) -> PyResult<Self> {
        let compression = parse_compression(compression)?;
        Ok(Self {
            inner: Parquet::new(compression),
        })
    }
}

/// Internal `PyO3` wrapper around `transferred_files::FilesSource`.
#[gen_stub_pyclass]
#[pyclass(name = "_FilesSource", module = "transferred._native", unsendable)]
pub struct PyFilesSource {
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
        let format: Arc<dyn FormatRead> = Arc::new(parquet_arg(format)?);
        Ok(Self {
            inner: Some(FilesSource::new(source, format)),
        })
    }
}

impl PyFilesSource {
    /// Takes the wrapped source, leaving `None` behind.
    pub(crate) fn take(&mut self) -> Option<FilesSource> {
        self.inner.take()
    }
}

/// Internal `PyO3` wrapper around `transferred_files::FilesDestination`.
#[gen_stub_pyclass]
#[pyclass(name = "_FilesDestination", module = "transferred._native", unsendable)]
pub struct PyFilesDestination {
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
        let format: Arc<dyn FormatWrite> = Arc::new(parquet_arg(format)?);
        Ok(Self {
            inner: Some(FilesDestination::new(path, format, single_file)),
        })
    }
}

impl PyFilesDestination {
    /// Takes the wrapped destination, leaving `None` behind.
    pub(crate) fn take(&mut self) -> Option<FilesDestination> {
        self.inner.take()
    }
}

/// Extracts a `Parquet` codec from the `format=` argument. Parquet is the only
/// format today, so any `Parquet` instance resolves here.
fn parquet_arg(format: &Bound<'_, PyAny>) -> PyResult<Parquet> {
    Ok(format.extract::<PyRef<'_, PyParquet>>()?.inner.clone())
}

fn parse_compression(s: Option<&str>) -> PyResult<Compression> {
    match s {
        None => Ok(Compression::None),
        Some("zstd") => Ok(Compression::Zstd),
        Some("snappy") => Ok(Compression::Snappy),
        Some(other) => Err(PyValueError::new_err(format!(
            "unknown compression: {other}. expected one of: zstd, snappy, None"
        ))),
    }
}
