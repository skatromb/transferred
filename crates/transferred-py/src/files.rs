//! Files source/destination + Parquet format Python wrappers.

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
/// Constructed by the user-facing Python `Parquet`; not used directly.
#[gen_stub_pyclass]
#[pyclass(name = "_Parquet", module = "transferred._native", unsendable)]
pub struct PyParquet {
    pub(crate) inner: Parquet,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyParquet {
    #[new]
    #[pyo3(signature = (compression = "zstd", row_group_size = None))]
    fn new(compression: &str, row_group_size: Option<usize>) -> PyResult<Self> {
        let compression = parse_compression(compression)?;
        Ok(Self {
            inner: Parquet::new(compression, row_group_size),
        })
    }
}

/// Internal `PyO3` wrapper around `transferred_files::FilesSource`.
/// Constructed by the user-facing Python `Files`; not used directly.
#[gen_stub_pyclass]
#[pyclass(name = "_FilesSource", module = "transferred._native", unsendable)]
pub struct PyFilesSource {
    pub(crate) inner: Option<FilesSource>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilesSource {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    #[pyo3(signature = (path, format = None))]
    fn new(
        #[gen_stub(override_type(
            type_repr = "str | os.PathLike | list[str | os.PathLike]",
            imports = ("os",)
        ))]
        path: &Bound<'_, PyAny>,
        format: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let source = if path.cast::<PyList>().is_ok() {
            let paths: Vec<PathBuf> = path.extract()?;
            GlobOrPaths::Paths(paths)
        } else {
            let single: PathBuf = path.extract()?;
            GlobOrPaths::Glob(single.to_string_lossy().into_owned())
        };
        let format: Arc<dyn FormatRead> = read_format(format)?;
        Ok(Self {
            inner: Some(FilesSource::new(source, format)),
        })
    }
}

/// Internal `PyO3` wrapper around `transferred_files::FilesDestination`.
/// Constructed by the user-facing Python `Files`; not used directly.
#[gen_stub_pyclass]
#[pyclass(name = "_FilesDestination", module = "transferred._native", unsendable)]
pub struct PyFilesDestination {
    pub(crate) inner: Option<FilesDestination>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilesDestination {
    #[gen_stub(override_return_type(
        type_repr = "typing.Self",
        imports = ("typing")
    ))]
    #[new]
    #[pyo3(signature = (path, format = None))]
    fn new(path: PathBuf, format: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let format: Arc<dyn FormatWrite> = write_format(format)?;
        Ok(Self {
            inner: Some(FilesDestination::new(path, format)),
        })
    }
}

/// Resolve the `format=` argument to a read codec; `None` defaults to Parquet.
fn read_format(format: Option<&Bound<'_, PyAny>>) -> PyResult<Arc<dyn FormatRead>> {
    Ok(Arc::new(parquet_arg(format)?))
}

/// Resolve the `format=` argument to a write codec; `None` defaults to Parquet.
fn write_format(format: Option<&Bound<'_, PyAny>>) -> PyResult<Arc<dyn FormatWrite>> {
    Ok(Arc::new(parquet_arg(format)?))
}

/// Extract a `Parquet` codec from the `format=` argument. Parquet is the only
/// format today, so `None` and any `Parquet` instance both resolve here.
fn parquet_arg(format: Option<&Bound<'_, PyAny>>) -> PyResult<Parquet> {
    match format {
        None => Ok(Parquet::default()),
        Some(obj) => Ok(obj.extract::<PyRef<'_, PyParquet>>()?.inner.clone()),
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
