//! Files source/destination + Parquet format Python wrappers.

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::PyClass;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use transferred_files::{
    Compression, FilesDestination, FilesSource, FormatRead, FormatWrite, GlobOrPaths, Parquet,
};

use crate::transfer::{PyDestination, PySource};

/// A file format codec. Pass to `FilesSource`/`FilesDestination(format=...)`.
#[pyclass(name = "Format", module = "transferred._native", subclass)]
pub(crate) struct PyFormat {
    read: Arc<dyn FormatRead>,
    write: Arc<dyn FormatWrite>,
}

impl PyFormat {
    fn init<Subclass: PyClass<BaseType = Self>, Codec: FormatRead + FormatWrite + 'static>(
        subclass: Subclass,
        format: Codec,
    ) -> PyClassInitializer<Subclass> {
        let write = Arc::new(format);
        let read: Arc<dyn FormatRead> = Arc::<Codec>::clone(&write);
        PyClassInitializer::from(Self { read, write }).add_subclass(subclass)
    }
}

/// Internal `PyO3` wrapper around `transferred_files::Parquet`.
#[pyclass(name = "_Parquet", module = "transferred._native", extends = PyFormat, subclass)]
pub(crate) struct PyParquet;

#[pymethods]
impl PyParquet {
    #[new]
    #[pyo3(signature = (compression))]
    fn new(compression: Option<&str>) -> PyResult<PyClassInitializer<Self>> {
        Ok(PyFormat::init(
            Self,
            Parquet::new(parse_compression(compression)?),
        ))
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
    fn new(path: PathArg, format: &PyFormat) -> PyClassInitializer<Self> {
        let source = match path {
            PathArg::Many(paths) => GlobOrPaths::Paths(paths),
            PathArg::One(single) => GlobOrPaths::Glob(single.to_string_lossy().into_owned()),
        };
        PySource::init(Self, FilesSource::new(source, Arc::clone(&format.read)))
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
    fn new(path: PathBuf, format: &PyFormat, single_file: bool) -> PyClassInitializer<Self> {
        PyDestination::init(
            Self,
            FilesDestination::new(path, Arc::clone(&format.write), single_file),
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
