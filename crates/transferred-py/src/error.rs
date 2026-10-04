//! Map `TransferredError` to Python exception hierarchy.

use std::error::Error;

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_stub_gen::derive::gen_stub_pyclass;
use transferred_core::TransferredError as CoreError;

/// Base exception for all `transferred` failures.
///
/// Subclasses: `SourceError` (and `EmptySourceError`), `DestinationError`, `ArrowError`, `IoError`.
///
/// Example:
///     ```py
///     >>> from transferred import Transfer, TransferredError
///     >>> try:
///     ...     Transfer(source=..., destination=...).run()
///     ... except TransferredError as e:
///     ...     print(f"transfer failed: {e}")
///     ```
#[gen_stub_pyclass]
#[pyclass(extends = PyException, module = "transferred._native", subclass, frozen)]
pub(crate) struct TransferredError;

#[pymethods]
impl TransferredError {
    #[new]
    #[pyo3(signature = (*_args))]
    const fn new(_args: &Bound<'_, PyTuple>) -> Self {
        Self
    }
}

/// Source read failed (file missing, malformed Parquet, etc.).
#[gen_stub_pyclass]
#[pyclass(extends = TransferredError, module = "transferred._native", subclass, frozen)]
pub(crate) struct SourceError;

#[pymethods]
impl SourceError {
    #[new]
    #[pyo3(signature = (*_args))]
    fn new(_args: &Bound<'_, PyTuple>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(TransferredError).add_subclass(Self)
    }
}

/// Source produced no batches — nothing to transfer.
#[gen_stub_pyclass]
#[pyclass(extends = SourceError, module = "transferred._native", subclass, frozen)]
pub(crate) struct EmptySourceError;

#[pymethods]
impl EmptySourceError {
    #[new]
    #[pyo3(signature = (*_args))]
    fn new(_args: &Bound<'_, PyTuple>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(TransferredError)
            .add_subclass(SourceError)
            .add_subclass(Self)
    }
}

/// Destination write failed (permission denied, disk full, schema mismatch).
#[gen_stub_pyclass]
#[pyclass(extends = TransferredError, module = "transferred._native", subclass, frozen)]
pub(crate) struct DestinationError;

#[pymethods]
impl DestinationError {
    #[new]
    #[pyo3(signature = (*_args))]
    fn new(_args: &Bound<'_, PyTuple>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(TransferredError).add_subclass(Self)
    }
}

/// Arrow schema or array conversion failed.
#[gen_stub_pyclass]
#[pyclass(extends = TransferredError, module = "transferred._native", subclass, frozen)]
pub(crate) struct ArrowError;

#[pymethods]
impl ArrowError {
    #[new]
    #[pyo3(signature = (*_args))]
    fn new(_args: &Bound<'_, PyTuple>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(TransferredError).add_subclass(Self)
    }
}

/// Filesystem I/O error not attributable to source or destination logic.
#[gen_stub_pyclass]
#[pyclass(extends = TransferredError, module = "transferred._native", subclass, frozen)]
pub(crate) struct IoError;

#[pymethods]
impl IoError {
    #[new]
    #[pyo3(signature = (*_args))]
    fn new(_args: &Bound<'_, PyTuple>) -> PyClassInitializer<Self> {
        PyClassInitializer::from(TransferredError).add_subclass(Self)
    }
}

/// Joins an error with everything that caused it, a driver's own message often being a bare category.
fn causes(error: &dyn Error) -> String {
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(err) = cause {
        message.push_str(": ");
        message.push_str(&err.to_string());
        cause = err.source();
    }
    message
}

pub(crate) fn to_pyerr(err: CoreError) -> PyErr {
    match err {
        CoreError::Source(cause) => PyErr::new::<SourceError, _>(causes(&*cause)),
        CoreError::EmptySource => PyErr::new::<EmptySourceError, _>(err.to_string()),
        CoreError::Destination(cause) => PyErr::new::<DestinationError, _>(causes(&*cause)),
        CoreError::Arrow(cause) => PyErr::new::<ArrowError, _>(causes(&cause)),
        CoreError::Io(cause) => PyErr::new::<IoError, _>(causes(&cause)),
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TransferredError>()?;
    module.add_class::<SourceError>()?;
    module.add_class::<EmptySourceError>()?;
    module.add_class::<DestinationError>()?;
    module.add_class::<ArrowError>()?;
    module.add_class::<IoError>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt;

    use super::causes;

    /// One link of a cause chain, printing its own message only.
    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Self>>);

    impl fmt::Display for Layer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl Error for Layer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self.1.as_deref()?)
        }
    }

    /// A driver names a category and leaves the detail to its cause, so both have to reach Python.
    #[test]
    fn joins_a_cause_chain_into_one_message() {
        let detail = Layer("relation \"nope\" does not exist", None);
        let reported = Layer("db error", Some(Box::new(detail)));

        assert_eq!(
            causes(&reported),
            "db error: relation \"nope\" does not exist"
        );
    }
}
