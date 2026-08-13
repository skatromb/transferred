//! Map `TransferredError` to Python exception hierarchy.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use transferred_core::TransferredError as CoreError;

create_exception!(
    transferred._native,
    TransferredError,
    PyException,
    r#"Base exception for all `transferred` failures.

Subclasses: `SourceError`, `DestinationError`, `ArrowError`, `IoError`.

Example:
    ```py
    >>> from transferred import Transfer, TransferredError
    >>> try:
    ...     Transfer(source=..., destination=...).run()
    ... except TransferredError as e:
    ...     print(f"transfer failed: {e}")
    ```"#
);
create_exception!(
    transferred._native,
    SourceError,
    TransferredError,
    "Source read failed (file missing, malformed Parquet, etc.)."
);
create_exception!(
    transferred._native,
    EmptySourceError,
    SourceError,
    "Source produced no batches — nothing to transfer."
);
create_exception!(
    transferred._native,
    DestinationError,
    TransferredError,
    "Destination write failed (permission denied, disk full, schema mismatch)."
);
create_exception!(
    transferred._native,
    ArrowError,
    TransferredError,
    "Arrow schema or array conversion failed."
);
create_exception!(
    transferred._native,
    IoError,
    TransferredError,
    "Filesystem I/O error not attributable to source or destination logic."
);

/// Joins an error with everything that caused it, a driver's own message often being a bare category.
fn causes(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut cause = err.source();
    while let Some(err) = cause {
        message.push_str(": ");
        message.push_str(&err.to_string());
        cause = err.source();
    }
    message
}

pub fn to_pyerr(err: CoreError) -> PyErr {
    match err {
        CoreError::Source(err) => SourceError::new_err(causes(&*err)),
        CoreError::EmptySource => EmptySourceError::new_err(err.to_string()),
        CoreError::Destination(err) => DestinationError::new_err(causes(&*err)),
        CoreError::Arrow(err) => ArrowError::new_err(causes(&err)),
        CoreError::Io(err) => IoError::new_err(causes(&err)),
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("TransferredError", py.get_type::<TransferredError>())?;
    m.add("SourceError", py.get_type::<SourceError>())?;
    m.add("EmptySourceError", py.get_type::<EmptySourceError>())?;
    m.add("DestinationError", py.get_type::<DestinationError>())?;
    m.add("ArrowError", py.get_type::<ArrowError>())?;
    m.add("IoError", py.get_type::<IoError>())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt;

    use super::causes;

    /// One link of a cause chain, printing its own message only.
    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Layer>>);

    impl fmt::Display for Layer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl Error for Layer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.1.as_deref().map(|layer| layer as &dyn Error)
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
