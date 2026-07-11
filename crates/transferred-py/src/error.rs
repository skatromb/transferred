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

pub fn to_pyerr(err: CoreError) -> PyErr {
    match err {
        CoreError::Source(err) => SourceError::new_err(err.to_string()),
        CoreError::EmptySource => EmptySourceError::new_err(err.to_string()),
        CoreError::Destination(err) => DestinationError::new_err(err.to_string()),
        CoreError::Arrow(err) => ArrowError::new_err(err.to_string()),
        CoreError::Io(err) => IoError::new_err(err.to_string()),
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
