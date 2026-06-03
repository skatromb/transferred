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
        CoreError::Source(msg) => SourceError::new_err(msg),
        CoreError::Destination(msg) => DestinationError::new_err(msg),
        CoreError::Arrow(e) => ArrowError::new_err(e.to_string()),
        CoreError::Io(e) => IoError::new_err(e.to_string()),
        CoreError::Other(msg) => TransferredError::new_err(msg),
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("TransferredError", py.get_type::<TransferredError>())?;
    m.add("SourceError", py.get_type::<SourceError>())?;
    m.add("DestinationError", py.get_type::<DestinationError>())?;
    m.add("ArrowError", py.get_type::<ArrowError>())?;
    m.add("IoError", py.get_type::<IoError>())?;
    Ok(())
}
