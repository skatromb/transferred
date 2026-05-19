//! Map `ElError` to Python exception hierarchy.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use transferred_core::ElError as CoreError;

create_exception!(
    transferred._native,
    ElError,
    PyException,
    r#"Base exception for all `transferred` failures.

Subclasses: `SourceError`, `DestinationError`, `ArrowError`, `IoError`.

Example:
    ```py
    >>> from transferred import Transfer, ElError
    >>> try:
    ...     Transfer(source=..., destination=...).run()
    ... except ElError as e:
    ...     print(f"transfer failed: {e}")
    ```"#
);
create_exception!(
    transferred._native,
    SourceError,
    ElError,
    "Source read failed (file missing, malformed Parquet, etc.)."
);
create_exception!(
    transferred._native,
    DestinationError,
    ElError,
    "Destination write failed (permission denied, disk full, schema mismatch)."
);
create_exception!(
    transferred._native,
    ArrowError,
    ElError,
    "Arrow schema or array conversion failed."
);
create_exception!(
    transferred._native,
    IoError,
    ElError,
    "Filesystem I/O error not attributable to source or destination logic."
);

pub fn to_pyerr(err: CoreError) -> PyErr {
    match err {
        CoreError::Source(msg) => SourceError::new_err(msg),
        CoreError::Destination(msg) => DestinationError::new_err(msg),
        CoreError::Arrow(e) => ArrowError::new_err(e.to_string()),
        CoreError::Io(e) => IoError::new_err(e.to_string()),
        CoreError::Other(msg) => ElError::new_err(msg),
    }
}

pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ElError", py.get_type::<ElError>())?;
    m.add("SourceError", py.get_type::<SourceError>())?;
    m.add("DestinationError", py.get_type::<DestinationError>())?;
    m.add("ArrowError", py.get_type::<ArrowError>())?;
    m.add("IoError", py.get_type::<IoError>())?;
    Ok(())
}
