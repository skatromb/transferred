//! Regenerate `python/transferred/_native/__init__.pyi` from `#[gen_stub_*]` annotations.
//! Run with `cargo run --bin stub_gen -p transferred-py`.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use pyo3_stub_gen::Result;

/// `create_exception!` macros are invisible to `pyo3-stub-gen`. Declare the
/// exception hierarchy here so type checkers can resolve it.
const EXCEPTIONS_TRAILER: &str = r#"
class TransferredError(Exception):
    """Base exception for all `transferred` failures.

    Subclasses: `SourceError` (and `EmptySourceError`), `DestinationError`, `ArrowError`, `IoError`.

    Example:
        ```py
        >>> from transferred import Transfer, TransferredError
        >>> try:
        ...     Transfer(source=..., destination=...).run()
        ... except TransferredError as e:
        ...     print(f"transfer failed: {e}")
        ```
    """

class SourceError(TransferredError):
    """Source read failed (file missing, malformed Parquet, etc.)."""

class EmptySourceError(SourceError):
    """Source produced no batches — nothing to transfer."""

class DestinationError(TransferredError):
    """Destination write failed (permission denied, disk full, schema mismatch)."""

class ArrowError(TransferredError):
    """Arrow schema or array conversion failed."""

class IoError(TransferredError):
    """Filesystem I/O error not attributable to source or destination logic."""
"#;

fn main() -> Result<()> {
    let stub = _native::stub_info()?;
    stub.generate()?;
    let stub_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("python/transferred/_native/__init__.pyi");
    OpenOptions::new()
        .append(true)
        .open(&stub_path)?
        .write_all(EXCEPTIONS_TRAILER.as_bytes())?;
    Ok(())
}
