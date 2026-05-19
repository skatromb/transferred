//! Regenerate `python/transferred/_native/__init__.pyi` from `#[gen_stub_*]` annotations.
//! Run with `cargo run --bin stub_gen -p transferred-py`.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use pyo3_stub_gen::Result;

/// `create_exception!` macros are invisible to `pyo3-stub-gen`. Declare the
/// exception hierarchy here so type checkers can resolve it.
const EXCEPTIONS_TRAILER: &str = "\n\
class ElError(Exception): ...\n\
class SourceError(ElError): ...\n\
class DestinationError(ElError): ...\n\
class ArrowError(ElError): ...\n\
class IoError(ElError): ...\n";

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
