//! Regenerate `python/transferred/_native.pyi` from `#[gen_stub_*]` annotations.
//! Run with `cargo run --bin stub_gen -p transferred-py`.

use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    let stub = _native::stub_info()?;
    stub.generate()?;
    Ok(())
}
