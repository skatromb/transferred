//! Regenerate `python/transferred/_native/__init__.pyi` from `#[gen_stub_*]` annotations.
//! Run with `cargo run --bin stub_gen -p transferred-py`.

use _native::stub_info;
use pyo3_stub_gen::Result;

fn main() -> Result<()> {
    stub_info()?.generate()
}
