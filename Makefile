# Full gate: Rust + Python.
.PHONY: check
check: rust-check python-check


# ============================================================================
# Rust
# ============================================================================

# Full Rust gate: fmt, clippy, tests.
.PHONY: rust-check
rust-check: fmt clippy cargo-test

# Check formatting.
.PHONY: fmt
fmt:
	@cargo fmt --all -- --check

# Lint with clippy, deny warnings.
# Avoid `--all-features`: pyo3 `extension-module` breaks linkage outside maturin.
.PHONY: clippy
clippy:
	@cargo clippy --workspace --tests --features transferred-core/dev -- -D warnings

# Run Rust tests.
.PHONY: cargo-test
cargo-test:
	@cargo test --workspace --features transferred-core/dev


# ============================================================================
# Python
# ============================================================================

# Regenerate `_native.pyi` stubs from `#[gen_stub_*]` annotations.
.PHONY: stubs
stubs:
	@cargo run --bin stub_gen -p transferred-py

# Fail if regenerated stubs differ from committed file.
.PHONY: stubs-check
stubs-check: stubs
	@git diff --exit-code crates/transferred-py/python/transferred/_native/__init__.pyi

# Full Python gate: lint, types, tests.
.PHONY: python-check
python-check: ruff ty pytest

# Provision venv + build extension. Other Python targets depend on this.
.PHONY: python-setup
python-setup:
	@cd crates/transferred-py && \
		uv sync --group dev && \
		uv run --no-sync maturin develop --uv

# Lint + format-check Python sources.
.PHONY: ruff
ruff: python-setup
	@cd crates/transferred-py && \
		uv run --no-sync ruff format --check && \
		uv run --no-sync ruff check

# Type-check Python sources against the auto-generated `_native` stubs.
# Catches drift like missing exception classes in the stub.
.PHONY: ty
ty: python-setup
	@cd crates/transferred-py && \
	uv run --no-sync ty check

# Run pytest. Same entry point for local + CI.
.PHONY: pytest
pytest: python-setup
	@cd crates/transferred-py && \
	uv run --no-sync pytest
