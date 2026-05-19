# Full Python gate: lint, types, tests.
.PHONY: check-python
check-python: lint-python typecheck-python test-python

# Provision venv + build extension. Other Python targets depend on this.
.PHONY: setup-python
setup-python:
	cd crates/transferred-py && \
		uv sync --group dev && \
		uv run --no-sync maturin develop --uv

# Lint Python sources.
.PHONY: lint-python
lint-python: setup-python
			cd crates/transferred-py && uv run --no-sync ruff check python tests

# Type-check Python sources against the auto-generated `_native` stubs.
# Catches drift like missing exception classes in the stub.
.PHONY: typecheck-python
typecheck-python: setup-python
	cd crates/transferred-py && uv run --no-sync ty check python tests

# Run pytest. Same entry point for local + CI.
.PHONY: test-python
test-python: setup-python
	cd crates/transferred-py && uv run --no-sync pytest tests
