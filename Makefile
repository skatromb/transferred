# Provision venv, build extension, run pytest. Same entry point for local + CI.
.PHONY: test-python
test-python:
	cd crates/transferred-py && \
		uv sync --group dev && \
		uv run --no-sync maturin develop --uv && \
		uv run --no-sync pytest tests
