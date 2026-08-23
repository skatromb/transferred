# Full gate: Rust + Python + stub drift.
.PHONY: check
check: rust-check python-check stubs-check

# Integration tests, gated behind each crate's `integration` feature. Needs Docker.
.PHONY: check-integration
check-integration:
	@cargo test -p transferred-postgres --features integration



# ============================================================================
# Rust
# ============================================================================

# Full Rust gate: fmt, clippy, tests, supply chain audit.
.PHONY: rust-check
rust-check: fmt clippy cargo-test deny

# Format in place. Auto-fixes locally; CI fails if anything changed.
.PHONY: fmt
fmt:
	@cargo fmt --all
	@if [ -n "$$CI" ]; then git diff --exit-code -- '*.rs'; fi

# Lint with clippy, deny warnings. Enables `integration` so gated test targets are linted too.
# Avoid `--all-features`: pyo3 `extension-module` breaks linkage outside maturin.
.PHONY: clippy
clippy:
	@cargo clippy --workspace --tests \
		--features transferred-core/dev,transferred-postgres/integration -- -D warnings

# Run Rust tests.
.PHONY: cargo-test
cargo-test:
	@cargo test --workspace --features transferred-core/dev

# Install cargo-deny for supply chain audits.
.PHONY: deny-install
deny-install:
	@cargo install cargo-deny --locked

# Audit advisories, licenses, bans and sources against `deny.toml`.
.PHONY: deny
deny: deny-install
	@cargo deny --locked check



# ============================================================================
# Stubs (Rust-generated, Python-consumed)
# ============================================================================

# Fail if regen changes the current on-disk stub
.PHONY: stubs-check
stubs-check: STUB_PYI := crates/transferred-py/python/transferred/_native/__init__.pyi
stubs-check:
	@cp $(STUB_PYI) $(STUB_PYI).bak
	@$(MAKE) -s stubs
	@if ! diff -q $(STUB_PYI).bak $(STUB_PYI) >/dev/null; then \
		echo "stub drift — regenerated stub differs, commit the update:"; \
		diff $(STUB_PYI).bak $(STUB_PYI) || true; \
		rm -f $(STUB_PYI).bak; exit 1; \
	fi
	@rm -f $(STUB_PYI).bak

# Regenerate `_native.pyi` stubs from `#[gen_stub_*]` annotations.
.PHONY: stubs
stubs:
	@cargo run --bin stub_gen -p transferred-py


# ============================================================================
# Python
# ============================================================================

# Full Python gate: lint, types, tests.
.PHONY: python-check
python-check: ruff wps ty pytest

# Provision venv + build extension. Other Python targets depend on this.
.PHONY: python-setup
python-setup:
	@cd crates/transferred-py && \
		uv sync --group dev && \
		uv run --no-sync maturin develop --uv

# Lint + format Python sources. Auto-fixes locally; CI fails if anything changed.
.PHONY: ruff
ruff: python-setup
	@cd crates/transferred-py && \
		uv run --no-sync ruff format . ../../examples ../../perf && \
		uv run --no-sync ruff check . ../../examples ../../perf
	@if [ -n "$$CI" ]; then git diff --exit-code -- '*.py'; fi

# Lint with wemake-python-styleguide. `WPS` rules only — see `.flake8`.
.PHONY: wps
wps: python-setup
	@uv run --project crates/transferred-py --no-sync flake8 .

# Type-check Python sources against the auto-generated `_native` stubs.
.PHONY: ty
ty: python-setup
	@cd crates/transferred-py && \
	uv run --no-sync ty check

# Run pytest. Same entry point for local + CI.
.PHONY: pytest
pytest: python-setup
	@cd crates/transferred-py && \
	uv run --no-sync pytest

# Dependency groups synced before a Python build. `perf` widens it with its baselines.
PYTHON_GROUPS := --group dev

# Rebuild extension in release mode. Use for benchmarks / perf testing.
.PHONY: python-dev-build
python-dev-build:
		@cd crates/transferred-py && \
			uv sync $(PYTHON_GROUPS) && \
			uv run --no-sync maturin develop --uv --release



# ============================================================================
# Performance
# ============================================================================

# Run perf workloads against duckdb. Forces a release-mode build — debug builds skew numbers. Needs Docker.
# `check`, `ruff`, `ty` and `pytest` undo that by installing debug, don't run them while perfing.
.PHONY: perf
perf: PYTHON_GROUPS := --group dev --group perf
perf: python-dev-build
	@uv run --project crates/transferred-py --no-sync python -m perf.run

# Same, with dlt's four legs. Minutes each, so they stay out of the quick loop.
.PHONY: perf-full
perf-full: export PERF_DLT := 1
perf-full: perf

# A/B our legs across published releases — `PERF_VERSIONS=0.1.1,0.1.2 make perf-versions`.
.PHONY: perf-versions
perf-versions: PYTHON_GROUPS := --group dev --group perf
perf-versions: python-dev-build
	@uv run --project crates/transferred-py --no-sync python -m perf.versions

# Types each engine's Postgres target lands. Scale-independent — `PERF_ROW_NUM=100000 make fidelity`.
.PHONY: fidelity
fidelity: PYTHON_GROUPS := --group dev --group perf
fidelity: python-dev-build
	@uv run --project crates/transferred-py --no-sync python -m perf.fidelity

# Profile one workload's on-CPU time. macOS only. `make profile WORKLOAD=parquet_to_postgres`.
.PHONY: profile
profile: PYTHON_GROUPS := --group dev --group perf
profile: python-dev-build
	@crates/transferred-py/.venv/bin/python -m perf.profile $(WORKLOAD)



# ============================================================================
# Release
# ============================================================================

# Read workspace version from cargo metadata.
VERSION := $(shell cargo metadata --no-deps --format-version 1 \
	| jq -r '.packages[] | select(.name=="transferred-core") | .version')

# Refresh Cargo.lock after manually editing `version = ...` in Cargo.toml.
.PHONY: bump-lock
bump-lock:
	@cargo update -p transferred-core -p transferred-files -p transferred-postgres -p transferred-py

# Pre-release validation before the version bump.
.PHONY: pre-release
pre-release: check check-integration examples

# Pre-flight: on main, clean tree, in sync with origin.
.PHONY: release-check
release-check:
	@[ "$$(git rev-parse --abbrev-ref HEAD)" = "main" ] \
		|| { echo "release-check: must be on main"; exit 1; }
	@git diff --quiet && git diff --cached --quiet \
		|| { echo "release-check: working tree not clean"; exit 1; }
	@git fetch origin main --quiet
	@[ "$$(git rev-parse HEAD)" = "$$(git rev-parse origin/main)" ] \
		|| { echo "release-check: local main not in sync with origin/main"; exit 1; }
	@echo "release-check: ok (version $(VERSION))"

# Cut and push annotated tag `vX.Y.Z` matching the workspace version.
.PHONY: release-tag
release-tag: release-check
	@git rev-parse "v$(VERSION)" >/dev/null 2>&1 \
		&& { echo "tag v$(VERSION) already exists"; exit 1; } \
		|| true
	@git tag -a "v$(VERSION)" -m "v$(VERSION)"
	@git push origin "v$(VERSION)"
	@echo "pushed tag v$(VERSION)"

# Move tag `vX.Y.Z` to current main (delete local+remote, recreate). Use to re-cut a version before publish.
.PHONY: release-retag
release-retag: release-check
	@git push origin ":refs/tags/v$(VERSION)" 2>/dev/null || true
	@git tag -d "v$(VERSION)" 2>/dev/null || true
	@git tag -a "v$(VERSION)" -m "v$(VERSION)"
	@git push origin "v$(VERSION)"
	@echo "re-tagged v$(VERSION)"

# Check validity of every examples/*.py against the current build. Needs Docker for Postgres.
.PHONY: examples
examples: python-setup
	@set -e; \
	docker run -d --rm --name transferred_examples -p 5432:5432 \
		-e POSTGRES_PASSWORD=pw imresamu/postgis:18-3.6 >/dev/null; \
	trap 'docker stop transferred_examples >/dev/null' EXIT; \
	until docker exec transferred_examples pg_isready -q; do sleep 1; done; \
	for file in examples/*.py; do \
		out=$$( (cd examples && \
			uv run --python ../crates/transferred-py/.venv \
				--with pyarrow --with polars --with pydantic \
				python "$$(basename $$file)") 2>&1 ) \
			&& echo "ok $$file" \
			|| { echo "fail $$file"; echo "$$out"; exit 1; }; \
	done
