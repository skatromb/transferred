# `transferred` — Plan

Versioned roadmap and progress. Architecture and contracts in [DESIGN.md](./DESIGN.md).

Legend: `[x]` done · `[~]` in progress · `[ ]` pending.

## 0.0.1 — first publishable, ergonomics test

Goal: end-to-end Python wheel with Parquet round-trip published to PyPI + corresponding Rust crates published to crates.io. Validates the FFI seam and the publish pipeline, not connector breadth.

**Scope:**

- Parquet source + destination only. No Postgres, no BigQuery.
- Python API: `Transfer(source=..., destination=...).run() -> RunReport`. Accepts `Parquet` source + destination only.
- Use source-inferred Arrow schemas, no `schema=` / `schema_overrides=` kwargs yet.
- `RunReport`, `ElError` hierarchy surfaced into Python.
- License: MIT, `LICENSE` file at repo root.
- Workspace version shared across crates. Untie later if cadence diverges.

**Tasks:**

- [x] Workspace skeleton (`Cargo.toml`, `rust-toolchain.toml`, per-crate dirs).
- [x] `transferred-core` traits (`Source`, `Destination`, `Transfer`, `ElError`, `RunReport`, `BatchStream`).
- [x] `transferred-parquet` source (async).
- [x] `transferred-parquet` destination (async, atomic tmp+rename, zstd).
- [x] Parquet round-trip dogfood test (wide schema, AAA structure).
- [x] `dev` feature flag with `TestSource` / `TestDestination`.
- [x] **Rename crates `el-*` → `transferred-*`** (workspace, paths, imports).
- [x] **LICENSE file** at repo root (MIT).
- [x] **Per-crate `description`, `readme`, `keywords`, `categories`** in Cargo.toml — crates.io rejects without them.
- [x] **`transferred-py` crate** — PyO3 module, mixed-mode maturin layout (`python/transferred/`).
  - [x] `Transfer` Python class wrapping Rust `Transfer`.
  - [x] `Parquet` source + destination Python wrappers.
  - [x] `RunReport` Python class (attribute access, `__repr__`).
  - [x] `ElError` Python exception hierarchy (`transferred.ElError` root + subclasses for source/destination/schema failures).
  - [x] Auto-generated `_native.pyi` via `pyo3-stub-gen` + `cargo run --bin stub_gen -p transferred-py`; `py.typed` marker.
- [x] **`pyproject.toml`** — maturin config, wheel targets cp314 + cp314t. No cp313.
- [x] **Python test harness (reproducible, CI-portable).**
  - [x] PEP 735 dev dependency group in `crates/transferred-py/pyproject.toml` (`maturin`, `pytest`, `pyarrow`, `ruff`, `ty`).
  - [x] `crates/transferred-py/tests/` — pytest suite. First test: Parquet round-trip via Python API.
  - [x] `make test-python` / `lint-python` / `typecheck-python` / `check-python` targets — single entry points used by local + CI.
  - [x] CI workflow calls `make python-check` (runs ruff + ty + pytest). No CI-only side path.
- [x] **Stub-gen drift guard** — `cargo run --bin stub_gen -p transferred-py` + `git diff --exit-code` on `crates/transferred-py/python/transferred/_native/__init__.pyi`. Wired into CI PR gate; fails PR if stubs drift from annotations.
- [x] **Reserve names** on crates.io and PyPI. After `transferred-py` exists so PyPI wheel reservation is co-located with Rust crate reservation.
- [x] **CI: PR gate workflow** (`.github/workflows/checks.yml`).
  - [x] `cargo fmt --check`, `cargo clippy --workspace --tests`.
  - [x] `cargo test --workspace --features transferred-core/dev`.
  - [x] `cargo run --bin stub_gen -p transferred-py` + `git diff --exit-code` on `crates/transferred-py/python/transferred/_native/__init__.pyi` — fails PR if stubs drift from code.
  - [x] `make python-check` (ruff + ty + pytest).
  - [x] rust-cache for incremental builds.
- [x] **CI: release workflow** (`.github/workflows/release.yml`, tag-triggered).
  - [x] Cargo publish each workspace crate in dep order: core → parquet → py. Tolerant of already-uploaded versions on re-run. `crates-io` GitHub environment gates the job; `CARGO_REGISTRY_TOKEN` scoped to that environment.
  - [x] Build wheels via maturin-action matrix (Linux x86_64/aarch64, macOS arm64, Windows x86_64). macOS x86_64 dropped — slow runner queue, shrinking user base.
  - [x] Publish to PyPI via Trusted Publishers (OIDC, no token in repo). `pypi` GitHub environment with required-reviewer rule.
- [x] **Update README.md** to match the published 0.0.1 surface (install command, working Python example, crate links).
- [x] **Cut 0.0.1 tag.** Published to crates.io (`transferred-core`, `transferred-parquet`, `transferred-py`) and PyPI (`transferred`).

## 0.0.2 — Python-native iterable source

Goal: load API responses and Python-native data without forcing the user through Parquet.

**Scope:**

- `ArrowSource` (in `transferred.arrow`) — accepts a `pa.RecordBatchReader` and exposes it to Rust via the Arrow C Data Interface.
- `_iterable_to_arrow` (in `transferred.iterable`) — converts iterables of `dict | dataclass | pydantic.BaseModel` rows into an `ArrowSource`. Tuples not supported (no column names). Module-direction: `iterable` depends on `arrow`, not the other way.
- Auto-coercion in `Transfer(source=...)` — raw iterables (excluding `str`/`bytes`/`bytearray`/`dict`) routed through `_iterable_to_arrow`; existing sources pass through.
- Schema inference from first batch via `pa.RecordBatch.from_pylist`.
- Destination schema applies as coercion target (iterable has no native schema of its own).
- pyarrow is an optional dep via `transferred[arrow]` extra (`transferred[iterable]` aliased). Base install stays lean; missing pyarrow at `ArrowSource` construction raises `ImportError` with install hint.

**Tasks:**

- [x] `ArrowSource` class (`transferred.arrow`) + `_iterable_to_arrow` converter (`transferred.iterable`).
- [x] Source coercion dispatcher in Python `Transfer` wrapper: `Iterable` → `_iterable_to_arrow`, source → passthrough.
- [x] Per-chunk pyarrow conversion (chunks freed as Rust consumes them; users steered toward generators over lists via docstring).
- [x] Tests: list-of-dicts, generator, dataclass, pydantic, mixed-null columns, auto-coercion, dict rejection.
- [x] **Docstrings with usage examples** on every public Python class (`Transfer`, `ParquetSource`, `ParquetDestination`, `RunReport`, `ElError`, `SourceError`, `DestinationError`, `ArrowError`, `IoError`). Surface in IDE hover.
- [x] Tighten Python `Transfer` type annotations: replace `source: Any, destination: Any` on `__new__` with `Source | Iterable[dict | dataclass | BaseModel]` for source + concrete destination type. May need a `Source` Protocol or native pyclass union.
- [ ] Ergonomics test
- [ ] Deploy 0.0.2

## 0.0.3 — Intermediate Parquet -> Parquet perf test

**Scope:** intermediate Parquet performance test. Includes memory profiling across iterable + Parquet paths so regressions are caught later.

**Tasks:**

- [ ] Look at real size of data without compression
- [ ] Test against natural parquet lib
- [ ] Test load to 1 file or to multiple files
- [ ] Adapt `Path` to what's expected in Parquet (wildcards like `path/to/partitions/*.parquet`)
- [ ] Memory profile across workloads: iterable (generator vs list), Parquet round-trip, varying `_BATCH_SIZE`. Establish baseline numbers.
- [ ] Reusable memory-monitoring harness (peak RSS / Arrow buffer accounting) wired into a perf suite. Future PRs can assert ceilings to prevent regressions.
- [ ] Docs: memory profile, `batch_size` tuning guidance.

## 0.1.0 — Postgres source → BigQuery destination

Goal: original thesis. Atomic full load from PG to BQ.

**Scope:**

- `transferred-postgres` source: `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` → Arrow `RecordBatch`. Both `table=` and `query=` compile to COPY. Docker PG+PostGIS fixture for tests.
- `transferred-bigquery` destination: Storage Write API in `pending` mode against transient staging table → server-side copy job `WRITE_TRUNCATE` from staging into final → `DROP TABLE staging`. No GCS staging.
- BQ schema vocabulary in Python (`"INT64"`, `"NUMERIC(18, 4)"`, `"GEOGRAPHY"`, `bigquery.SchemaField`).
- Schema inference from `information_schema`.
- Auth via `gcp_auth` (ADC, service-account JSON, gcloud, workload identity).
- Schema redesign — destination-owned vocab, trait additions (`parse_user_schema`, `apply_overrides`, `to_destination_schema`). Designed against PG + BQ + Parquet concretely.
- Coercion engine — runtime cast from inferred Arrow schema to canonical schema. Tier 1 auto, Tier 2 warn, Tier 3 fail.
- Type registry initial coverage: primitives, `arrow.json`, `arrow.uuid`, ranges (expand), `geography(_, 4326)` → BQ `GEOGRAPHY` (Tier 1), `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY` (Tier 2 warn). Other tier-3 cases refused with `columns=`/`skip_columns=` workaround.
- `tracing` → Python `logging` bridge.

**Tasks:**

- [ ] `transferred-postgres` connect + COPY binary parser.
- [ ] PG → Arrow type mapping (per DESIGN.md coverage table).
- [ ] Integration test: docker-compose PG+PostGIS fixture.
- [ ] `transferred-bigquery` Storage Write client (tonic + googleapis).
- [ ] Atomic staging-table + copy-replace + drop-staging flow.
- [ ] Auth integration (`gcp_auth`).
- [ ] BQ env-gated integration test.
- [ ] CI: docker PG service for PR gate.
- [ ] Logging bridge crate.
- [ ] **Schema redesign** — destination-owned vocab, trait additions (`parse_user_schema`, `apply_overrides`, `to_destination_schema`). Implement for Parquet, PG, BQ in one pass.
- [ ] **Coercion engine** — runtime cast from inferred Arrow schema to canonical schema. Tier 1 auto, Tier 2 warn, Tier 3 fail.

## 0.1.1 — Postgres destination, BigQuery source

**Scope:**

- `transferred-postgres` destination: `COPY ... FROM STDIN`, atomic swap via `BEGIN; DROP target; RENAME staging; COMMIT;`. Client-side schema compare needed (no server-side enforcement like BQ).
- `transferred-bigquery` source: Storage Read API.
- Round-trip integration tests (PG ↔ BQ).

## 0.2 — widen the matrix

- Add dependabot
- S3 destination (Parquet) via `object_store`.
- GCS destination (Parquet) — nearly free once S3 works.
- `mode="append"` where atomic-replace is wrong.
- Partitioned Parquet directory destination (enables true partition parallelism).
- Type registry expansion driven by new connectors.
- Concurrent transfers in one process — task-count cap, optional byte-aware semaphore.

## Later — deliberately deferred

- Incremental loads. Deferred; model TBD.
- CRS reprojection (`proj` FFI), `ST_MakeValid`, Z/M handling.
- Hstore / ltree / composite promotion from `arrow.opaque` to structured Arrow forms.
- `strict_mode` flag.
- Resumability after partial failure.
- CLI
- Transformations beyond what type mapping forces.
- Streaming and CDC.
- Byte-aware memory semaphore (when partition parallelism reveals skew issues).

## Never ~~say never~~
- YAML/TOML config.
