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
- [x] Ergonomics test
- [x] Deploy 0.0.2

## 0.0.3 — Intermediate Parquet -> Parquet perf test

**Scope:** intermediate Parquet performance test. Includes memory profiling across iterable + Parquet paths so regressions are caught later.

**Findings so far** (branch `perf-harness-investigation`, not merged):

- Parquet → Parquet streaming is sound. dhat shows Rust-side heap peak flat at ~24 MiB from 1M → 40M rows (375 MB → 15.4 GB cumulative allocations, but live working set bounded).
- Throughput steady at ~13.1M rows/s across 4M / 40M / 400M / 4B row inputs.
- RSS plateaus at ~330 MiB even at 4B rows. Growth observed in short runs is allocator freelist ramp; macOS libmalloc decommits eventually (saw 334 → 133 MiB mid-run).
- CPU/wall ≈ 1.0 at steady state — single-core bound. Real parallelism opportunity in `transferred-parquet`.
- memray (Python heap) matches dhat — Python adds negligible heap on the Parquet → Parquet path. The 90 MiB RSS overhead vs Rust-only is dynamic linker + pyarrow `.so` mmap, not heap.
- Throughput ≈ parity with raw pyarrow when row-group sizes match (transferred 13.1M vs pyarrow 13.6M rows/s). An earlier "transferred wins throughput" reading came from pyarrow's default `iter_batches(batch_size=65536)` creating ~16x more row groups than parquet-rs's `DEFAULT_MAX_ROW_GROUP_ROW_COUNT = 1M`, which compresses worse — not from transferred being faster.
- Real advantage is **memory**: transferred ~2x less RSS on Parquet→Parquet (95 vs 224 MB), ~6x less on iterable-generator→Parquet (103 vs 631 MB). Streaming holds tight where pyarrow buffers.
- Iterable-list form: 1.5 GB RSS at 4M rows. Docs must steer users to generators.


Tested @ 4M rows. Baselines use `batch_size=1_000_000` to match parquet-rs's
`DEFAULT_MAX_ROW_GROUP_ROW_COUNT` — without it pyarrow writes ~16x more row
groups (its `iter_batches` default is 65536), compression diverges, and output
sizes aren't comparable.

┌───────────────────────────────────────┬────────┬────────┬────────┬────────┐
│               Workload                │ wall s │ RSS MB │ rows/s │ out MB │
├───────────────────────────────────────┼────────┼────────┼────────┼────────┤
│ transferred parquet→parquet           │ 0.31   │ 94.7   │ 13.1M  │ 13.1   │
├───────────────────────────────────────┼────────┼────────┼────────┼────────┤
│ baseline pyarrow parquet→parquet      │ 0.29   │ 224.1  │ 13.6M  │ 13.4   │
├───────────────────────────────────────┼────────┼────────┼────────┼────────┤
│ transferred iterable-gen→parquet      │ 1.39   │ 103.2  │ 2.9M   │ 13.1   │
├───────────────────────────────────────┼────────┼────────┼────────┼────────┤
│ baseline pyarrow iterable-gen→parquet │ 1.68   │ 630.7  │ 2.4M   │ 13.4   │
├───────────────────────────────────────┼────────┼────────┼────────┼────────┤
│ transferred iterable-list→parquet     │ 0.78   │ 1527.6 │ 5.1M   │ 13.1   │
└───────────────────────────────────────┴────────┴────────┴────────┴────────┘

**Tasks:**

- [x] Perf harness (Python): peak RSS via `os.wait4` rusage, CPU via rusage, timeline RSS via `psutil` sampler. Subprocess-per-workload to isolate setup from run. Workloads emit JSON on stdout.
- [x] Workload: Parquet → Parquet single-file via `transferred`, `PERF_ROWS=N` env override.
- [x] Workloads: iterable-generator → Parquet, iterable-list → Parquet.
- [x] Baseline: Parquet → Parquet + iterable-generator → Parquet via raw `pyarrow.parquet`, no `transferred`.
- [x] Land the harness on `main`.
- [x] Wildcard `Path` support — `ParquetSource` accepts `path/to/partitions/*.parquet`.
- [ ] Workload: Parquet → Parquet multi-file.
- [ ] `FileFormat` trait. Implementations: `Parquet(compression, row_group_size)`, `Avro`, `Json`, `Csv`.
- [ ] File-shaped destinations carry an optional `format`: `LocalFilesystem(path, format=None)`, `S3(bucket, key, format=None)`, `GCS(...)`.
- [ ] Row-protocol destinations have no `format` knob: `BigQuery(...)`, `Postgres(...)`.
- [ ] `format` resolution:
  - [ ] File source + no `format` arg: inherit source's format (path-extension first, sniff bytes on ambiguity).
  - [ ] File source + explicit `format`: convert.
  - [ ] Non-file source + no `format`: default to `Parquet()`.
  - [ ] Non-file source + explicit `format`: convert.

## Interlude

Update DESIGN.md after all Interlude decisions are made.

### Schema inference

I think I made it wrong deciding that schema should be inferred `Destination -> Arrow -> Source`.
It's actually should (or also could?) be the opposite: we need to preserve `Source`'s schema.
What should we do with schema resilience then? Just fail when `Source` schema is not compatible
anymore with existing `Destination` — that means breaking change happened at the `Source`,
so we need to raise in that case.

### `Source` and `Destination` abilities design

If we'll make each `Source` and `Destination` development lean, so that you may implement just basics and they'll be ready to use, how could full functionality be pluggable? If we use traits and implementations as a marker, is it possible to check:
if trait is implemented:
    do incremental_load
else:
    do full_load
?

Or should we do this in other way?

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
- Airflow / Dagster / whatever is popular operators
- Multiple destinations `Transfer`s
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
