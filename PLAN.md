# `transferred` — Plan

Versioned roadmap and progress. Architecture and contracts in [DESIGN.md](./docs/design/DESIGN.md).

Legend: `[x]` done · `[~]` in progress · `[ ]` pending.

## 0.0.1 — first publishable, ergonomics test

Goal: end-to-end Python wheel with Parquet round-trip published to PyPI + corresponding Rust crates published to crates.io. Validates the FFI seam and the publish pipeline, not connector breadth.

**Scope:**

- Parquet source + destination only. No Postgres, no BigQuery.
- Python API: `Transfer(source=..., destination=...).run() -> RunReport`. Accepts `Parquet` source + destination only.
- Use source-inferred Arrow schemas, no `schema=` kwarg yet.
- `RunReport`, `TransferredError` hierarchy surfaced into Python.
- License: MIT, `LICENSE` file at repo root.
- Workspace version shared across crates. Untie later if cadence diverges.

**Tasks:**

- [x] Workspace skeleton (`Cargo.toml`, `rust-toolchain.toml`, per-crate dirs).
- [x] `transferred-core` traits (`Source`, `Destination`, `Transfer`, `TransferredError`, `RunReport`, `BatchStream`).
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
  - [x] `TransferredError` Python exception hierarchy (`transferred.TransferredError` root + subclasses for source/destination/schema failures).
  - [x] Auto-generated `_native.pyi` via `pyo3-stub-gen` + `cargo run --bin stub_gen -p transferred-py`; `py.typed` marker.
- [x] **`pyproject.toml`** — maturin config, wheel targets cp314 + cp314t. No cp313.
- [x] **Python test harness (reproducible, CI-portable).**
  - [x] PEP 735 dev dependency group in `crates/transferred-py/pyproject.toml` (`maturin`, `pytest`, `pyarrow`, `ruff`, `ty`).
  - [x] `crates/transferred-py/tests/` — pytest suite. First test: Parquet round-trip via Python API.
  - [x] `make test-python` / `lint-python` / `typecheck-python` / `check-python` targets — single entry points used by local + CI.
  - [x] CI workflow calls `make python-check` (runs ruff + ty + pytest). No CI-only side path.
- [x] **Stub-gen drift guard** — `cargo run --bin stub_gen -p transferred-py` + `git diff --exit-code` on `crates/transferred-py/python/transferred/_native/__init__.pyi`. Wired into CI PR gate; fails PR if stubs drift from annotations.
- [x] **Reserve names** on crates.io and PyPI. After `transferred-py` exists so PyPI wheel reservation is co-located with Rust crate reservation.
- [x] **CI: PR gate workflow** (`.github/workflows/check.yml`).
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
- [x] **Docstrings with usage examples** on every public Python class (`Transfer`, `ParquetSource`, `ParquetDestination`, `RunReport`, `TransferredError`, `SourceError`, `DestinationError`, `ArrowError`, `IoError`). Surface in IDE hover.
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
- [x] **New crate `transferred-files`** — absorbs `transferred-parquet` (which ceases to exist). Owns the `FileFormat` trait, format codecs, and the `Files` source + destination. Update workspace `Cargo.toml`, all imports, `transferred-py` dep. Pre-1.0 crate removal — allowed (DESIGN.md versioning).
  - [x] `release.yml` publish order → core → files → py (drop parquet).
- [x] `FileFormat` trait (in `transferred-files`) — symmetric `read` (decode → Arrow) + `write` (encode ← Arrow). Promote to `transferred-core` only if formats ever split into their own crates.
- [x] `Parquet(compression="zstd")` codec — implements `FileFormat` (both read + write). Keeps the parquet-rs default row-group size (1,048,576); `row_group_size` knob dropped from the surface for now. `Avro`/`Csv` — later, in-crate.
- [x] `FilesSource`/`FilesDestination` (local), format-agnostic, delegate codec to the resolved `FileFormat`. Replace `ParquetSource`/`ParquetDestination` — hard removal, no shim. **Suffix convention everywhere** (`{Name}Source`/`{Name}Destination`) — avoids the common Files→Files import clash; applies to `PostgresSource`/`PostgresDestination`, `BigQueryDestination`, `S3Destination` too. Internal `_FilesSource`/`_FilesDestination`.
  - [x] Directory output (default) — `path` is a directory (overwritten if present), one `part-NNNNN.<ext>` per source partition; tmp dir + atomic dir rename.
  - [x] `single_file=True` — flatten partitions into one part inside the directory (tmp + rename). A flag, not extension inference — no path-shape ambiguity (dotted dirs, type conflicts).
  - [x] `FormatWrite::file_extension()` → part-file extension (Parquet → `parquet`).
  - [x] `RunReport.written_objects: Vec<String>` — generic identifiers of what each destination wrote (file paths, S3 URIs, `project.dataset.table`, `schema.table`). Files store `path.display()`. Empty when nothing written. Keeps the report flat — no per-destination report structs.
  - [x] `EmptySource` error variant + Python `EmptySourceError` (subclass of `SourceError`), raised when the source yields zero batches across all partitions.
- [x] Python: `Files` source + destination wrappers + `Parquet` format wrapper; remove the Parquet wrappers; stub-gen regen; docstrings (Args + Example) per AGENTS.md; update tests (dogfood, pytest round-trip + multi-file + single-file + empty-source).
- [x] Perf harness refactor to Files API + multi-file workload:
  - [x] Migrate `transferred` workloads off removed `ParquetSource`/`ParquetDestination` to `FilesSource`/`FilesDestination(format=Parquet(...))`; single-file ones use `single_file=True` to stay comparable to pyarrow baselines.
  - [x] New workload: Parquet → Parquet multi-file (`FilesSource(glob)` → directory output, one part per partition).
  - [x] `emit_result` output_bytes sums files when output is a directory.
- [x] Reserve `transferred-files` on crates.io — name free; `release.yml` claims it in dep order (core → files → py).
- [x] Let's make CI checks to run only on related Rust and Python changes.
- [x] `FilesSource` directory path — `FilesSource(dir)` leaked `Is a directory (os error 21)` from the parquet reader. `GlobOrPaths::resolve` now rejects directories (both glob + path-list inputs) with a clear "is a directory, not a file" message hinting a glob. No auto-expansion — deferred.
- [x] Deploy 0.0.3

## 0.1.0 — Postgres source + destination

Goal: Atomic full load PG → PG and PG → Parquet. Direct type mapping only; formal schema/coercion work deferred to 0.4.

**Scope:**

- `transferred-postgres` source: `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` → Arrow `RecordBatch`. Both `table=` and `query=` compile to COPY. Tests self-provision a throwaway PG+PostGIS container via `testcontainers`.
- Source schema inference via prepared-statement RowDescription (`prepare()` the inner SELECT → column type OID + typmod); uniform across `table=`/`query=`. The COPY binary stream carries no type/name metadata — only length-prefixed field bytes — so types must come from RowDescription, not the stream. PostGIS SRID from typmod.
- `transferred-postgres` destination: atomic full replace — staging table built from the source-derived schema, `COPY ... FROM STDIN`, then `BEGIN; DROP target IF EXISTS; RENAME staging; COMMIT;` (transactional DDL). Source schema wins, silent overwrite — consistent with Files/BQ. Target readable during load; brief exclusive lock only at swap. Indexes/grants/ownership not preserved (full replace); index-preserving replace strategy deferred to 0.4 `on_schema_change` (cf. dlt `replace_strategy`).
- Destination table-creation options bag (additive over source-derived DDL): PG `primary_key=`.
- Direct PG ↔ Arrow ↔ destination type mapping (no canonical vocab, no coercion engine, no user `schema=` — all deferred to 0.4). Coverage: primitives, `arrow.json`, `arrow.uuid`, ranges, PG `geography`/`geometry`. Anything unmapped falls back to `arrow.opaque`.
- `tracing` → Python `logging` bridge.

**Tasks:**

- [x] `transferred-postgres` connect + COPY binary parser.
- [x] `transferred-postgres` destination — `COPY ... FROM STDIN`, atomic swap.
  - No encoder crate: `tokio_postgres::binary_copy::BinaryCopyInWriter` already owns the COPY framing, and every value we send has a `ToSql` impl. Cost is one `Box<dyn ToSql>` per cell, since `write_raw` is row-wise and Arrow is columnar.
  - `pgpq` (MIT) rejected. Probed at 0.11.1 against the schema our source emits: `Interval(MonthDayNano)` hard-errors (`PostgresType::Interval` is only reachable from Arrow `Duration`), `FixedSizeBinary` is routed to the `LargeBinary` encoder and fails its downcast at encode time, extension metadata is ignored (`arrow.json` → `text`), `timestamptz` degrades to `timestamp`, and `PostgresType` carries no precision/scale, `Uuid`, or PostGIS variants. Its real job is *deriving* a PG schema from Arrow, which we don't need — the source already has the true PG types.
  - Target identifiers are split by `parse_ident()` server-side, then requoted, so `schema.table` works without our own SQL parser.
- [x] PG → Arrow type mapping (per DESIGN.md coverage table). Done: primitives, `date`/`timestamp`/`timestamptz`, `uuid`, `json`/`jsonb`, `interval`, `numeric(p,s)`, PostGIS. Left: ranges.
- [x] `numeric(p,s)` → `Decimal128(p,s)`, precision/scale from `Column::type_modifier()`. Bare `numeric` (typmod `-1`) defaults to `Decimal128(38, 9)` + WARN rather than failing — 0.1 has no `schema=` override, so failing would leave a very common column type unusable. 38/9 matches BigQuery `NUMERIC` exactly, so the default reaches BQ uncoerced in 0.2.0.
  - Decode via `rust_decimal` (`db-tokio-postgres`), not by hand: its `postgres/driver.rs` already handles base-10000 digit groups, `weight`, and the `NaN`/`±Infinity` sign flags that Arrow `Decimal128` cannot represent at all.
  - Ceiling: `rust_decimal`'s 96-bit mantissa stops at `2^96 - 1 ≈ 7.92E+28` vs BQ `NUMERIC`'s `9.99E+28`, so values in that band error out despite fitting the declared type. `Decimal256`/BQ `BIGNUMERIC` (76, 38) is unreachable with `rust_decimal` — dropped from 0.1.
  - Bare-`numeric` scale is a trade, not a free choice: the ~28-significant-digit ceiling spends fraction and integer digits from one budget, measured as scale 9 → 20 integer digits, scale 16 → 13, scale 20 → 9. Scale 9 rounds PG's computed scales (division 20 dp, `avg()` 16 dp) but keeps 20 integer digits; scale 20 would preserve them and then fail on any value ≥ 1E+9. Rounding matches PG's own `::numeric(38,9)` cast (verified half-away-from-zero on both signs), so the loss is PG's semantics, not ours — and it now emits a WARN naming the column. **Precondition for revisiting: replace `rust_decimal` with an `i256` wire decoder**, most likely driven by 0.2.0 needing BQ `BIGNUMERIC`.
  - Forces `PgToArrowFn` from a `fn` pointer to a boxed closure: each wire value carries its own `dscale`, but `Decimal128(p,s)` needs every value at scale `s`, so the builder must capture `s` to rescale.
  - Typmod decoding is hand-rolled because nothing publishes it: `tokio-postgres` only carries `type_modifier()`, `postgres-protocol` has no numeric decoder, and `arrow-pg` — the one crate named for this job — is encode-only (Arrow → PG wire for pgwire; no `FromSql` anywhere in it).
- [x] Integration test infra: `testcontainers` PG container started by the test itself, gated behind each crate's `integration` feature (`make check-integration`).
- [x] Integration containers are reaped at exit. They used to strand one container and its anonymous volume per test binary per run — 58 containers and 4.2 GB of volumes over four days — because a container shared by several tests has to live in a `static`, which Rust never drops, so `ContainerAsync::drop` never fired.
  - `testcontainers` has no reaper in 0.27.3 or 0.28.0: cleanup is `Drop` only (`lib.rs:33`), and `watchdog` covers just SIGTERM/SIGINT/SIGQUIT, not a normal exit. Ryuk — the sidecar the Go/Java/Python/.NET ports use — is unimplemented in the Rust port ([#577](https://github.com/testcontainers/testcontainers-rs/issues/577) open since 2024-04, [#949](https://github.com/testcontainers/testcontainers-rs/pull/949) unmerged).
  - Teardown runs from a `#[ctor::dtor]`, the only exit hook libtest leaves. It removes containers by id through the `docker` CLI rather than `ContainerAsync::rm`, because Rust destroys the main thread's locals before atexit runs: building a tokio runtime there dies with `use of std::thread::current() is not possible after the thread's local data has been destroyed`, and every `testcontainers` call needs one.
  - Workspace `unsafe_code` goes `forbid` → `deny`, so the test file can allow what the macro expands to. AGENTS.md already names `tests/` as the place for file-level allows; `forbid` cannot be overridden at all, which made that impossible.
  - Still leaks on `SIGKILL` and if `docker` is absent from `PATH`. Only Ryuk covers those, and it is not worth hand-rolling for them alone.
- [x] Integration test: round-trip PG → PG. Copies each fixture table through `Transfer` and asserts a re-read equals the original, so the destination is checked against the source mapping rather than hand-written SQL. Also covers replacing an existing target and staging-table cleanup.
- [x] CI: `integration` job as a separate PR gate, parallel to `rust`/`python`.
- [x] Swapped `cargo-vet` → `cargo-deny`. vet ran exemption-only (empty `audits.toml`, ~200 self-set `safe-to-deploy` exemptions = auto-approved), so it gated version pins but verified nothing, while every dependency bump cost an exemption edit. `deny.toml` now gates advisories/licenses/bans/sources; `supply-chain/` is gone.
  - `unmaintained = "workspace"` instead of the default `all`: the only hits are `paste` and six `unic-*`, all transitive under `pyo3-stub-gen` → `rustpython-parser` with no upgrade available. The alternative — seven `ignore` entries — is the exemption churn we just removed. Real `vulnerability` advisories are a separate class and stay denied.
  - License allow-list is the encountered set, not a boilerplate list. LGPL-2.1/BSL-1.0/Unlicense/0BSD appear in the tree (`r-efi`, `ryu`, `whoami`, `csv`, `memchr`, `adler2`) but every one is dual-licensed with MIT or Apache-2.0, so nothing copyleft is actually selected.
- [x] Python `PostgresDestination` wrapper: `_PostgresDestination` pyclass, public wrapper, stub regen.
  - No pytest round-trip against a testcontainer. Type mapping is pinned in the Rust integration tests — duplicating it in Python is the split we undid by porting those tests to Rust. The only Python-specific surface is the `extract_destination` downcast, and the docstring doctest exercises it when it constructs `Transfer`, before any I/O. `testcontainers` + Docker in the Python CI job would buy a second copy of coverage that already exists.
  - Docstring examples show the full `Transfer(…).run()`, with `# doctest: +SKIP` only on the line needing a live database, so everything up to it stays verified. Same treatment applied to `PostgresSource`; `FilesDestination` got a real executed `.run()`.
- [x] Integration test: failure paths — a load that dies mid-stream, a swap blocked by a dependent view, and a target name at PG's 63-byte ceiling. Each asserts the target keeps its rows and no staging table survives. The swap case found a real leak: a failed `batch_execute` left the session in an aborted transaction that silently swallowed `drop_staging`. Fixed by running the swap through `Client::transaction()`, whose `Drop` rolls back and leaves the session usable for cleanup.
- [x] Integration test: schema-qualified target (`myschema.tbl`), covering `Target::resolve`'s schema split and `qualify`.
- [x] TLS via `tokio-postgres-rustls`. `sslmode` follows libpq: `prefer` (the default) and `require` encrypt without authenticating the server, `verify-full` checks the chain against the platform trust store. Deviating would break every DSN copied out of a cloud console, which is how most users meet TLS at all.
  - `tokio_postgres::Config` only parses `disable`/`prefer`/`require`, and errors on any unknown DSN key, so `verify-full` is rewritten to the `require` it implies and the intent carried alongside. `verify-ca` stays unsupported — it fails with `invalid value for option 'sslmode'`, which names the option.
  - Roots come from the platform store (`rustls-native-certs`). No `sslrootcert=` yet, so strict mode against RDS (108 private roots in its bundle) or Cloud SQL (per-instance CA) needs the CA installed system-wide.
  - `ring`, not `aws-lc-rs`: the packaged crate ships pre-assembled objects (`ring-0.17.14/build.rs:435`), so the wheel matrix needs no nasm, perl or cmake.
  - `source.rs` and `destination.rs` shared one hand-rolled connect each; they now share `connection.rs`, which also puts the source's `eprintln!` on the logging bridge.
- [x] `tracing` → Python `logging` bridge. `tracing`'s `log` feature emits `log` records while no subscriber is active, and `pyo3-log` forwards them from the `#[pymodule]`. Logger names come from the event target, so each call site sets a short one (`postgres::destination`) and `set_prefix("transferred")` roots the tree: `transferred.postgres.destination`. `Caching::Loggers`, not the default — caching levels would freeze `setLevel` calls made after the first record. Warnings reach the user unconfigured via `logging.lastResort` (stderr, `WARNING`); no handler of ours, which would hijack the app's logging.
- [x] Unsupported-type fallback → `arrow.opaque`. An unmapped OID lands as `Binary` tagged `Opaque::new(type_name, "PostgreSQL")` plus a WARN naming the column, instead of failing the whole transfer over one exotic type. Default, not opt-in, and self-describing rather than the silent `Binary` DESIGN.md forbids. Names come from the catalogue through `tokio-postgres`'s `get_type`, so user-defined types describe themselves as well as built-ins.
  - Needs a `FromSql` whose `accepts` is unconditional: `BinaryCopyOutRow::try_get` checks the type before touching the bytes, so no existing getter can reach an unknown OID. `RawBytes` hands back the field verbatim, which is enough because the COPY stream carries no per-column OID of its own.
  - The destination maps `Binary` → `bytea` and ignores the tag, so a PG → PG trip keeps the bytes and drops the type name. Reading `type_name` back to rebuild DDL would mean canonicalising the values the Opaque spec cautions against, for a same-vendor case too synthetic to earn it.
  - `RunReport.coercions` stays empty: the report is assembled by the destination while this coercion happens in the source. Wiring source-side coercions in is 0.4.0's tiered-reporting task, which bare `numeric` waits on too.
- [x] PostGIS `geometry`/`geography` → `Binary` + `geoarrow.wkb`. Wire bytes pass through untouched, so every value keeps its own SRID; the column's coordinate system comes from typmod as `crs: "EPSG:<srid>"`, and `geography` adds `edges: "spherical"`.
  - `geoarrow-schema` rejected despite owning the canonical `WkbType`: 0.8.0 pins `arrow-schema` 58 against our 59, so its `ExtensionType` impls belong to a different crate than our `Field`, and a git dep cannot ship in a published crate. Hand-rolling `geoarrow.wkb` is ~60 lines of `ExtensionType` over the `serde_json` already in the crate. Swapping to it later means rewriting two call sites — the constructor in `pg_to_arrow`, the reader in `arrow_to_pg` — and deleting the module; nothing else touches `Wkb`. The release buys no capability, only one fewer file to own and constants from upstream: it carries CRS representations without interpreting them, so the WGS84 question BQ asks is ours either way. 0.2.0 is the trigger for moving the type rather than replacing it, `transferred-postgres` having stopped being its only consumer. The Parquet destination needs none of this: it hands the Arrow schema to the writer untouched, so any CRS spelling rides through verbatim.
  - EWKB, not ISO WKB. `format.md` asks producers for ISO "where possible" but lets consumers accept either, and rewriting the header would cost a copy per value while destroying the only CRS an unconstrained column has. `geometry_send` sets `0x20000000` in the type word and appends the SRID: `SRID=4326;POINT(1 2)` → `0101000020e6100000…`. `ST_AsBinary` drops it — don't use it.
  - An unconstrained column takes mixed SRIDs, `geography` included: it accepted 4269 despite PostGIS defaulting to 4326. So typmod `-1` means "no column CRS", not 4326. SRID from typmod is `(typmod & 0x0FFFFF00) >> 8`, which must not run on `-1` — that reads as SRID 1048575. PostGIS's own `0` means unknown too.
  - `crs_type: "authority_code"` with a bare `EPSG:` prefix, not a `spatial_ref_sys` lookup: right for every SRID PostGIS ships, wrong for user-defined ones under another authority. 0.1 takes that over an extra query per transfer.
  - The destination declares `geometry(Geometry,<srid>)`, subtype left free since the tag names none, so the column CRS survives PG → PG. Wire encoding stays `bytea`: binary COPY carries no per-column OIDs, so the bytes reach `geometry_recv` without ever resolving PostGIS's per-database OID.
  - `arrow.opaque` is the wrong tag here: its metadata is only `{type_name, vendor_name}` (no CRS slot), and the spec forbids canonicalising those values — which the 0.2.0 BQ `GEOGRAPHY` mapping would have to do.
  - Test image: `imresamu/postgis:18-3.6` (arm64 + amd64, Debian trixie, 233 MB), running the whole suite rather than a second container for geo alone. `postgis/postgis` publishes amd64 only (887 MB, emulated on Apple Silicon).
- [x] PG `enum` → `Utf8`, `citext` → `Utf8`. Both fell to `arrow.opaque`, handing a warehouse `bytea` where a status column should read `'cancelled'`. Measured on one real 2400-column schema: 10 enum columns, 2 `citext`, against 1 genuinely opaque `tsvector`.
  - Neither needs decoding — both wire forms *are* the UTF-8 text. Dispatch is `Kind::Enum(_)` for the enum and the name for `citext`, whose OID is per-database like PostGIS's.
  - `postgres-types` declines enum OIDs in `FromSql for &str` (it accepts `citext` by name), so the getter is a permissive `RawText`, sibling to the existing `RawBytes`.
  - `Utf8`, not `Dictionary(_, Utf8)`, though an enum is exactly a dictionary: no destination in 0.1/0.2 reads one, and the variant set has nowhere to live, Arrow having no canonical enum extension. PG → PG therefore lands `text`, dropping the type as PostGIS drops its subtype. Case-insensitivity goes the same way — it belongs to `citext`'s operators, not its bytes, so `email = 'Foo'` stops finding `foo` after a trip.
- [ ] PG ranges → `Struct{lower, upper, lower_inc, upper_inc, empty}` + `transferred.pg_range`, the private-extension tier DESIGN.md specifies. Covers `int4range`/`int8range`/`numrange`/`daterange`/`tsrange`/`tstzrange`; the destination rebuilds the PG range type from the tag, so PG → PG stays lossless.
  - Nothing to hand-roll on the wire: `postgres-protocol` 0.6.12, already a direct dependency, ships `range_from_sql`/`range_to_sql` with `Range`/`RangeBound`, and `Type::kind()` reports `Kind::Range(element)` for the subtype. `postgres-types` has no `FromSql` for ranges, so the getter is ours — same shape as the existing `RawBytes`.
  - The cost is the seam, not the parsing. `range_from_sql` hands each bound back as raw `&[u8]`, while every scalar builder today reads through `col::<T>(rows, i)`. Elements need a second dispatch, keyed on the element OID, that decodes one value from bytes — which the per-column table has no place for as written.
  - Multiranges (PG 14+, `Kind::Multirange`) stay out: they need `List<Struct>`, and the `arrow.opaque` fallback keeps them transferable meanwhile.
  - Expand (range → five flat columns) is a *destination* coercion and stays out of scope: PG holds ranges natively. It is 0.2.0's problem, and only for part of the family — BQ `RANGE` takes `DATE`/`DATETIME`/`TIMESTAMP` elements only, so `daterange`/`tsrange`/`tstzrange` land natively while `int4range`/`int8range`/`numrange` have to expand.

## 0.2.0 — BigQuery source + destination

Goal: add BigQuery source + destination. Atomic full load PG ↔ BQ. Direct type mapping; formal schema/coercion still deferred to 0.4.

**Scope:**

- `transferred-bigquery` destination: Storage Write API in `pending` mode against transient staging table → server-side copy job `WRITE_TRUNCATE` from staging into final → `DROP TABLE staging`. No GCS staging.
- `transferred-bigquery` source: Storage Read API.
- BQ schema vocabulary in Python (`"INT64"`, `"NUMERIC(18, 4)"`, `"GEOGRAPHY"`, `bigquery.SchemaField`).
- Destination table-creation options: BQ `partition_by=`/`cluster_by=` (set-at-create, cost/perf-relevant — higher priority than PG PK). Extends the 0.1.0 options bag.
- Auth via `gcp_auth` (ADC, service-account JSON, gcloud, workload identity).
- Direct Arrow ↔ BQ type mapping: `geography(_, 4326)` → BQ `GEOGRAPHY`, `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY`. Unsupported types error. Tiered coercion (auto/warn/fail) deferred to 0.4.
- BQ `GEOGRAPHY` exists only in WGS84, so the mapping has to *decide* whether a `geoarrow.wkb` column is WGS84, not merely carry its CRS. `crs: "EPSG:4326"` is a string compare; a PROJJSON or WKT2 CRS needs PROJ, and no geoarrow crate supplies it — `geoarrow-schema` only carries the value and delegates conversion to a `CrsTransform` the caller writes, its own default silently dropping the CRS. So refusing anything but an authority code is the 0.2.0 answer, and `Wkb` moves out of `transferred-postgres` for BQ to read it.
- `Timestamp(_, None)` → BQ `DATETIME`, never `TIMESTAMP`. Both Arrow `None` and PG `timestamp` mean wall-clock without a zone, and `TIMESTAMP` is an instant, so reaching for it would invent a zone for the commonest column type in a PG schema. Users who want an instant must name the zone the naive values are read in, which is 0.4's `schema=`.

**Tasks:**

- [ ] `transferred-bigquery` Storage Write client (tonic + googleapis).
- [ ] Atomic staging-table + copy-replace + drop-staging flow.
- [ ] `transferred-bigquery` source — Storage Read API.
- [ ] Auth integration (`gcp_auth`).
- [ ] BQ schema vocabulary + direct Arrow ↔ BQ type mapping. Type names come from the `TableFieldSchema.Type` enum in `google/cloud/bigquery/storage/v1/table.proto` — a proto we compile anyway for Storage Write, so prost generates the list and upstream owns it. Verified against googleapis master: `STRING, INT64, DOUBLE, STRUCT, BYTES, BOOL, TIMESTAMP, DATE, TIME, DATETIME, GEOGRAPHY, NUMERIC, BIGNUMERIC, INTERVAL, JSON, RANGE`, plus `Mode: NULLABLE/REQUIRED/REPEATED`.
  - `transferred-bigquery` re-exports the prost enum; `transferred-py` wraps it in `#[pyclass(eq, eq_int)]`. pyo3 can't be a dep of the connector crate, so the wrapper is a hand-written exhaustive `match` — which is the point: it stops compiling when Google adds a variant. `pyo3-stub-gen` 0.23 ships `gen_stub_pyclass_enum` / `gen_stub_pyclass_complex_enum`, so the existing stub-drift CI gate covers the Python side.
  - Only the *names* come from upstream. Precision/scale are separate `TableFieldSchema` fields, `ARRAY` is `mode=REPEATED`, `STRUCT` carries `fields` — so the parameterised surface (`t.Numeric(18, 4)`, `t.Array(...)`) is ours either way.
  - Two vocabularies exist: Storage Write v1 is GoogleSQL (`INT64`, `BOOL`, `STRUCT`), the v2 REST jobs API is legacy (`INTEGER`, `BOOLEAN`, `RECORD`). Staging-table create + copy job go through v2, so both get touched. Storage v1 is the user-facing one; the v2 mapping stays internal.
  - Not borrowed from the Python SDK — probed the alternatives:
  - `google-cloud-bigquery` rejected. 63 MB installed (38 MB grpc, 12 MB cryptography, 25 packages), and it buys no checking anyway: `SchemaField("x", "INT65").to_api_repr()` constructs fine and fails only server-side, because `field_type` is a bare `str`. A slim `--no-deps` install doesn't factor out — dropping grpc lands at 6.1 MB then `ImportError: google.rpc`, adding `googleapis-common-protos` + `grpcio-status` lands at 7.5 MB then `requests`, and each step is an unsupported combo that breaks on the next SDK bump. `types-google-cloud-bigquery` is not published, and stubs wouldn't help — `schema=` needs runtime objects.
  - `sqlglot` rejected. Cheap (3.1 MB, pure Python, no deps) and validates at construction — `DataType.build("INT65", dialect="bigquery")` raises `ParseError`, and it even knows PG's tail (`hstore`, `tsrange`, `geometry`, `jsonb`, `int4range`; not `ltree`). But it normalises to one cross-dialect vocabulary — BQ `INT64` becomes `DType.BIGINT` — which is the cross-destination DSL DESIGN rules out, it still takes strings so there's no autocomplete, and its checking is structural only: `NUMERIC(18, 4, 5)` parses clean.
  - Same call the non-SDK tools make (sqlglot, DuckDB's BQ extension, ADBC) — dbt-bigquery and dlt eat the full SDK because they are heavy apps already. The difference here is that the proto gives us the list for free, so nothing is hand-maintained.
- [ ] BQ env-gated integration test.
- [ ] Round-trip integration tests (PG ↔ BQ).

## 0.3 — S3 + GCS

- S3 destination (Parquet) via `object_store`.
- GCS destination (Parquet) — nearly free once S3 works.

## 0.4 — schema redesign

Implements the source-owned schema direction decided during the Interlude. Replaces the direct per-connector type mapping shipped in 0.1.0/0.2.0 with a canonical vocab + coercion engine + user `schema=` API.

**Scope:**

- Schema inference direction: `Source → Destination`. Source schema is ground truth; user overrides via `columns=` short-circuit source inference; coercion check resolves source → destination compatibility.
- Loud-fail semantics:
  - Static (plan-time): declared precision can never fit target → `SchemaError` before any read.
  - Runtime (row-level): Arrow `cast` with `safe=true`. First overflow row aborts run. Atomic destinations guarantee no half-written state.
- Drift framing (stateless): if destination already exists, compare source vs existing destination schema. Error:
  ```
  SchemaError: source column 'foo' (type Y) incompatible with existing destination
  column 'foo' (type Z). Likely source schema drift. Override with schema=.
  ```
- Formal coercion engine — Tier 1 auto, Tier 2 warn, Tier 3 fail. Reporting via `RunReport.coercions`.
- User schema API in Python: single `schema=` knob. Full by default; partial when an ellipsis key (`...: ...`) is present — remaining columns inferred. Source-side filtering via `columns=` / `skip_columns=` (mutually exclusive). Destination-native vocabulary.
- Type registry — coverage extended beyond the 0.1.0/0.2.0 direct-mapping baseline.

**Tasks:**

- [ ] Source schema introspection trait surface.
- [ ] Destination schema validation trait surface (replaces 0.1.0/0.2.0 ad-hoc per-connector mapping).
- [ ] Coercion engine: Arrow `cast` with `safe=true`, Tier-aware reporting wired into `RunReport`.
- [ ] User schema API in Python: `schema=`, `columns=`, `skip_columns=`.
  - PG type names come from `postgres_types::Type` — `type_gen.rs` is marked "Autogenerated file - DO NOT EDIT" and generated from PostgreSQL's own catalog. 185 consts at 0.2.14, ranges and arrays included (`TS_RANGE`, `INT4_RANGE`, `NUM_RANGE`, `INT4_ARRAY`), with `Kind::{Array, Range, Multirange}` carrying the element type. Already a transitive dep through `tokio-postgres`, so unlike the BQ enum this needs no new dependency and no proto compile.
  - Extension types (`geometry`, `hstore`, `ltree`) get OIDs at `CREATE EXTENSION` time and cannot be in a static list. One escape hatch, `pg.Raw("hstore")`, validated by `::regtype` — the same split `postgres_types` draws with `Kind::Other`.
  - Typmod stays ours: `numeric(18, 4)` is `Type::NUMERIC` plus a typmod int, so `pg.Numeric(18, 4)` is a `transferred` constructor. Mirrors BQ, where precision/scale are separate `TableFieldSchema` fields.
- [ ] Migrate Parquet, PG, BQ connectors to new trait surface.

## Backlog

- Format dispatch — moot while Parquet is the only format, so deferred until a second exists. File source no `format`: inherit source's (path extension, byte-sniff on ambiguity); explicit `format`: convert. Non-file source: default `Parquet()` or convert if explicit.
- Incremental loads. Model decided (see [INCREMENTAL.md](./docs/design/INCREMENTAL.md), D1–D10); scheduling into a version TBD.
- Cross-connector `batch_size` / byte-based memory budget (`set_max_row_group_bytes` + reader batch). Design against ≥2 connectors (PG in 0.1.0, BQ in 0.2.0); don't pin to one connector's shape.
- Airflow / Dagster / whatever is popular operators
- `sslrootcert=` DSN parameter — pin a CA file instead of the platform store, for `verify-full` against RDS or Cloud SQL. Needs stripping the key before `tokio_postgres::Config` sees it.
- CRS reprojection (`proj` FFI), `ST_MakeValid`, Z/M handling.
- Hstore / ltree / composite promotion from `arrow.opaque` to structured Arrow forms.
- `strict_mode` flag.
- Resumability after partial failure.
- CLI
- Multiple destinations `Transfer`s
- Format-driven file rotation — Files hands `Format` a sink *factory*; `Format` rolls to the next part when its byte/row-group budget says so. Files still owns opening (keeps the codec backend-agnostic for object_store/S3 reuse).
- Collapse `Source::stream_partitions` (`Vec<BatchStream>`) → a single `BatchStream`. The destination consumes batches without caring about source partition identity, so output partitioning becomes a destination/format policy (pairs with format-driven file rotation above). Simplifies the `Source` contract; trade-off is losing the source-partition → part-file mapping (parallel-per-partition write).
- Try to run code in a [dev container](https://zed.dev/blog/dev-containers)

## Never ~~say never~~
- Transformations beyond what type mapping forces.
- YAML/TOML config.
- Streaming and CDC (but who knows... v2.0?)
