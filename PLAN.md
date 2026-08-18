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

- `transferred-postgres` source: `table=` compiled to `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` → Arrow `RecordBatch`. Tests self-provision a throwaway PG+PostGIS container via `testcontainers`.
- Source schema inference via prepared-statement RowDescription (`prepare()` the inner SELECT → column type OID + typmod). The COPY binary stream carries no type/name metadata — only length-prefixed field bytes — so types must come from RowDescription, not the stream. PostGIS SRID from typmod.
- `transferred-postgres` destination: atomic full replace — staging table built from the source-derived schema, `COPY ... FROM STDIN`, then `BEGIN; DROP target IF EXISTS; RENAME staging; COMMIT;` (transactional DDL). Source schema wins, silent overwrite — consistent with Files/BQ. Target readable during load; brief exclusive lock only at swap. Indexes/grants/ownership not preserved (full replace); index-preserving replace strategy deferred to 0.4 `on_schema_change` (cf. dlt `replace_strategy`).
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
- [x] Integration test: PG → Parquet. Writes every fixture table out and reads the part file back, which is what pins the extension tags to a real file: `geoarrow.wkb` and `transferred.pg_range` are nobody's canonical types, so they survive only as long as the writer keeps field metadata.
  - PG `interval` reaches no Parquet file at all, and the test says so rather than skipping it. arrow-rs writes an interval as the legacy 12-byte `INTERVAL` (months/days/millis) and refuses `MonthDayNano` outright (`parquet-59.1.0/src/arrow/arrow_writer/mod.rs:1756`); `YearMonth` and `DayTime` are the only units it takes. Nothing to fix on our side — the mapping is right and Parquet is short a type.
  - It takes the whole table down with it, there being no `columns=` to leave a column behind until 0.4.
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
  - `ring`, not `aws-lc-rs`: the packaged crate ships pre-assembled objects (`ring-0.17.14/build.rs:435`), so the wheel matrix needs no nasm, perl or cmake. It still assembles them per target, which is enough to break a *cross* build — the aarch64 wheel is therefore built on `ubuntu-24.04-arm` rather than cross-compiled.
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
- [x] PG ranges → `Struct{lower, upper, lower_inc, upper_inc, empty}` + `transferred.pg_range`, the private-extension tier DESIGN.md specifies. Covers `int4range`/`int8range`/`numrange`/`daterange`/`tsrange`/`tstzrange`; the destination rebuilds the PG range type from the bounds, so PG → PG stays lossless.
  - Nothing to hand-roll on the wire: `postgres-protocol` 0.6.12, already a direct dependency, ships `range_from_sql`/`range_to_sql`/`empty_range_to_sql` with `Range`/`RangeBound`, and `Type::kind()` reports `Kind::Range(element)` for the subtype. `postgres-types` has no `FromSql` for ranges, so the column reads through the existing permissive `RawBytes`.
  - No second dispatch after all: naming the six built-in range OIDs directly (`Type::INT4_RANGE` and friends) puts the element type in the match arm, so each range is one more row of the per-column table. A user-defined range type falls to `arrow.opaque`, which is also all a per-database OID could support.
  - The tag carries no metadata: the six differ by the type of their bounds alone (`Int32`/`Int64`/`Decimal128`/`Date32`/`Timestamp(_, None)`/`Timestamp(_, UTC)`), so `range_element` doubles as the destination's dispatch and as the extension's own shape check. A `type_name` slot is what user-defined ranges would need.
  - `Union` over `Empty | Nonempty` is the honest shape and is unusable: Parquet has no union type at all (`parquet-59.1.0/src/arrow/schema/mod.rs:855` is `unimplemented!()`, a panic in a crate we cannot catch, tracked as ARROW-8817), and an Arrow union carries no validity buffer, so SQL NULL would need a third variant on every path including PG → PG. Any Parquet encoding we invented for it would be a struct with mutually exclusive fields — this struct, minus the standard shape.
  - Bounds reuse the element's own encoder on the way back, an encoder reporting `IsNull::Yes` being exactly the infinite bound, which `write_bound` then truncates away. So the destination needs no per-element table, only `range_type` mapping six element OIDs to their range.
  - `numrange` pins its bounds to bare `numeric`'s `Decimal128(38, 9)` and WARNs: a range constrains no precision on its element, so there is no typmod to read and no narrower choice to make.
  - Multiranges (PG 14+, `Kind::Multirange`) stay out: they need `List<Struct>`, and the `arrow.opaque` fallback keeps them transferable meanwhile.
  - Expand (range → five flat columns) is a *destination* coercion and stays out of scope: PG holds ranges natively. It is 0.2.0's problem, and only for part of the family — BQ `RANGE` takes `DATE`/`DATETIME`/`TIMESTAMP` elements only, so `daterange`/`tsrange`/`tstzrange` land natively while `int4range`/`int8range`/`numrange` have to expand.
- [x] Deploy 0.1.0. `release.yml` publishes `transferred-postgres` too — it was missing from the loop, and `transferred-py` depends on it, so the tag would have died on `cargo publish -p transferred-py`.
  - The aarch64 wheel is built on `ubuntu-24.04-arm`, not cross-compiled: `transferred-postgres` is the first C dependency in the wheel, and `ring`'s ARM assembly needs a `__ARM_ARCH` the manylinux cross-gcc does not define.
  - macOS and Windows had shipped nothing but a free-threaded wheel since 0.0.3, so every install there built from the sdist. `--find-interpreter` stops at the first `python3.14` on PATH, and `PythonT.framework` ships an executable of that exact name; those two platforms now name their interpreter outright, one job per ABI. Linux keeps discovery — a manylinux container cannot see host interpreters, and walks `/opt/python/*` where the names do not collide.
  - Both misses shipped because neither failed a build. The post-release smoke test in the release skill is what catches an incomplete set; nothing in CI asserts one.

## 0.1.1 — Arrow data in, not one pyarrow class

Goal: stop `ArrowSource` from being narrower than the seam beneath it.

**Scope:**

- [x] `ArrowSource` takes anything exposing `__arrow_c_stream__` — `pa.Table`, `pa.RecordBatch`, `pa.RecordBatchReader`, a `polars.DataFrame`, a duckdb result. Making a reader out of a table one already holds is ceremony, not a design.
  - Nothing to add in Rust: `arrow-pyarrow`'s `FromPyArrow for ArrowArrayStreamReader` tries the [PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html) first and only then falls back to pyarrow's private `_export_to_c` (`arrow-pyarrow-59.1.0/src/lib.rs:375`). The `isinstance` check in Python was the only thing narrowing it.
  - The pyarrow import goes with it, so `ArrowSource` no longer raises `ImportError`: on the capsule path pyarrow is never touched. The `transferred[arrow]` extra still covers `_iterable_to_arrow`, which really does build `pa.RecordBatch`es, and that path keeps its own hint.
  - Typed as an `ArrowStream` protocol rather than a union of three pyarrow classes — the union would lie about polars and duckdb the same way `RecordBatchReader` lied about `Table`.
- [x] `examples/dataframe_to_parquet.py` — the seam had no example at all, which is how the narrowing survived two releases. Covers a `polars.DataFrame`, a `pa.Table` and a reader, none of them wrapped; the polars column lands as `string_view`, its own layout crossing untouched.
- [x] Examples are named after the job, not the trip: `postgres_to_parquet.py`, with the load demoted to setup. A round-trip asserts data came back intact — that is what the test suites are for. The Parquet → Parquet one went with its name: recompressing a file is not why anyone reaches for a transfer tool, and `test_parquet_roundtrip.py` already covers `FilesSource`.
- [x] `Transfer(df, destination)` takes a DataFrame with no wrapper, the way `Transfer([{...}], …)` already takes rows. Dispatch on `__arrow_c_stream__` in `Transfer.__new__`, before the `isinstance(source, Iterable)` branch: a DataFrame *is* iterable, so today it reaches the row converter and dies describing the wrong problem — polars iterates its columns (`unsupported row type 'Series'`), pandas iterates column names (`unsupported row type 'str'`).
  - This is what makes the seam discoverable, not the class name. A polars user does not search for "Arrow", and `ArrowSource` is undiscoverable for exactly that reason — but the fix is an entry point with nothing to look up, not a rename. `ArrowSource` stays for the explicit path, where the word Arrow is the right one.
  - `Transfer`'s own docstring is the second half: `Args: source:` names the accepted things by library — a polars or pandas `DataFrame`, a duckdb result, `pa.Table`/`RecordBatch`/`RecordBatchReader` — because IDE hover and `help(Transfer)` are where a user actually looks.
  - Name the pandas floor wherever pandas is promised. Verified against 3.0.5; `__arrow_c_stream__` arrived in pandas 2.2, so anything older needs `pa.Table.from_pandas` — check its whatsnew before writing the number down.
- [x] Deploy 0.1.1.

## 0.1.2 — Postgres perf leg

Goal: measure the Postgres legs and let the README quote a number worth reading. `rows: 3, duration: 2ms` proves the API compiles, nothing else — the 0.0.3 findings are the only throughput and RSS figures we have, and they cover Parquet only.

**Scope:**

- [x] `postgres_to_parquet` and `parquet_to_postgres` workloads in `perf/workloads/`.
  - Fixtures are module constants (`perf.postgres.DSN`, `perf.fixtures.SEED`) rather than arguments, so a workload names what it needs and `setup` disappeared from all twelve. The CLI is `run <out>`; Postgres destinations ignore it and report `pg_total_relation_size` as bytes written.
- [x] Container lifecycle in `perf/postgres.py` — plain `docker run` against `imresamu/postgis:18-3.6`, no testcontainers dependency. Container and seed outlive a run and a stopped one is restarted, not replaced, because reseeding costs minutes.
  - Readiness must be probed over TCP (`pg_isready -h localhost -U postgres`). The image serves a unix-socket-only server while its own init scripts run, and a socket probe answers "ready" mid-initialisation — which raced our `create extension postgis` against the image's and killed the container.
  - `check_disk` refuses to start when the host cannot hold the peak. Measured against `perf/` rather than `/`, which on macOS is a sealed system volume reporting different free space.
- [x] Seed server-side (`create table … select from generate_series`), one wide table of 22 columns covering ints, floats, `numeric`, text, `citext`, enum, `bytea`, temporals, `uuid`, `jsonb`, two range types and PostGIS `geometry`/`geography`. `PERF_ROWS` sets the scale, 50M by default, and `PERF_SLOW_ROWS` caps the legs that move every row through Python.
  - No `interval` column. Arrow maps it to `Interval(MonthDayNano)`, which parquet-rs cannot write, so a table holding one cannot reach any Parquet leg.
  - The Parquet fixtures are dumped *from* that table rather than generated independently, so one schema definition serves both legs and no two generators can drift.
- [x] Baselines, and why each: dlt for both Postgres legs (the tool we compete with — same job, a framework wrapped around it), duckdb's `postgres_scanner` for both (a whole engine doing the same job in one statement). `psycopg` is out: it needs per-column type declarations for 22 columns and holds every row as Python objects, which does not fit in memory at this scale.
  - ADBC was measured on both legs and then dropped. On the wire it is the closest thing to a like-for-like — the same binary COPY under a C driver — but it speaks Arrow and Parquet and nothing else, so it is no competitor to a tool whose point is to grow more formats. What it was good for it already told us: it has no mapping from an Arrow struct to a Postgres type, and it does not autocommit, so an ingest rolls back on connection exit while `adbc_ingest` still reports the row count.
  - duckdb writes too — the write leg was missing only because our own `attach` said `read_only`. It hits ADBC's wall from the other side: a column for its own STRUCT needs a named composite type in Postgres, which it will not invent. Reading a range into a struct is not how it gets there anyway — it reads one as its Postgres text form, so the round trip lands `varchar` and the values survive intact.
  - `create table pg.x as select from pg.y` under an attached Postgres is not a server-side pushdown, though `explain` shows a single `PG_CREATE_TABLE_AS` operator over a `POSTGRES_SCAN`: 2M rows cost 1.93s of duckdb's own CPU, so the rows do cross the wire twice. Worth checking before quoting any duckdb number that names two Postgres relations.
  - At 1M rows duckdb wins both legs — 0.96s writing against our 1.60s, having already read in 9.51s against our 15.04s at 10M. It is the baseline to beat, and the one worth studying: no per-row Python anywhere. Not a parallel extract, though it was written down as one twice before anybody looked — `pg_stat_activity` polled through its read leg shows exactly one backend, the same one connection and one `COPY` we hold.
- [x] Rerun the suite at 10M so the duckdb write leg has a number comparable to the rest, and refresh `docs/DLT_COMPARISON.md` from it. Our write leg is the one that moved: 16.52s → 12.85s, RSS 144 → 140 MB, on the encoder and the release profile below. The `CPU/wall` column now in the doc says where our write time goes — 0.45, so over half the leg waits on one Postgres backend. duckdb read faster on both legs here, which the harness redesign further down retired as a projection artifact; every absolute number in this item predates the round-robin suite and is not comparable to one taken after it.
- [x] Measured where the write leg's wall time goes, since `CPU/wall = 0.45` says most of it is spent waiting: at 10M rows, decoding Parquet is 1.23s, `write_batch` 11.90s, `finish` 0.07s. So the wait is inside `send`, blocked by backpressure from one Postgres backend parsing our COPY — not idle time between batches, and `finish` returning instantly says no queue builds up.
  - Prefetching the next batch under `try_join!` while the current one is in flight buys nothing, measured: 12.97s → 13.09s wall, 5.78s → 5.84s client CPU, both inside the noise. It also proves nothing about threads — both futures are polled by one task, so the decode never overlaps the wait.
  - Moving the encode off the runtime is the honest version of that test, and it buys 2.3%: encoding each batch in `spawn_blocking` while the main task pulls the next one and sends the last measures 13.34s against 13.65s back-to-back, client CPU 5.81s → 5.58s, RSS unchanged at 131 MB. The CPU really does leave the runtime; the wall does not follow, because the server is the one that is busy. Not worth an `Arc`, a `JoinHandle` and a whole batch materialised per encode, so the one-batch-in-flight loop stands.
  - The server saturates one core for the whole load — 13.25s of container CPU over a 14.19s leg, and `pg_stat_activity` shows one backend. So does duckdb: it too holds exactly one connection and one `COPY`, which retires the idea that its `CPU/wall = 1.45` is several backends. It is its own parallel Parquet decode.
  - Handed duckdb's own 20-column projection our leg lands at 10.9s against its 10.3s, both tables the same size to within a megabyte (2317 vs 2316 MB). So the 3s the full row costs us buys the two range columns duckdb cannot represent, and the 6% left over is PostGIS validating the two geometry columns duckdb declares `bytea`. There is no client-side gap left to close on this leg.
  - Server-side parallelism — several COPY streams into one staging table — is therefore the only path past one core, and it belongs in the `-core` partition traits rather than a destination-local hack.
- [x] Drop `PgValue = Box<dyn ToSql>` from the write path. It was one heap allocation per value: a 20-column million-row load boxed 20M times, and a `sample` profile of it spent 27% of on-CPU time in malloc/free and 16% in memmove, against 3% decoding Parquet. Binary `COPY` is a length-prefixed field per value, so a column now writes straight into the buffer through an encoder bound once per batch.
  - Writing the framing ourselves also picked the chunk size, which `BinaryCopyInWriter` fixes at 4 KB (`tokio-postgres-0.7.18/src/binary_copy.rs:94`) — a message every few rows, and a third of the client CPU on its own. 64 KB is where the curve flattens.
  - Client CPU at 1M rows fell 1.06s → 0.48s on duckdb's own 20 columns, which is below duckdb's 0.60s: we now spend less CPU than it does while carrying the narrower types. Wall went 1.24–1.34s → 1.05–1.20s against its 0.92–1.01s, so what is left is the server validating `geometry` where duckdb declared `bytea`, not our encoding.
- [x] Flatten the encoder into one `Encoding` enum, dropping the `ValueWriter`/`MakeWriter` traits, `BindValue`, `ArrayWriter` and the boxed closure per column that came with the encoder above. An anonymous `Box<dyn Fn>` has no fields and no named methods, so neither go-to-definition nor grep can follow a value from the batch to the buffer — which is the only thing the machinery bought.
  - Costs 36% client CPU at 1M rows, 0.50s → 0.67s, both with `lto = "thin"`. The generic `ArrayWriter<A, F>` inlined its whole chain and downcast once per batch; a plain `match` on `Encoding` cannot, so a value now pays two poorly-predicted indirect branches — `as_any` inside the downcast, then the match. Recovering it needs a second enum holding the already-downcast arrays, which is the machinery again.
  - Taken deliberately: the encoder is read far more often than the 0.17s is spent, and wall clock at this scale is server-bound either way (1.28s vs 1.32s).
- [x] `[profile.release] lto = "thin"`. The workspace declared no release profile at all, so every number measured before this one was taken without cross-crate inlining: `put_slice` lives in `bytes` and `to_sql` in `postgres-types`, so the hot loop made a real call out of the crate to copy four bytes. Client CPU at 1M rows fell 0.85s → 0.67s, RSS 102 → 94 MB, and the profile lost `put_slice` (10.4%) outright.
  - `codegen-units = 1` measured at 0.85s — nothing — while taking the release build from 31s to 1:06. The win is cross-crate inlining, which thin LTO does across all 16 units in parallel, not merging them.
  - `lto = "fat"` is worth 14% more client CPU and no wall clock at all: 5.78s → 4.97s over a 10M-row load whose wall stayed at 13.0s, since the leg waits on the server. It costs 4x the link — 14s → 56s to rebuild the extension — so thin stays. Revisit if a leg ever becomes client-bound.
  - `panic = "abort"` stays out: PyO3 turns a Rust panic into a Python exception by catching the unwind, so aborting would take the interpreter with it.
  - `strip` stays out: it shrinks the wheel and hides every frame the perf legs are diagnosed from. `debug = "line-tables-only"` is the opposite trade, worth it only once a profile needs line numbers rather than symbols.
- [x] `measure()` refuses a debug build of the extension. `python-setup` installs a debug one over the release build `python-dev-build` leaves in the same path, so a `make check` between two perf runs swapped the binary being measured with nothing to show it — the `--release` in the perf targets was never the problem. Sits in `measure` because every entry point goes through it, including a hand-run workload module.
  - Compares the loaded `.so` against `target/release/lib_native.*` by stat signature, maturin's copy being byte-identical down to mtime. Contents deliberately unread: 11 MB through a buffer would land in the RSS the harness reports.
- [x] Report RSS beside throughput. Dropped `peak arrow MB` instead of fixing it: `measure()` sampled `pa.total_allocated_bytes()` only before and after the thunk, so it read 0 in every run since the harness landed. The RSS sampler thread already answers the memory question.
- [x] Sample the workload's whole process tree and report RSS and CPU as `min/avg/max`. Postgres runs in a container, so it is outside both figures by construction and the columns describe the engine, not the server it drives.
  - Each number comes from whichever source cannot miss it: the RSS peak and the CPU mean from rusage, which totals the process's whole life, and the rest from the samples. A sampled peak misses a spike between two reads, and a sampled mean misses work between them.
  - A quarter-second interval, not the second it started as: the Parquet legs finish inside a second and would report no samples at all. The first read is discarded — `psutil`'s `cpu_percent` is a delta against its own previous call, so a first one always reads 0.0 and would peg every `min` to zero.
  - The tree, not the process: `Process` objects are kept per pid across samples for the same reason, and children are re-listed each time so a subprocess a library forks is counted while it lives.
- [x] Repeat every workload `PERF_REPEATS` times (3 by default) and report the minimum, beside a `spread` column of slowest over fastest. Noise on a shared machine is one-sided — it can only add time — so the minimum is the engine's own cost and the maximum is someone else's work. The first run doubles as the warm-up, so no separate concept is needed.
  - `spread` is what justifies the repeat count rather than a rule of thumb: at 10M rows everything lands under 1.31x, so three suffice. It also disqualifies a number — duckdb's read reports 2.77x, so its 9.49s is not a measurement worth quoting.
  - Outputs and write targets are reclaimed *between* repeats, not after all of them. Left in place, a previous run's Parquet inflates the next run's reported bytes.
- [x] Run the workloads **round-robin** — every workload once per pass, `PERF_REPEATS` passes — because this machine slows by roughly half over an hour of sustained load. Running one workload's repeats back to back charges whoever comes late for the whole drift, and the table then ranks engines by their position in the suite rather than by speed. Interleaving spreads the drift over all of them, and every engine gets the same shot at the quietest pass.
  - The drift is real and large. Four suites in a row moved together — our read leg at 16.5s, 20.4s, 22.9s, 25.1s; dlt's at 29s, 120s, 38s, 37s — while each suite stayed internally consistent, so `spread` reported ~1.1x throughout and disclosed none of it. `spread` measured scatter *within* a workload while the bias sat *between* them, which is exactly what it cannot see. It now reports pass-to-pass drift instead, so it is an error bar rather than a quality score, and more passes will not shrink it.
  - `/sys/fs/cgroup/cpu.stat` is what located it: 22s of server CPU under a 23.5s leg, one backend parsing our COPY, against 11s under a 13.1s leg. `usage_usec` counts time on CPU rather than work done, so a doubled figure means the same work at half the speed. Confirmed from the other side by the two-leg A/B below, which ran cool and landed at 13.5s and 15.8s where the full suite reports 22.7s and 22.1s.
  - **Three server-state controls were written and all three removed**, each after measuring it rather than reasoning about it. Round-robin is the whole answer, and the drift it answers is the machine's.
    - `docker restart` before every measured run: 3 interleaved passes on both Postgres legs, fresh against stale server, minima 13.54s vs 13.70s writing and 15.81s vs 15.67s reading — under a percent either way, with the stale server ahead on reading. The one measurement that argued for it (23.5s → 16.5s) came off a week-old container and did not survive being run properly.
    - `pg_prewarm(TABLE, 'read')`: the cache it warmed lives in the Docker VM's kernel and a container restart does not touch it — `buff/cache` measured 13331 MB before a restart and 13329 MB after. So a restart only ever cleared Postgres's own 128 MB of `shared_buffers`, half a percent of the table, and the prewarm was a no-op after the first read of the suite. The docstring justifying it ("a restart hands the suite an empty cache") was simply wrong.
    - `checkpoint` after each target is dropped: written for dirty pages left by the previous load, which was never the cause; a restart would have flushed anyway, and one mechanism beats two.
  - The lesson generalises past this harness: **a number is only comparable to another from the same suite.** Suites are not comparable across days or even across hours, so the doc gets rebuilt from one run rather than patched leg by leg.
- [x] `perf/fidelity.py` — the types each engine's Postgres target actually lands, from `format_type(atttypid, atttypmod)` rather than `information_schema`, which reports a bare `numeric` for a `numeric(12,4)` and would credit every engine with a loss none of them makes. Scale-independent, so `PERF_ROWS=100000 make fidelity` answers it in a minute. The doc's fidelity table was assembled by hand before this, which is the kind of table that rots silently.
- [x] Every engine round-trips its own dump: the write leg loads back the Parquet its own read leg wrote (`perf/dumps.py`), replacing the per-baseline projections that came before. The projections compared engines on a common column list, which sounds fair and is not — the shared fixture was written by our own `Transfer`, so it carried a `transferred.pg_range` tag no baseline can read, and every baseline's write leg was measured against our format. Now each loads a file it chose the types of, and what a dump has already lost was lost in the read leg, where the table says so.
  - So the two Postgres legs are timed separately and neither is a round trip. A PG → PG leg was built first and thrown away: it measures reading and writing at once and cannot say which engine is better at which, and duckdb's version of it is one CTAS statement whose plan we then had to go read.
  - Which columns each engine gives up was earned by running the thing: connectorx panics (uncatchably, in Rust) on `daterange`, `int8range` and both PostGIS types; dlt refuses `extension<arrow.uuid>` and `extension<arrow.json>` outright when reading Parquet, though its own reader emits those Postgres types as plain strings; its CSV `COPY` cannot carry `bytea`.
  - The read legs' `dump()` is split out of `run()` so the write legs reuse it, and dumps are built during setup rather than inside a measured subprocess, whose peak RSS is read from rusage over its whole life. Ours needs no dump of its own — the shared seed already is one.
  - dlt's two write paths each drop something, and CSV is the better trade: being textual it only gives up `bytea`, while parquet-over-ADBC additionally rejects `int16`, `int32` and `float32` — it declares the table with those widened and then ships the original file to `COPY`, which fails with `insufficient data left in message`.
  - dlt records deployed schema versions in `_dlt_version` and skips the DDL when a table is listed there, so a repeat whose target the harness dropped fails on a missing relation with no error above it. Each dlt workload clears that bookkeeping before it is timed.
- [x] Cap the baselines that move every row through Python (`PERF_SLOW_ROWS`, 1M). dlt on its defaults costs ~46 us a row reading and ~57 us writing; at `ROWS` either leg alone would outlast the rest of the suite, and the gap it exists to show is already plain at a million.
- [x] `check_disk` models the real peak: the seed plus the one write target a leg is filling, which our destination doubles while staging. That only holds because targets are dropped as they finish — the 50M attempt died mid-run at `ENOSPC` precisely because three targets accumulated against an estimate written for two. Docker's disk use on macOS is a ratchet, so the estimate must cover the high-water mark, not the steady state.
  - The bytes-per-row constant is schema-specific and no library can supply it, so the seed reports its measured size next to the assumption and flags a >20% drift. Judged only past a million rows: below that, page and toast overhead spread over too few rows and every seed looks oversized.
- [x] Rebuilt `docs/DLT_COMPARISON.md` from one round-robin suite at 10M, on a container created for it. Reading: 14.66s against duckdb's 9.84s and tuned dlt's 21.32s. Writing: 14.28s against duckdb's 11.39s and dlt's 229.75s, with peak RSS 133 MB against 271 and 2816.
  - **duckdb wins both legs, and the write leg it lost in the previous suite.** That earlier reading — 22.70s against its 26.17s — does not survive a fresh container: both engines got faster on one, ours 1.6x and duckdb's 2.3x. A second suite of only these two reproduced the new order (14.47s / 9.91s reading, 13.16s / 11.11s writing), so it is the order and not a pass. What remains ours is memory — 261 MB peak reading against its 1788 — and the types, seven of which it gives up to our four.
  - What changed between the suites is unproven: the container and its volume were destroyed and recreated, and every leg got faster by 1.6x to 2.3x. The three controls tried earlier all targeted server *state* and none of them touched the volume underneath it, which is the one thing that differed. So `docker rm -fv` before a suite a doc is built from is the procedure — not a knob, and not a diagnosis.
  - duckdb does two things better than expected, both found by `make fidelity`: it writes `uuid` as canonical `arrow.uuid`, and `geometry` as GeoParquet 1.0 WKB, so both survive a round trip as Postgres types. But its GeoParquet metadata carries no CRS, so the geometry lands with **SRID 0** — the type survives and the spatial reference does not, silently. Its `geography` column, carried as text, keeps 4326 inside the EWKB hex: the typed column loses what the untyped one preserves.
  - dlt's caller-side Arrow rewrite is now ~21s of its 227s rather than the 104s recorded before, measured at 2.13 µs a row over its own extract. Not an improvement in dlt — its own extract already spent the ranges, `uuid` and `jsonb` down to text, so the map has nothing left to unwrap but the one binary column. Its own extract-normalize-load is therefore ~206s, still 9x ours.
  - Also corrected: dlt no longer preserves `numeric(12,4)` — reading back its own extract it declares `numeric(38,10)`. The old table credited it with keeping that one.
- [x] Cut the suite to four engines over two legs — us, duckdb, dlt tuned, dlt on its defaults. pyarrow and fastparquet were a Parquet codec measured against a transfer tool, and the iterable legs measured our Python row converter, which nothing here competes with; their numbers stand in 0.0.3. Nothing left in the suite is unpaired.
  - dlt's four legs run only under `PERF_DLT=1`, which `make perf-full` sets: two of them cost minutes each, so the quick loop is us against duckdb in five minutes rather than forty. Without the flag the suite prints `dlt: skipped`, since a table missing half the comparison must not look complete.
  - Results are rewritten after every measured run rather than at the end. A suite is dozens of subprocesses over an hour and one failure used to take every earlier measurement with it — which is exactly what the 50M `ENOSPC` run left behind, a log and nothing else.
  - `dumps.ensure` stamps what its `dump` reported writing, not the row count asked for. Running a workload module by hand at another scale silently left a 100k dump stamped 50M, which the next suite would have loaded as current.
- [x] README example at a scale worth showing, output verbatim from a `--release` build: 10M rows of a five-column `public.orders`, 3s 379ms, 137 MiB peak resident, interpreter included. `examples/postgres_to_parquet.py` stays at three cities — an example is read, a benchmark is run.
- [x] `perf/versions.py` — the same two legs under each published wheel, one venv per version straight from PyPI (`PERF_VERSIONS=0.1.1,0.1.2 make perf-versions`). A release claims a perf win; this is what checks it, and nothing before it could: the baselines don't move between our releases, so `perf.run` measures the wrong axis.
  - Versions are interleaved *inside* one suite, for the same reason its workloads are. Measuring all of 0.1.1 and then all of 0.1.2 is exactly the two-suite comparison this machine's drift invalidates, and it would credit whichever ran first.
  - `--only-binary :all:`, so a missing wheel fails the run rather than silently compiling an sdist here — which would measure this toolchain and this `[profile.release]`, the local build wearing an old version number.
  - The venv takes the suite's own Python series: `uv venv` otherwise picks the first interpreter it finds, a 3.12 on this machine, and no wheel of ours fits it.
  - `measure()`'s guard now names the debug build it refuses instead of demanding the one in `target/release`. Same strength — a debug install is `maturin develop`'s copy of `target/debug`, byte-identical down to mtime — and it stops refusing a released wheel, which is a release build living in a venv.
- [x] Deploy 0.1.2.

## 0.2.0 — BigQuery source + destination

Goal: add BigQuery source + destination. Atomic full load PG ↔ BQ. Direct type mapping; formal schema/coercion still deferred to 0.4.

**Scope:**

- `transferred-bigquery` destination: Storage Write API in `pending` mode against transient staging table → server-side copy job `WRITE_TRUNCATE` from staging into final → `DROP TABLE staging`. No GCS staging.
- `transferred-bigquery` source: Storage Read API.
- BQ schema vocabulary in Python (`"INT64"`, `"NUMERIC(18, 4)"`, `"GEOGRAPHY"`, `bigquery.SchemaField`).
- Destination table-creation options bag (additive over the source-derived DDL): BQ `partition_by=`/`cluster_by=`, both set-at-create and cost-relevant. A PG `primary_key=` waits for incremental loads, which is the only thing that reads one.
- Auth via `gcp_auth` (ADC, service-account JSON, gcloud, workload identity).
- Direct Arrow ↔ BQ type mapping: `geography(_, 4326)` → BQ `GEOGRAPHY`, `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY`. Unsupported types error. Tiered coercion (auto/warn/fail) deferred to 0.4.
- BQ `GEOGRAPHY` exists only in WGS84, so the mapping has to *decide* whether a `geoarrow.wkb` column is WGS84, not merely carry its CRS. `crs: "EPSG:4326"` is a string compare; a PROJJSON or WKT2 CRS needs PROJ, and no geoarrow crate supplies it — `geoarrow-schema` only carries the value and delegates conversion to a `CrsTransform` the caller writes, its own default silently dropping the CRS. So refusing anything but an authority code is the 0.2.0 answer, and `Wkb` moves out of `transferred-postgres` for BQ to read it.
- Decide there whether `transferred.pg_range` becomes `transferred.range`. BQ `RANGE<DATE|DATETIME|TIMESTAMP>` is always `[lower, upper)` with NULL for an infinite bound and no empty range at all, so a BQ range fits the same five-field struct — at which point the `pg_` in the name is a lie, and `empty` reads as the PG-only field it is. Renaming is one constant plus the metadata every reader compares against, so it is a 0.2.0 decision, not a 0.1.0 hedge.
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

## 0.2.1 — Arrow interchange contract

Goal: state what the Arrow layer between a source and a destination *is*, so a connector author learns it from a document rather than from reading `pg_to_arrow.rs`.

**Scope:**

- The contract is implicit today. DESIGN.md §Type system records what 0.1 happens to do; the rules themselves live in each connector's match arms, so a new connector cannot tell which Arrow types it must accept, which it may emit, or what the tags oblige it to.
- Spell out the supported `DataType` set per direction, and what is deliberately outside it (`Union` — no Parquet encoding, `Dictionary`, `Duration`, `Interval` past Parquet's reach).
- Promote the extension tiers from prose to normative: canonical (`arrow.uuid`, `arrow.json`, `arrow.opaque`), community (`geoarrow.wkb`), ours (`transferred.*`, with 0.2.0's `transferred.pg_range` → `transferred.range` decision settled first).
- Say what a destination owes a tag it does not know. Files writes the metadata verbatim, Postgres refuses and names the type — both are defensible and neither is written down as the rule.
- Say where a shared extension type lives. `Wkb` and the range type leave `transferred-postgres` in 0.2.0 regardless; the contract decides whether `transferred-core` owns every `transferred.*` tag or connectors keep their own.
- Decide which of it is public Rust API. `Wkb`, `PgRange` and `range_fields` are `pub` so a caller can declare such a column in a hand-built Arrow schema — and because `tests/` is a separate crate that sees nothing else. `#[doc(hidden)]` is the alternative, for all of them together, and the contract is what makes the choice answerable.
- Conformance is a shared test corpus — one `RecordBatch` per contract row that a connector crate round-trips through itself. A trait with no behaviour would only restate the type signatures.

After BQ, not now: with one source and two destinations there is a single call site per rule, so rule and code are indistinguishable. BQ brings a second source, a third destination, and the first mappings (`GEOGRAPHY`, `RANGE`) that must read tags another connector wrote.

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
- Postgres source `query=` — an arbitrary SELECT in place of `table=`, compiled to the same COPY.
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
