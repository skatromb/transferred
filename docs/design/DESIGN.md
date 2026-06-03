# `transferred` — Design

Package name `transferred` on both crates.io and PyPI. Workspace is split into per-connector crates (`transferred-core`, `transferred-parquet`, `transferred-postgres`, `transferred-bigquery`) plus a Python binding crate. Workspace version is shared across all crates; untie only if release cadence diverges.

## Why

Existing ETL tools each hit at least one dealbreaker:

- **Airbyte** — clunky UX, heavy operationally.
- **Pandas** — memory overhead, single-threaded by default.
- **Polars** — fast and lean, but Parquet support has gaps that bite in practice.
- **dlt** — mixes the issues above.

I want the tool I'd actually use. Small, fast, opinionated. Built in Rust because I like Rust and it pays off here.

## What

A simple and pleasant tool that moves a table-shaped dataset from one system to another. Not T for Transformation, only extract and load.
Transformations are someone else's job (dbt, warehouse SQL, Polars after landing).

Properties it must have:

- **Simple Python API.** A transfer fits on a screen.
- **Lean.** No per-row Python overhead. Arrow end-to-end.
- **Mature on formats.** Parquet read/write covers what Polars currently misses (gaps catalogued as we hit them).
- **Boring where it matters.** Full-load only. No streaming. No DAG orchestrator. No web UI.

Explicit non-goals:

- No transformations.
- No streaming / CDC.
- No scheduling — keep that for Airflow/Dagster/cron.
- No CLI — Python entrypoint only.
- Incremental loads — architecture leaves room, planned for later release.

## How

### Versioning

While the project is in its initial development, breaking changes are allowed but require a minor bump; patch bumps stay non-breaking. No deprecation cycles, no compat shims. Full API review precedes the stable release; strict SemVer after.

### API surface (Python, code-first)

```python
from transferred import Transfer
from transferred.sources import Postgres
from transferred.destinations import LocalFilesystem, BigQuery
from transferred.formats import Parquet

# Local file destination — no cloud creds needed.
# Format inferred from path extension; pass `format=Parquet(...)` to override.
Transfer(
    source=Postgres(dsn="postgres://...", table="public.orders"),
    destination=LocalFilesystem(path="./out/orders.parquet"),
).run()

# BigQuery destination.
Transfer(
    source=Postgres(dsn="postgres://...", table="public.orders"),
    destination=BigQuery(project="my-proj", dataset="raw", table="orders"),
).run()
```

**Source auto-coercion.** `source=` accepts anything iterable, not only `Source` instances. Python-side dispatcher normalises:

```python
# Generator of dicts
def fetch_orders():
    for page in api.paginate("/orders"):
        yield from page["results"]

Transfer(source=fetch_orders(), destination=LocalFilesystem("out.parquet")).run()

# List of dicts
Transfer(source=[{"id": 1}, {"id": 2}], destination=LocalFilesystem("out.parquet")).run()

# Dataclasses — converted via dataclasses.asdict per chunk
Transfer(source=order_iter, destination=LocalFilesystem("out.parquet")).run()
```

Module layout for the iterable + Arrow path:

- `transferred.arrow.ArrowSource` — accepts a `pa.RecordBatchReader` only. The Arrow seam into Rust. Future overloads (`pa.Table`, `pa.RecordBatch`) drop in here.
- `transferred.iterable._iterable_to_arrow` — converts an iterable of `dict` / `@dataclass` / `pydantic.BaseModel` into an `ArrowSource` via `pa.RecordBatch.from_pylist`. Depends on `arrow` (one-way), not the other way.
- `transferred.transfer.Transfer` — Python wrapper around `_native.Transfer`. Coerces iterables on construction.

Dispatcher rules in `Transfer.__init__`:

- `Source` instance (e.g. `ParquetSource`, `ArrowSource`) → used directly.
- Any other `Iterable` (excluding `str`/`bytes`/`bytearray`/`dict`) → wrapped via `_iterable_to_arrow`. Rows are batched into `pa.RecordBatch` of `_BATCH_SIZE` (4096), one FFI crossing per batch, schema inferred from first batch.
- Anything else → `TypeError`.

Row shapes accepted by the iterable path: `dict`, `dataclass`, `pydantic.BaseModel` (v1 + v2). All normalized to `dict[str, Any]` on the Python side via a once-sniffed converter; pyarrow then builds the `RecordBatch`. `namedtuple` / `attrs` / `msgspec.Struct` deferred — trivial to add when requested.

Source accepts `table=` OR `query=` (mutually exclusive):

```python
Postgres(dsn="...", query="SELECT id, total FROM orders WHERE region = 'EU'")
```

Internally both compile to `COPY (SELECT ...) TO STDOUT`. `table=` is sugar.

Source-side column filtering — `columns=` or `skip_columns=` (mutually exclusive):

```python
source=Postgres(
    dsn="...",
    table="public.orders",
    skip_columns=["legacy_blob"],
)
```

**Schema direction — source-owned, destination-validated.** Source schema is ground truth: connectors infer it natively (PG `information_schema`, Parquet file metadata, Arrow batch schema, …) and preserve it end-to-end. User overrides per column via `schema=` short-circuit source inference. Destination validates the resolved schema against its accepted type set and fails loudly on incompatibility. Vocabulary stays destination-native (BQ types in `BigQuery`, PG types in `Postgres`, …); Arrow is internal, never spelled in Python. Source-side filtering via `columns=` / `skip_columns=` (mutually exclusive) is the only source-side typing knob.

**User schema API.** Single `schema=` knob. Strict by default; partial when an ellipsis key (`...: ...`) is present.

```python
# Strict full schema — every source column must be listed.
BigQuery(
    project="p", dataset="d", table="orders",
    schema={
        "id":         "INT64",
        "total":      "NUMERIC(18, 4)",
        "created_at": "TIMESTAMP",
        "tags":       "ARRAY<STRING>",
    },
)

# Partial schema — `...: ...` sentinel means "infer the rest from source".
BigQuery(
    project="p", dataset="d", table="orders",
    schema={
        "total": "NUMERIC(18, 4)",
        ...:     ...,
    },
)

# Or native lib objects, where the library ships them.
BigQuery(
    project="p", dataset="d", table="orders",
    schema=[
        bigquery.SchemaField("id", "INT64"),
        bigquery.SchemaField("total", "NUMERIC", precision=18, scale=4),
    ],
)
```

Rules:

- `schema=` is **strict** by default: every source column must have an entry. Missing → `SchemaError: column 'X' inferred from source has no entry in schema=`.
- Ellipsis key (`...: ...`) flips it to **partial**: unlisted source columns are inferred.
- Schema column not present in source → `SchemaError: schema column 'X' not found in source`.
- Vocabulary is owned by each destination. No cross-destination DSL.
- Coercion follows the tier model in §Type mapping. Tier-3 lossy-semantic conversions fail by default.

`.run()` returns a `RunReport`:

```python
report = transfer.run()
report.rows            # 12_481_902
report.bytes_written   # 1_503_948_211
report.duration        # timedelta
report.coercions       # list[Coercion] — column, original type, target, level
report.staging         # transient artifacts (deleted unless keep_staging=True)
```

No row-level Python callbacks. The FFI boundary is crossed once per transfer (per-batch for `ArrowSource`), not per row.

### File destinations and formats

File-shaped destinations are decoupled from file formats. A destination describes **where** bytes land; a `FileFormat` describes **how** they are encoded.

- `FileFormat` trait. Implementations carry encoder knobs:
  - `Parquet(compression, row_group_size, ...)`
  - `Avro(...)`
  - `Json(...)`
  - `Csv(...)`
- File-shaped destinations carry an optional `format`:
  - `LocalFilesystem(path, format=None)`
  - `S3(bucket, key, format=None)`
  - `GCS(bucket, key, format=None)`
- Row-protocol destinations have no `format` knob — the wire protocol is the encoding:
  - `BigQuery(project, dataset, table)` — Storage Write API.
  - `Postgres(dsn, table)` — `COPY ... FROM STDIN`.

**Format resolution** when the user does not pass `format=` explicitly:

| Source              | `format=` arg | Behaviour                                                                              |
| ------------------- | ------------- | -------------------------------------------------------------------------------------- |
| File source         | absent        | Inherit source's format. Detected by path extension first; bytes-sniff on ambiguity.   |
| File source         | present       | Convert source → requested format.                                                     |
| Non-file source     | absent        | Default to `Parquet()`.                                                                |
| Non-file source     | present       | Convert to requested format.                                                           |

Byte-copy short-circuit (won't do): even when source and destination resolve to the same `FileFormat`, the engine always decodes through Arrow. Skipping the decode would bypass schema validation and coercion reporting, and the perf win does not justify a second code path.

```python
from transferred.destinations import LocalFilesystem, S3
from transferred.formats import Csv, Avro

# Inherit format from source extension.
Transfer(source=parquet_src, destination=LocalFilesystem("out.parquet")).run()

# Override format.
Transfer(source=parquet_src, destination=LocalFilesystem("out.csv", format=Csv())).run()

# Non-file source → default Parquet.
Transfer(source=pg_src, destination=S3(bucket="dwh", key="orders/")).run()

# Non-file source + explicit format.
Transfer(source=pg_src, destination=S3(bucket="dwh", key="orders/", format=Avro())).run()
```

### Architecture

```
+----------+    Vec<BatchStream>    +-------------+
|  Source  |  ===================>  | Destination |
|  reader  |  Arrow RecordBatch     |   writer    |
+----------+                        +-------------+
     |                                   ^
     v                                   |
  source schema -- user override --> validate against destination
   (inferred)      (schema=, per col)    (destination-native vocab)
                                              |
                                              v
                                         coerce per column
                                         (Tier 1/2/3)
```

- Single Rust process. `transferred` Python module is a PyO3 extension.
- `Source` trait: `partitions(self) -> Result<Vec<BatchStream>>`. Each `BatchStream` = one partition's async `Stream<Item = RecordBatch>`. Non-partitionable sources return a one-element `Vec`.
- `Destination` trait consumes `Vec<BatchStream>`. Single-file destinations serialize partitions; partition-aware destinations (e.g., partitioned Parquet directory, BQ multi-stream) run them concurrently.
- Async end-to-end: native async I/O via `AsyncArrowWriter` and `ParquetRecordBatchStream`. No `spawn_blocking`, no internal mpsc channels.
- Backpressure happens naturally — `.next().await` on the source stream blocks until the writer is ready.
- Schema resolution happens once, before partitions are produced.

**Schema resolution flow.** Source-owned; destination validates. Arrow is internal.

1. Source produces its inferred Arrow schema natively.
2. If `destination.schema=` is set, destination parses it (with ellipsis sentinel handling) into a per-column override map. Strict mode (no `...: ...`) requires every source column to appear; missing → fail.
3. Engine resolves canonical schema column-by-column: declared user type if present, else inferred type from source.
4. Destination validates each resolved column against its accepted type set. Plan-time fail (`SchemaError`) only when no Arrow cast kernel exists at all (type fundamentally incompatible, e.g. `geography → INT64`). Width/precision mismatches that *might* fit at the row level are deferred to step 6.
5. If an existing destination is present (file, table), compare resolved schema vs existing. Incompatibility → `SchemaError: source column 'X' (type Y) incompatible with existing destination column 'X' (type Z). Likely source schema drift. Override with columns=.`
6. Source emits batches; engine coerces each batch per-column to the canonical schema (Tier 1 auto, Tier 2 warn, Tier 3 fail). Arrow `cast` uses `safe=true`; first overflow row aborts the run. Atomic destinations guarantee no half-written state on failure.
7. Destination writes canonical batches, mapping to its native representation.

Destination trait surface (Rust, indicative shape):

```rust
trait Destination {
    /// Parse user-provided schema (with ellipsis sentinel) into a per-column override map.
    fn parse_user_schema(&self, schema: UserSchema) -> Result<UserOverrides>;
    /// Validate resolved Arrow schema against destination's accepted types.
    /// Returns canonical schema (after Tier 1 widening) or fails (Tier 3 / missing kernel).
    fn validate_schema(&self, source_arrow: ArrowSchema, overrides: UserOverrides) -> Result<ArrowSchema>;
    /// Existing write path.
    async fn write(self: Box<Self>, schema: SchemaRef, partitions: Vec<BatchStream>) -> Result<RunReport>;
}
```

`UserSchema` / `UserOverrides` are opaque carriers passed in from Python (string DSL, native lib objects, or mixed). Destination is sole interpreter.

### Memory model

Goal: keep per-worker memory consumption predictable, under a configured cap (default 256 MiB), without surprising OOMs.

Current model (serial, single partition):

- One batch in flight at any time. `source.next().await` yields one `RecordBatch`; `writer.write(&batch).await` consumes it; loop.
- No buffering between source and destination.
- Async readers don't prefetch; async writers buffer one row group internally (configure `WriterProperties::set_max_row_group_size` to keep this bounded — default is large).
- Peak per-pipeline memory ≈ `1 × batch_bytes + writer_row_group_buffer`.

Parallel partitions (deferred to partition feature):

- Per partition: same 1-batch-in-flight + writer buffer.
- Concurrency cap K via `stream::iter(partitions).buffered(K)`.
- Worst-case memory ≈ `K × (batch_bytes + writer_row_group_buffer)`.
- Default K = `min(parallelism_config, available_parallelism())`.
- Tune row group size down when K > 1.

Byte-aware budget (deferred):

- Currently no semaphore. Memory bounded by batch shape × K.
- If real workloads show skew (huge variable-width columns) blowing out the bound, introduce a byte-aware semaphore in `transferred-core` and have partitions acquire permits sized by `RecordBatch::get_array_memory_size()`.

Concurrent transfers in one process (deferred):

- Currently each transfer assumes it owns the worker's memory budget. Multiple `Transfer.run()` calls in one process compound memory.
- For now, run independent transfers in separate processes if isolation matters.

Python-side memory (iterable path via `_iterable_to_arrow`):

- Generator sources stream one row at a time. The internal `_iterable_to_reader` collects `_BATCH_SIZE` rows (4096) into a tuple via `itertools.batched`, calls `pa.RecordBatch.from_pylist(chunk)`, drops the tuple, hands the batch to Rust via Arrow C Data Interface. One FFI crossing per batch.
- Peak Python-side memory per batch ≈ `_BATCH_SIZE × avg_row_bytes × 2` (chunk + Arrow buffers briefly co-resident).
- List sources: caller is responsible for what they materialise — engine cannot help if the user pre-builds a 10 GiB list. Documented in `ArrowSource` docstring as "prefer a generator over a materialized `list` for large inputs".
- `memory_budget_mb=` knob (deferred): translated to row count via running average row size, adjusts batch_size adaptively.

**Conversion seam — design intent.** Row-shape normalization (dict / dataclass / pydantic → `dict[str, Any]`) and dict → Arrow batch building both happen on the **Python side**. Rust only ever sees `arrow::RecordBatch` arriving across the C Data Interface, single hot path.

Rationale:
- Both paths (Python via pyarrow, or Rust via PyO3) cross the CPython FFI **once per cell** — every read of a `PyLongObject` / `PyUnicodeObject` requires a CPython API call regardless of which language owns the loop. The speed gap is the constant factor (pyarrow ~2x faster than equivalent Rust + PyO3), not an order of magnitude.
- The decisive factor is **code volume and maintenance**: ~200 lines of unsafe-ish PyO3 conversion + null handling + schema inference + nested-type support on the Rust side, versus a single `pa.RecordBatch.from_pylist()` call.
- pyarrow has battle-tested type inference, null handling, nested types, and is the de-facto interop currency in the Python data ecosystem (Polars, DuckDB, pandas 2.0+).
- Cost accepted: pyarrow ships as an **optional dep** via `transferred[arrow]` extra (`transferred[iterable]` aliased) (~30 MB wheel). Base install stays lean for users who only use Rust-native sources/destinations (Parquet, future Postgres, BigQuery). Missing pyarrow at `ArrowSource` construction raises `ImportError` with install hint.

Fast path for callers who already have Arrow: `ArrowSource(pa_record_batch_reader)` skips the iterable conversion and goes straight to the C Data Interface. Future overloads (`pa.Table`, `pa.RecordBatch`) extend this fast path.

### Runtime contract

- **Atomic loads.** Each backend uses its own native atomic primitive.
    - BQ: Storage Write API in `pending` mode against a transient staging table in the destination dataset, then a server-side copy job with `WRITE_TRUNCATE` from staging into the final table, then `DROP TABLE staging`. Atomicity comes from the copy job; the Storage Write commit makes the staging table whole, the copy-replace makes the final table whole. Partitioning, clustering, description, labels, IAM on the final table are preserved (data replaced, table object not recreated). Schema enforcement is server-side: AppendRows rejects mismatched rows, the copy job rejects mismatched schemas. No client-side staging in GCS, no Parquet encoding, no `staging_bucket` knob on the public API. Errors surfaced as `TransferredError` subclasses.
    - Postgres: `BEGIN; DROP target; RENAME staging; COMMIT;`. Client-side schema compare needed here since there's no equivalent server-side enforcement.
  Transfers never leave the destination half-written. `mode="append"` and `mode="upsert"` are out of scope while the project is in initial development. `on_schema_change="replace"` to opt into destructive schema replacement is a deferred kwarg.
- **Source filter surface.** `table=` and `query=` are the two ways to bound the extract. No partial filter DSL on top — keeps the API one knob wide.
- **Credentials.** All GCP auth delegates to `gcp_auth`: Application Default Credentials, `GOOGLE_APPLICATION_CREDENTIALS` service-account JSON, gcloud user creds, workload identity. Postgres uses standard DSN-embedded creds or libpq env vars.
- **Run report.** `RunReport` returned by `.run()` is the canonical post-run record. Logs are for trace; `RunReport` is for programs.
- **Logging.** Rust uses `tracing`. A bridge layer emits events into Python's `logging` so users get one config story (`logging.getLogger("transferred").setLevel(...)`).
- **Batching.** Reader batch size is its own default (Parquet ≈ 1024 rows). No bytes target enforced; in-flight memory bounded by 1 batch per partition (see Memory model).
- **Concurrency.** Async end-to-end on the tokio multi-thread runtime owned by the Rust side. Partitions within one transfer run concurrently (deferred); separate transfers run in separate processes. PyO3 releases the GIL on every entry. Supported interpreters: Python 3.14 standard and free-threaded (`cp314`, `cp314t`) — see Tech stack.

### Incremental loads

See [INCREMENTAL.md](INCREMENTAL.md)

### Type mapping

User-facing vocabulary is **destination-native**. Arrow is the internal lingua franca, never spelled in the Python API. Each destination owns its own schema DSL parser (string forms like `"NUMERIC(18, 4)"`) and accepts native lib objects (`bigquery.SchemaField`, `pa.Field`) where the destination's ecosystem ships them. Cross-destination consistency is not a goal — `STRING` in BQ and `text` in PG and `Utf8` in Arrow are independent vocabularies.

Arrow covers most primitives directly. The tricky types — geometry, JSON, UUID, ranges, intervals, vendor-specific — go through a registry, not ad-hoc per-connector code.

**Lookup order for any source-native type:**

1. Native Arrow type if one matches (`int4` → `Int32`, `numeric(p,s)` → `Decimal128(p,s)`, `interval` → `Interval(MonthDayNano)`, `timestamptz` → `Timestamp(Microsecond, "UTC")`, …).
2. Canonical Arrow extension if one exists (`uuid` → `arrow.uuid`, `json`/`jsonb` → `arrow.json`).
3. Community extension we trust (PostGIS `geometry`/`geography` → `geoarrow.wkb` with CRS metadata).
4. Private `transferred.*` extension over the most structured storage type that loses nothing (Postgres ranges → `transferred.pg_range` over a `Struct{lower, upper, lower_inc, upper_inc, empty}`).
5. `arrow.opaque` as last resort, carrying raw bytes. Destinations that can't decode it refuse, loudly.

**Destinations declare capability per extension:**

```rust
enum ExtensionSupport {
    Native,                       // destination maps directly to its own type
    FallbackOnly(Fallback),       // destination can't represent it; apply Fallback
    Unknown,                      // destination doesn't know this extension at all
}

enum Fallback {
    Expand,   // range → multiple columns; struct → flatten
    Text,     // serialize to canonical string form
    Refuse,   // fail with diagnostic
}
```

**Coercion safety tiers.** Not every coercion is equally safe. The runtime classifies each and picks a default:

| Tier                 | What it covers                                           | Default               | Reporting               |
| -------------------- | -------------------------------------------------------- | --------------------- | ----------------------- |
| Safe (lossless)      | Range → expand. JSON → `arrow.json`. UUID → `arrow.uuid`. Standard primitive widening. `geography(_, 4326)` → BQ `GEOGRAPHY`. | Auto-apply            | INFO, in run summary    |
| Lossy structural     | Unknown type → `arrow.opaque` (bytes). Composite → struct flatten. Hstore → JSON. `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY` (planar→geodesic edge reinterpretation). | Auto-apply            | WARN, in run summary    |
| Lossy semantic       | CRS reprojection. `ST_MakeValid`. Z/M drop. Decimal truncation. tz coercion.            | **Fail**              | ERROR, stops the run    |

User overrides choose the strategy per column when the default doesn't fit:

```python
column_overrides={
    "valid":  Range(strategy="expand"),    # already the default; override only if you want "text"
    "blob":   Coerce(to="bytes"),
}
```

**Tier 3 workaround.** Lossy-semantic coercions are not implemented; the run fails on the offending column. Workaround = drop the column from the transfer via `columns=` or `skip_columns=` on the source. They are mutually exclusive.

**Run-summary contract.** Every coercion applied is recorded in `RunReport.coercions` and rendered in the end-of-run summary. Logs alone get ignored; the summary is where surprises become visible.

```
Coercions applied (3):
  [INFO] valid   tsrange → valid_lower, valid_upper, valid_lower_inc, valid_upper_inc, valid_empty
  [INFO] payload jsonb   → arrow.json
  [WARN] meta    address_t (composite) → STRUCT, field order preserved
```

Never silently coerce to `TEXT` or `BYTES` without a summary entry — that is the dlt failure mode and it produces broken pipelines that look green.

**Concrete coverage targets:**

| Source type (Postgres)        | Arrow representation                 | Notes                                                     |
| ----------------------------- | ------------------------------------ | --------------------------------------------------------- |
| `int2`/`int4`/`int8`          | `Int16`/`Int32`/`Int64`              | Native.                                                   |
| `numeric(p,s)`                | `Decimal128(p,s)` or `Decimal256`    | Native. `numeric` without precision → fail with override  |
| `text`/`varchar`              | `Utf8`                               | Native.                                                   |
| `bytea`                       | `Binary`                             | Native.                                                   |
| `bool`                        | `Boolean`                            | Native.                                                   |
| `date`                        | `Date32`                             | Native.                                                   |
| `timestamp`/`timestamptz`     | `Timestamp(Microsecond, tz)`         | Native. `tz=None` for `timestamp`.                        |
| `interval`                    | `Interval(MonthDayNano)`             | Native, exact match.                                      |
| `uuid`                        | `FixedSizeBinary(16)` + `arrow.uuid` | Canonical extension.                                      |
| `json`/`jsonb`                | `Utf8` + `arrow.json`                | Canonical extension.                                      |
| `geometry`/`geography` (PostGIS) | `Binary` + `geoarrow.wkb` + CRS   | Community extension. CRS from `geometry_columns`.         |
| `tsrange`/`int4range`/...     | `Struct` + `transferred.pg_range`    | Private extension. Default destination fallback = expand. |
| `hstore`, `ltree`, composites | `arrow.opaque` initially             | Later promotion to structured forms.                      |

### Tech stack

| Concern          | Choice                                              | Reason                                                         |
| ---------------- | --------------------------------------------------- | -------------------------------------------------------------- |
| Core             | Rust                                                | Performance, types, no GC.                                     |
| Python binding   | PyO3 + maturin, `cp314` + `cp314t`                  | Standard for adoption; free-threaded included.                 |
| Internal format  | Apache Arrow (`arrow-rs`)                           | Zero-copy to BQ, Parquet, Polars.                              |
| Async runtime    | Tokio                                               | Required by most cloud SDKs.                                   |
| Postgres         | `tokio-postgres` + binary `COPY`                    | `COPY` is the fastest extract path.                            |
| BigQuery         | Storage Write API via tonic + googleapis            | Direct write, no Parquet/GCS staging.                          |
| GCP auth         | `gcp_auth`                                          | ADC, service-account JSON, gcloud, workload identity.          |
| Object storage   | `object_store` crate                                | Unified S3/GCS/Azure API.                                      |
| Parquet          | `parquet` (arrow-rs)                                | Same family as Arrow. Audit gaps vs Polars.                    |
| Errors           | `thiserror`, surfaced as `transferred.TransferredError`      | One root exception, typed subclasses.                          |
| Logging          | `tracing` bridged into Python `logging`             | One config story for users.                                    |
| License          | MIT                                                 | Liberal. Matches the rest of the analytical Python/Rust stack. |

### Known risks

- **BigQuery Rust support is thin.** Mitigation: hand-rolled Storage Write client over tonic + googleapis. Don't depend on an unmaintained crate.
- **Parquet feature gaps.** Mitigation: keep a running list of formats Polars failed on; verify each against `arrow-rs` before claiming coverage. Upstream patches if we have to.
- **Schema drift between infer and override.** Mitigation: resolved schema is logged before the stream starts; destinations validate against it on write.
- **Tricky types silently broken.** Mitigation: type registry + destination capability declarations (see Type mapping). Default to refusal over coercion.
- **Free-threaded ecosystem maturity.** Mitigation: `transferred` itself only depends on PyO3 + arrow-rs across the FFI seam. Users on cp314t are responsible for their own dependency stack.

## Strategy

Versioned roadmap, milestone scope, and what's currently done live in [PLAN.md](./PLAN.md). DESIGN.md covers architecture and contracts only.
