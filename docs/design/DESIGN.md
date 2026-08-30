# `transferred` — Design

Package name `transferred` on both crates.io and PyPI. Workspace is split into per-connector crates (`transferred-core`, `transferred-files`, `transferred-postgres`, `transferred-bigquery`) plus a Python binding crate and `transferred-perf` (unpublished perf harness). `transferred-files` is the local-filesystem connector — it owns the `Files` source + destination *and* the file-format codecs (Parquet now; Csv/Avro later, in-crate). Formats split into their own crates only if that earns its keep. Workspace version is shared across all crates; untie only if release cadence diverges.

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
- **Incremental loads in later releases**
- **Picks specific columns or excludes ones in later releases**

Explicit non-goals:

- No transformations.
- No streaming / CDC.
- No scheduling — keep that for Airflow/Dagster/cron.
- No CLI — Python entrypoint only.

## How

### Versioning

While the project is in its initial development, breaking changes are allowed but require a minor bump; patch bumps stay non-breaking. No deprecation cycles, no compat shims. Full API review precedes the stable release; strict SemVer after.

### API surface (Python, code-first)

```python
from transferred import Transfer, PostgresSource, FilesDestination, BigQueryDestination
from transferred.formats import Parquet

# Local directory destination — no cloud creds needed.
# `format=` defaults to `Parquet()`.
Transfer(
    source=PostgresSource(dsn="postgres://...", table="public.orders"),
    destination=FilesDestination(path="./out/orders"),
).run()

# BigQuery destination.
Transfer(
    source=PostgresSource(dsn="postgres://...", table="public.orders"),
    destination=BigQueryDestination(project="my-proj", dataset="raw", table="orders"),
).run()
```

**Source auto-coercion.** `source=` accepts anything iterable, not only `Source` instances. Python-side dispatcher normalises:

```python
# Generator of dicts
def fetch_orders():
    for page in api.paginate("/orders"):
        yield from page["results"]

Transfer(source=fetch_orders(), destination=FilesDestination("out")).run()

# List of dicts
Transfer(source=[{"id": 1}, {"id": 2}], destination=FilesDestination("out")).run()

# Dataclasses — converted via dataclasses.asdict per chunk
Transfer(source=order_iter, destination=FilesDestination("out")).run()
```

Module layout for the iterable + Arrow path:

- `transferred.arrow.ArrowSource` — accepts anything exposing `__arrow_c_stream__`. The Arrow seam into Rust: `arrow-pyarrow`'s `FromPyArrow` reads the capsule, so `pa.Table`, `pa.RecordBatch`, `pa.RecordBatchReader` and non-pyarrow producers (polars, duckdb) all arrive the same way and pyarrow itself is never imported here.
- `transferred.iterable._iterable_to_arrow` — wraps an iterable of `dict` / `@dataclass` / `pydantic.BaseModel` as an `ArrowSource`. The batching itself lives in `_iterable_to_reader`, which builds the `pa.RecordBatchReader` (see Memory model). Depends on `arrow` (one-way), not the other way.
- `transferred.transfer.Transfer` — Python wrapper around `_native.Transfer`. Coerces iterables on construction.

Dispatcher rules in `Transfer.__init__`:

- `Source` instance (e.g. `FilesSource`, `ArrowSource`) → used directly.
- Any other `Iterable` (excluding `str`/`bytes`/`bytearray`/`dict`) → wrapped via `_iterable_to_arrow`. Rows are batched into `pa.RecordBatch` of `_BATCH_SIZE` (4096), one FFI crossing per batch, schema inferred from first batch.
- Anything else → `TypeError`.

Row shapes accepted by the iterable path: `dict`, `dataclass`, `pydantic.BaseModel` (v2 — recognised by `model_dump`, so pydantic is never imported to convert a row). All normalized to `dict[str, Any]` on the Python side via a once-sniffed converter; pyarrow then builds the `RecordBatch`. `namedtuple` / `attrs` / `msgspec.Struct` deferred — trivial to add when requested.

Source accepts `table=` OR `query=` (mutually exclusive):

```python
PostgresSource(dsn="...", query="SELECT id, total FROM orders WHERE region = 'EU'")
PostgresSource(dsn="...", table="public.orders", skip_columns=["legacy_blob"])
```

**Schema direction — source-owned, destination-validated.** Source schema is ground truth: connectors infer it natively (PG `information_schema`, Parquet file metadata, Arrow batch schema, …) and preserve it end-to-end. Destination validates the inferred schema against its accepted type set and fails loudly on incompatibility, naming the column and both types. Arrow is internal, never spelled in Python.

There is no user-facing schema knob and none is designed. A column whose type the destination
cannot take is dropped with `skip_columns=` on the source.

`.run()` returns a `RunReport`:

```python
report = transfer.run()
report.rows            # 12_481_902
report.bytes_written   # 1_503_948_211
report.written_objects # ["out/part-00001.parquet", ...] — paths/URIs/tables written
report.duration        # timedelta
report.coercions       # list[Coercion] — column, original type, target, level
```

No staging inventory. Staging artifacts are an implementation detail of each destination's
atomicity primitive and are always cleaned up; a `keep_staging=` escape hatch stays out until
someone needs to debug a real failure with it.

No row-level Python callbacks. The FFI boundary is crossed once per transfer (per-batch for `ArrowSource`), not per row.

### File destinations and formats

File-shaped destinations are decoupled from file formats. A destination describes **where** bytes land; a format codec describes **how** they are encoded.

- `FormatRead` (decode → Arrow) and `FormatWrite` (encode ← Arrow) traits — split, not one symmetric trait, so a read-only or write-only codec doesn't have to stub the other half. Both traits and the `Parquet` codec live in `transferred-files`. Implementations carry encoder knobs:
  - `Parquet(compression="zstd")` — keeps the parquet-rs default row-group size (1,048,576 rows; `DEFAULT_MAX_ROW_GROUP_ROW_COUNT`). Row-group sizing is a write-side memory lever but isn't exposed yet — revisit once a byte-based cap (`set_max_row_group_bytes`) earns its keep.
  - `Avro(...)`
  - `Json(...)`
  - `Csv(...)`
- File-shaped destinations carry a `format`, defaulting to `Parquet()`:
  - `FilesDestination(path, format=Parquet(), single_file=False)` — local filesystem. `path` is always a directory (see below).
  - `S3Destination(bucket, key, format=Parquet())`, `GCSDestination(bucket, key, format=Parquet())` — cloud, `object_store`-backed, each carrying its own typed auth params. Separate classes, not a `FilesDestination(backend=)` enum: per-backend auth surfaces (S3 region/keys/endpoint vs GCS service-account) differ, and `object_store`'s own unified surface is either Rust builders or a stringly-typed options bag — neither a clean typed Python API.
- Row-protocol destinations have no `format` knob — the wire protocol is the encoding:
  - `BigQueryDestination(project, dataset, table)` — Storage Write API.
  - `PostgresDestination(dsn, table)` — `COPY ... FROM STDIN`.

**`FilesDestination` output shape.** `path` is always a directory, overwritten if it exists;
written atomically via a tmp dir + rename. Written file paths come back in the run report.

| `single_file` | output |
| ------------- | ------ |
| `False` (default) | one `part-NNNNN.<ext>` per source partition |
| `True` | all partitions flattened into one `<dir>.<ext>` (named after the directory) |

A flag, not extension inference — no path-shape ambiguity (dotted dirs, type
conflicts). The directory has no extension, so format is never inferred from it.

**Format resolution.** `format=` defaults to `Parquet()` on both `FilesSource` and
`FilesDestination` — no inheritance from the source, no extension sniffing. Inferring a
format is only meaningful once a second codec exists; until then it is a branch with one
outcome.

Byte-copy short-circuit (won't do): even when source and destination resolve to the same format, the engine always decodes through Arrow. Skipping the decode would bypass schema validation and coercion reporting, and the perf win does not justify a second code path.

```python
from transferred import FilesDestination, S3Destination
from transferred.formats import Csv

FilesDestination("out")                          # one part-NNNNN.parquet per source partition
FilesDestination("out", single_file=True)        # all partitions flattened into out.parquet
FilesDestination("out", format=Csv())            # same shape, different codec
S3Destination(bucket="dwh", key="orders/")       # cloud, same defaults
```

### Architecture

```
+----------+    Vec<BatchStream>    +-------------+
|  Source  |  ===================>  | Destination |
|  reader  |  Arrow RecordBatch     |   writer    |
+----------+                        +-------------+
     |                                     ^
     v                                     |
 source schema  --------------------->  validate against its
   (inferred)                           accepted types, then
                                        coerce (Tier 1/2/3)
```

- Single Rust process. `transferred` Python module is a PyO3 extension.
- `Source` trait: `stream_partitions(self: Box<Self>) -> Result<Vec<BatchStream>>`. Each `BatchStream` = one partition's async `Stream<Item = Result<RecordBatch>>`. Non-partitionable sources return a one-element `Vec`.
- `Destination` trait: `write_partitions(self: Box<Self>, partitions: Vec<BatchStream>) -> Result<RunReport>`. Both traits are single-shot — consuming `Box<Self>` makes that a type error rather than a runtime one. Single-file destinations serialize partitions; partition-aware destinations (e.g., partitioned Parquet directory, BQ multi-stream) run them concurrently.
- Async end-to-end: native async I/O via `AsyncArrowWriter` and `ParquetRecordBatchStream`. No `spawn_blocking`, no internal mpsc channels.
- Backpressure happens naturally — `.next().await` on the source stream blocks until the writer is ready.
- Schema resolution happens once, before partitions are produced.

**Schema resolution flow.** Source-owned; destination validates. Arrow is internal.

1. Source produces its inferred Arrow schema natively.
2. Destination validates each column against its accepted type set. Plan-time fail (`SchemaError`) only when the type is fundamentally incompatible and no Arrow cast kernel exists at all. Width and precision mismatches that *might* fit are left to step 4.
3. If a destination already exists (file, table), its schema is compared against the source's. Incompatibility → `SchemaError`, naming the column and both types.
4. Source emits batches; the engine coerces each batch per column (Tier 1 auto, Tier 2 warn, Tier 3 fail). Arrow `cast` uses `safe=true`; the first overflow row aborts the run. Atomic destinations guarantee no half-written state on failure.
5. Destination writes the batches, mapping to its native representation.

Step 2 is the destination's, so `Destination` grows a schema-validation method next to `write_partitions`.

### Memory model

Goal: keep per-worker memory consumption predictable — bounded by batch shape, not by table size — without surprising OOMs. No configured cap yet; see the deferred byte-aware budget below.

Current model (serial, single partition):

- One batch in flight at any time. `source.next().await` yields one `RecordBatch`; `writer.write(&batch).await` consumes it; loop.
- No buffering between source and destination.
- Async readers don't prefetch; async writers buffer one row group internally (configure `WriterProperties::set_max_row_group_row_count` to keep this bounded — default 1,048,576 rows). `set_max_row_group_bytes` offers a byte-based cap, the natural cross-connector memory lever once ≥2 connectors exist.
- Peak per-pipeline memory ≈ `1 × batch_bytes + writer_row_group_buffer`.

Parallel partitions (deferred to partition feature):

- Per partition: same 1-batch-in-flight + writer buffer.
- Concurrency cap K via `stream::iter(partitions).buffered(K)`.
- Worst-case memory ≈ `K × (batch_bytes + writer_row_group_buffer)`.
- Default K = `min(parallelism_config, available_parallelism())`.
- Tune row group size down when K > 1.

Deferred (both):

- Byte-aware budget. Memory is bounded by batch shape × K, with no semaphore. If real workloads show skew (huge variable-width columns) blowing out the bound, `transferred-core` gets one and partitions acquire permits sized by `RecordBatch::get_array_memory_size()`.
- Concurrent transfers in one process. Each transfer assumes it owns the worker's budget, so several `Transfer.run()` calls compound memory. Run them in separate processes if isolation matters.

Python-side memory (iterable path via `_iterable_to_arrow`):

- Generator sources stream one row at a time. The internal `_iterable_to_reader` collects `_BATCH_SIZE` rows (4096) into a tuple via `itertools.batched`, calls `pa.RecordBatch.from_pylist(chunk)`, drops the tuple, hands the batch to Rust via Arrow C Data Interface. One FFI crossing per batch.
- Peak Python-side memory per batch ≈ `_BATCH_SIZE × avg_row_bytes × 2` (chunk + Arrow buffers briefly co-resident).
- List sources: caller is responsible for what they materialise — engine cannot help if the user pre-builds a 10 GiB list. Same for a `pa.Table`; `ArrowSource`'s docstring asks for a reader once the data outgrows RAM.
- `memory_budget_mb=` knob (deferred): translated to row count via running average row size, adjusts batch_size adaptively.

**Conversion seam — design intent.** Row-shape normalization (dict / dataclass / pydantic → `dict[str, Any]`) and dict → Arrow batch building both happen on the **Python side**. Rust only ever sees `arrow::RecordBatch` arriving across the C Data Interface, single hot path.

Rationale:
- Both paths cross the CPython FFI **once per cell** — every read of a `PyLongObject` / `PyUnicodeObject` is a CPython API call whichever language owns the loop. The gap is a constant factor (pyarrow ~2x), not an order of magnitude.
- So the decisive factor is **code volume**: ~200 lines of unsafe-ish PyO3 conversion, null handling, schema inference and nested types, against one `pa.RecordBatch.from_pylist()` call into a battle-tested implementation that the rest of the ecosystem already speaks.
- Cost accepted: pyarrow is an **optional dep** via the `transferred[arrow]` extra (`transferred[iterable]` aliased, ~30 MB wheel), so a base install stays lean for Rust-native connectors. Missing pyarrow at iterable conversion raises `ImportError` with an install hint; `ArrowSource` needs no pyarrow of its own.

Fast path for callers who already have Arrow: `ArrowSource(arrow_stream)` skips the iterable conversion and goes straight to the C Data Interface.

### Runtime contract

- **Atomic loads.** Each backend uses its own native atomic primitive.
    - BQ: Storage Write API in `pending` mode against a staging table named per load, then `CREATE OR REPLACE TABLE target AS SELECT * FROM staging`, then `DROP TABLE staging`. That one statement is where atomicity comes from. It is a query rather than the cheaper copy job because rows the Storage Write API has just committed sit in [write-optimized storage](https://docs.cloud.google.com/bigquery/docs/write-api-rest), which a copy job does not read — it returns zero rows, a clone the same, and a rename is refused with `has streaming data`. Flushing takes minutes, up to 90 in rare cases, and cannot be asked for. A `SELECT` is the only reader that sees them, so a load pays for one scan of what it wrote and the target is recreated — labels, description and table-level IAM do not survive. Staging carries a timestamp because a table created under a just-dropped name stays invisible to the Storage Write API, which refuses the write stream with `NotFound`. It is created through `tables.insert` with a typed field list rather than DDL, which is where `partition_by`/`cluster_by` land too, so the only SQL a load builds is the swap and the drop. Schema enforcement is server-side: AppendRows rejects a batch whose Arrow field does not match the column, naming both. No GCS staging, no Parquet encoding, no `staging_bucket` knob on the public API.
    - Postgres: staging table built from the source-derived schema, `COPY ... FROM STDIN`, then `BEGIN; DROP target; RENAME staging; COMMIT;` under transactional DDL. The swap runs through `Client::transaction()`, whose `Drop` rolls back — otherwise a failed statement strands the session in an aborted transaction and silently swallows staging cleanup. No client-side schema compare: source schema wins and the target is replaced outright, so there is nothing to compare against. Indexes, grants and ownership are not preserved.
  Transfers never leave the destination half-written. `mode="append"` and `mode="upsert"` are out of scope while the project is in initial development. `on_schema_change="replace"` to opt into destructive schema replacement is a deferred kwarg.
- **Source filter surface.** `table=` and `query=` bound the extract. No partial filter DSL on top — keeps the API one knob wide.
- **Errors.** Every failure surfaces as a `TransferredError` subclass, whichever connector raised it.
- **Credentials.** All GCP auth delegates to `google-cloud-auth`: Application Default Credentials, `GOOGLE_APPLICATION_CREDENTIALS` service-account JSON, gcloud user creds, workload identity. Postgres uses standard DSN-embedded creds or libpq env vars.
- **Run report.** `RunReport` returned by `.run()` is the canonical post-run record. Logs are for trace; `RunReport` is for programs.
- **Logging.** Rust uses `tracing`. A bridge layer emits events into Python's `logging` so users get one config story (`logging.getLogger("transferred").setLevel(...)`).
- **Batching.** Reader batch size is its own default (Parquet ≈ 1024 rows). No bytes target enforced; in-flight memory bounded by 1 batch per partition (see Memory model).
- **Concurrency.** Async end-to-end on a tokio runtime the Rust side builds per `run()` — current-thread while a transfer is one serial pipeline; multi-thread arrives with concurrent partitions. Separate transfers run in separate processes. `run()` detaches from the GIL for its whole duration. Supported interpreters: Python 3.14 standard and free-threaded (`cp314`, `cp314t`) — see Tech stack.

### Incremental loads

See [INCREMENTAL.md](INCREMENTAL.md)

### Type mapping

Arrow is the internal lingua franca and the only vocabulary the mapping speaks; no type name is spelled in the Python API. Cross-destination consistency is not a goal either — `STRING` in BQ, `text` in PG and `Utf8` in Arrow are independent vocabularies.

Arrow covers most primitives directly. The tricky types — geometry, JSON, UUID, ranges, intervals, vendor-specific — are meant to go through a registry rather than ad-hoc per-connector code. **That registry is deferred and does not exist yet.** The Arrow schema is the entire contract between a source and a destination, and each destination pattern-matches `(DataType, extension name)` for itself:

| Arrow representation      | Files (Parquet)                | Postgres                                                        |
| ------------------------- | ------------------------------ | --------------------------------------------------------------- |
| Native types              | written as-is                  | mapped by `arrow_to_pg`                                         |
| `arrow.uuid`, `arrow.json`| metadata written verbatim      | `uuid`, `json`                                                  |
| `geoarrow.wkb`            | metadata written verbatim      | `geometry`/`geography`, with the SRID when the CRS is an authority code |
| `arrow.opaque`            | metadata written verbatim      | `bytea`, type name dropped                                      |
| Anything else             | written as-is                  | refused, naming the Arrow type                                  |

Parquet interprets none of it — the schema goes straight to the writer, so every extension and every CRS spelling survives untouched. Postgres is the only destination that reads the tags, which is why the registry waits for a second reader rather than being built for one.

`DataType::Binary` carries three meanings — plain bytes, `arrow.opaque`, `geoarrow.wkb` — separated only by the extension name. A destination that forgets to check it writes a wrong column type silently, so each destination resolves all three in one place (`arrow_to_pg::ToPgColumn`), never split between the encoder and the DDL.

**Lookup order for any source-native type:**

1. Native Arrow type if one matches (`int4` → `Int32`, `numeric(p,s)` → `Decimal128(p,s)`, `interval` → `Interval(MonthDayNano)`, `timestamptz` → `Timestamp(Microsecond, "UTC")`, …).
2. Canonical Arrow extension if one exists (`uuid` → `arrow.uuid`, `json`/`jsonb` → `arrow.json`).
3. Community extension we trust (PostGIS `geometry`/`geography` → `geoarrow.wkb` with CRS metadata).
4. Private `transferred.*` extension over the most structured storage type that loses nothing (Postgres ranges → `transferred.pg_range` over a `Struct{lower, upper, lower_inc, upper_inc, empty}`).
5. `arrow.opaque` as last resort, carrying raw bytes. Destinations that can't decode it refuse, loudly.

Once the registry exists, a destination declares per extension whether it maps natively, needs a
fallback (expand a range into columns, flatten a struct, serialize to text) or refuses.

**Coercion safety tiers.** Not every coercion is equally safe. The runtime classifies each and picks a default:

| Tier                 | What it covers                                           | Default               | Reporting               |
| -------------------- | -------------------------------------------------------- | --------------------- | ----------------------- |
| Safe (lossless)      | Range → expand. JSON → `arrow.json`. UUID → `arrow.uuid`. Standard primitive widening. `geography(_, 4326)` → BQ `GEOGRAPHY`. | Auto-apply            | INFO, in run summary    |
| Lossy structural     | Unknown type → `arrow.opaque` (bytes). Composite → struct flatten. Hstore → JSON. `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY` (planar→geodesic edge reinterpretation). | Auto-apply            | WARN, in run summary    |
| Lossy semantic       | CRS reprojection. `ST_MakeValid`. Z/M drop. Decimal truncation. tz coercion.            | **Fail**              | ERROR, stops the run    |

Tier defaults are not overridable; per-column control arrives with the deferred type registry. Lossy-semantic coercions are not implemented at all, so the run fails on the offending column — drop it with `skip_columns=` on the source.

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
| `numeric(p,s)`                | `Decimal128(p,s)`                    | Native. Bare `numeric` → `Decimal128(38, 9)` (BQ `NUMERIC`) + WARN |
| `text`/`varchar`              | `Utf8`                               | Native.                                                   |
| `bytea`                       | `Binary`                             | Native.                                                   |
| `bool`                        | `Boolean`                            | Native.                                                   |
| `date`                        | `Date32`                             | Native.                                                   |
| `timestamp`/`timestamptz`     | `Timestamp(Microsecond, tz)`         | Native. `tz=None` for `timestamp`.                        |
| `interval`                    | `Interval(MonthDayNano)`             | Native, exact match.                                      |
| `uuid`                        | `FixedSizeBinary(16)` + `arrow.uuid` | Canonical extension.                                      |
| `json`/`jsonb`                | `Utf8` + `arrow.json`                | Canonical extension.                                      |
| `enum`, `citext`              | `Utf8`                               | Native. The wire form already is the text; the variant set and case-folding are not carried. |
| `geometry`/`geography` (PostGIS) | `Binary` + `geoarrow.wkb` + CRS   | Community extension. EWKB passed through; column CRS from typmod. |
| `tsrange`/`int4range`/...     | `Struct{lower, upper, lower_inc, upper_inc, empty}` + `transferred.pg_range` | Private extension. Bounds null when infinite; `empty` is a tag bit no pair of bounds can express. Destination fallback = expand (0.2.0). |
| `hstore`, `ltree`, composites | `arrow.opaque` initially             | Later promotion to structured forms.                      |

### Tech stack

| Concern          | Choice                                              | Reason                                                         |
| ---------------- | --------------------------------------------------- | -------------------------------------------------------------- |
| Core             | Rust                                                | Performance, types, no GC.                                     |
| Python binding   | PyO3 + maturin, `cp314` + `cp314t`                  | Standard for adoption; free-threaded included.                 |
| Internal format  | Apache Arrow (`arrow-rs`)                           | Zero-copy to BQ, Parquet, Polars.                              |
| Async runtime    | Tokio                                               | Required by most cloud SDKs.                                   |
| Postgres         | `tokio-postgres` + binary `COPY`                    | `COPY` is the fastest extract path.                            |
| BigQuery         | `google-cloud-bigquery` + `google-cloud-bigquery-v2` | Storage Write direct, no Parquet/GCS staging. Google's own.    |
| GCP auth         | `google-cloud-auth`                                 | ADC, service-account JSON, gcloud, workload identity.          |
| Object storage   | `object_store` crate                                | Unified S3/GCS/Azure API.                                      |
| Parquet          | `parquet` (arrow-rs)                                | Same family as Arrow. Audit gaps vs Polars.                    |
| Errors           | `thiserror`, surfaced as `transferred.TransferredError`      | One root exception, typed subclasses.                          |
| Logging          | `tracing` bridged into Python `logging`             | One config story for users.                                    |
| License          | MIT                                                 | Liberal. Matches the rest of the analytical Python/Rust stack. |

### Known risks

- **The BigQuery client is a preview.** Mitigation: `google-cloud-bigquery` is Google's own and moving weekly, which cuts both ways. Everything it touches sits in one module, and the Arrow bytes crossing to it are framed by us, so a breaking release is a local edit.
- **Parquet feature gaps.** Mitigation: keep a running list of formats Polars failed on; verify each against `arrow-rs` before claiming coverage. Upstream patches if we have to.
- **Schema drift between source and an existing destination.** Mitigation: the inferred schema is logged before the stream starts; destinations validate against it on write.
- **Tricky types silently broken.** Mitigation: each destination refuses an Arrow type it cannot represent and names it, rather than coercing (see Type mapping); the registry replaces that per-connector matching later.
- **Free-threaded ecosystem maturity.** Mitigation: `transferred` itself only depends on PyO3 + arrow-rs across the FFI seam. Users on cp314t are responsible for their own dependency stack.

## Strategy

Versioned roadmap and milestone scope live in [PLAN.md](../../PLAN.md), shipped versions in [DONE.md](../../DONE.md). DESIGN.md covers architecture and contracts only.
