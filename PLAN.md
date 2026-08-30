# `transferred` — Plan

What is left to build. Shipped versions move to [DONE.md](./DONE.md); architecture and contracts are in [DESIGN.md](./docs/design/DESIGN.md).

Legend: `[x]` done · `[~]` in progress · `[ ]` pending.

## 0.2.0 — BigQuery destination

Goal: atomic full load PG → BQ. Direct type mapping; formal schema/coercion still deferred to 0.7.

**Scope:**

- `transferred-bigquery` destination: Storage Write API in `pending` mode against a per-load staging table → `CREATE OR REPLACE TABLE target AS SELECT * FROM staging` → `DROP TABLE staging`. No GCS staging. Why a query and not a copy job is in DESIGN's runtime contract.
- Client: the official `google-cloud-bigquery` 0.16.x, with `google-cloud-bigquery-v2` for table and dataset metadata. Preview, and taken anyway — its write layer is the shape this load needs, the BQ source comes from the same family, and it carries no `arrow` of its own to collide with ours.
- Destination table-creation options bag (additive over the source-derived DDL): BQ `partition_by=`/`cluster_by=`, both set-at-create and cost-relevant. A PG `primary_key=` waits for incremental loads, which is the only thing that reads one.
- Auth via `google-cloud-auth` (ADC, service-account JSON, gcloud, workload identity).
- Direct Arrow ↔ BQ type mapping: `geography(_, 4326)` → BQ `GEOGRAPHY`, `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY`. Unsupported types error. Tiered coercion (auto/warn/fail) deferred to 0.7.
- BQ `GEOGRAPHY` exists only in WGS84, so the mapping has to *decide* whether a `geoarrow.wkb` column is WGS84, not merely carry its CRS. `crs: "EPSG:4326"` is a string compare; a PROJJSON or WKT2 CRS needs PROJ, and no geoarrow crate supplies it — `geoarrow-schema` only carries the value and delegates conversion to a `CrsTransform` the caller writes, its own default silently dropping the CRS. So refusing anything but an authority code is the 0.2.0 answer. BQ reads the tag and its metadata for itself; where a shared `Wkb` ends up living is the interchange contract's call.
- Decide there whether `transferred.pg_range` becomes `transferred.range`. BQ `RANGE<DATE|DATETIME|TIMESTAMP>` is always `[lower, upper)` with NULL for an infinite bound and no empty range at all, so a BQ range fits the same five-field struct — at which point the `pg_` in the name is a lie, and `empty` reads as the PG-only field it is. Renaming is one constant plus the metadata every reader compares against, so it is a 0.2.0 decision, not a 0.1.0 hedge. The name generalises further than the mapping does: `int4range` and `numrange` have no BQ range to land in, so they go as the struct we already store or as a simpler type, decided per range when the mapping is written.
- `Timestamp(_, None)` → BQ `DATETIME`, never `TIMESTAMP`. Both Arrow `None` and PG `timestamp` mean wall-clock without a zone, and `TIMESTAMP` is an instant, so reaching for it would invent a zone for the commonest column type in a PG schema. Users who want an instant must name the zone the naive values are read in, which is 0.7's `schema=`.
- `TableFieldSchema.timestamp_precision` lets a BQ column hold picoseconds, which no Arrow unit reaches. Reading one truncates to `Nanosecond`.

**Tasks:**

- [~] Storage Write client. `write.arrow(schema).pending(table)` opens the stream, `append(batch).send()` hands back one future per batch, `finalize()` then `commit()` close it. Row-level failures arrive as errors already, both `row_errors` and the `Response::Error` that rides inside a successful response.
- [ ] Arrow over the wire as IPC bytes, framed by `ipc.rs`. The crate depends on no `arrow`, so no version of it ever meets ours — Google's own integration tests frame the bytes the same way.
- [ ] Staging table through `TableService::insert_table`, no DDL. Arrays are `mode = REPEATED`, structs are `RECORD` plus nested `fields`, precision and scale are their own fields, and `partition_by`/`cluster_by` land as `TimePartitioning`/`Clustering`. The table name travels as data, so nothing here is spelled into SQL.
- [ ] Swap and drop-staging as query jobs, located from the dataset through `DatasetService::get_dataset`. Both name a table inside SQL text, so `check_identifier` still guards them.
- [ ] Auth. `google-cloud-auth` honours `GOOGLE_APPLICATION_CREDENTIALS` but has no `_JSON` twin, so CI reads the variable itself and builds the credential from the key with `service_account::Builder`.
- [~] Arrow ↔ BQ type mapping, internal only. The schema comes from the source, so no BQ type is ever named in Python here — that vocabulary arrives with `schema=` in 0.7. Primitives, `JSON`, `NUMERIC` and `BIGNUMERIC` land; `GEOGRAPHY`, `RANGE`, `uuid` and structs do not yet.
  - Type names are ours: `TableFieldSchema::type` is a bare `String` in the generated client, so nothing checks the spelling before the server does. Which vocabulary 0.7 borrows instead is open again.
  - What BQ accepts from an Arrow batch was measured, not assumed: `Decimal128` matches a `NUMERIC` column and `Decimal256` a `BIGNUMERIC`, each refusing the other; `TIMESTAMP` and `DATETIME` take microseconds only, naming the unit when they refuse; `Int16` and `Float32` widen on their own.
  - Precision/scale are separate `TableFieldSchema` fields, `ARRAY` is `mode=REPEATED`, `STRUCT` carries `fields` — the type name says nothing about any of them.
- [~] BQ env-gated integration test — `make check-integration`, credentials via `make gcp-login`.

## Interlude — per-destination run report

Runs once the BQ destination lands, while the API is still ours to change.

- `RunReport` becomes per-destination, with the type riding on the destination: `Destination[R]`, then `FilesDestination(Destination[FilesReport])`, and `Transfer[R]` infers `R` from the argument it is handed. `run()` returns `FilesReport` with its paths or `BigQueryReport` with its table and job, and `written_objects` goes away. The generic lives in the hand-written Python wrapper, so the generated `_native` stubs stay as they are.
- In Rust the report is an associated type on `Destination`, and the closed list of destinations lives in `transferred-py`, which constructs every connector anyway. A forgotten destination fails to compile, and so does one whose report has no Python conversion.

```rust
// transferred-core, naming no destination
pub trait Destination {
    type Written;
    async fn write_partitions(self, partitions: Vec<BatchStream>) -> Result<RunReport<Self::Written>>;
}
```

```rust
// transferred-py, the one place a closed list belongs
enum AnyDestination {
    Files(FilesDestination),
    Postgres(PostgresDestination),
    BigQuery(BigQueryDestination),
}

match destination {
    AnyDestination::Files(d) => report_class(Transfer::new(source, d).run().await?),
    AnyDestination::Postgres(d) => report_class(Transfer::new(source, d).run().await?),
    AnyDestination::BigQuery(d) => report_class(Transfer::new(source, d).run().await?),
}
```

- `Transfer` becomes `Transfer<D>`. The source stays `Box<dyn Source>`, having no report to type, so the parameter does not spread; `self: Box<Self>` drops back to `self`.
- DESIGN's API surface and run-report contract follow, before 0.3 starts.

## 0.3 — S3 + GCS

- S3 destination (Parquet) via `object_store`.
- GCS destination (Parquet) — nearly free once S3 works.

## 0.4 — BigQuery source

Storage Read API, on `google-cloud-bigquery-read`.

- Waits on that crate reaching crates.io: it sits at `0.0.0` in the repo, bootstrapped 19 August 2026, `ReadRows` server-side streaming on the 26th, regenerated on the 29th. Left on googleapis/google-cloud-rust#5745 are their librarian templates and a stream-resume design.
- The source is `create_read_session` plus the streaming `ReadRows`, with the Arrow IPC bytes coming back through `ipc.rs` the way they went out.
- Round-trip integration tests (PG ↔ BQ) land with the source, which is what reads a loaded table back.

## 0.5 — Incremental Postgres -> BQ

Model decided — see [INCREMENTAL.md](./docs/design/INCREMENTAL.md), D1–D10.

## 0.6 — Arrow interchange contract

Goal: state what the Arrow layer between a source and a destination *is*, so a connector author learns it from a document rather than from reading `pg_to_arrow.rs`.

**Scope:**

- The contract is implicit today. DESIGN.md §Type mapping records what 0.1 happens to do; the rules themselves live in each connector's match arms, so a new connector cannot tell which Arrow types it must accept, which it may emit, or what the tags oblige it to.
- Spell out the supported `DataType` set per direction, and what is deliberately outside it (`Union` — no Parquet encoding, `Dictionary`, `Duration`, `Interval` past Parquet's reach).
- Promote the extension tiers from prose to normative: canonical (`arrow.uuid`, `arrow.json`, `arrow.opaque`), community (`geoarrow.wkb`), ours (`transferred.*`, with 0.2.0's `transferred.pg_range` → `transferred.range` decision settled first).
- Say what a destination owes a tag it does not know. Files writes the metadata verbatim, Postgres refuses and names the type — both are defensible and neither is written down as the rule.
- Say where a shared extension type lives, and move it there. `Wkb` and the range type sit in `transferred-postgres` while BQ reads the same tags, so by here the duplication is real; the contract decides whether `transferred-core` owns every tag or connectors keep their own. `geoarrow-schema` is not the way out — 0.8.0 (March 2026) still requires `arrow-schema` 58 against our 59, so its `ExtensionType` impls are for a different crate's `Field`.
- Decide which of it is public Rust API. `Wkb`, `PgRange` and `range_fields` are `pub` so a caller can declare such a column in a hand-built Arrow schema — and because `tests/` is a separate crate that sees nothing else. `#[doc(hidden)]` is the alternative, for all of them together, and the contract is what makes the choice answerable.
- Conformance is a shared test corpus — one `RecordBatch` per contract row that a connector crate round-trips through itself. A trait with no behaviour would only restate the type signatures.

Not before here: hiding a `pub` type is a breaking change, so this cannot be a patch, and S3/GCS then incremental are wanted first. Landing it right before the schema redesign also gives that work a written target instead of a moving one.

## 0.7 — schema redesign

Implements the source-owned schema direction decided during the Interlude. Replaces the direct per-connector type mapping shipped in 0.1.0/0.2.0 with a canonical vocab + coercion engine + user `schema=` API.

**Scope:**

- Schema inference direction: `Source → Destination`. Source schema is ground truth; user overrides via `schema=` short-circuit source inference; coercion check resolves source → destination compatibility.
- Loud-fail semantics:
  - Static (plan-time): declared precision can never fit target → `SchemaError` before any read.
  - Runtime (row-level): Arrow `cast` with `safe=true`. First overflow row aborts run. Atomic destinations guarantee no half-written state.
- Drift framing (stateless): if destination already exists, compare source vs existing destination schema. Error:
  ```
  SchemaError: source column 'foo' (type Y) incompatible with existing destination
  column 'foo' (type Z). Likely source schema drift. Override with schema=.
  ```
- Formal coercion engine — Tier 1 auto, Tier 2 warn, Tier 3 fail. Reporting via `RunReport.coercions`.
- User schema API in Python: single `schema=` knob, always `dict[column, type]` with typed objects rather than string literals, so a typo is a red squiggle. Full by default; partial when an ellipsis key (`...: ...`) is present — remaining columns inferred. Source-side filtering via `columns=` / `skip_columns=` (mutually exclusive). Parameterless types are module-level singletons (`t.INT64`), parameterised ones constructors (`t.Numeric(18, 4)`) — precision and scale sit outside the type name upstream too, so those are ours.
- Vocabulary is per destination, no cross-destination DSL:

| Destination | `schema=` values | Python → Rust seam |
| ----------- | ---------------- | ------------------ |
| `FilesDestination` | `pa.DataType` | assemble a `pa.Schema`, hand over `__arrow_c_schema__()` — the C Data Interface `ArrowSource` already uses |
| `BigQueryDestination` | `transferred.bigquery.types` | `TableFieldSchema`, the Storage Write wire shape |
| `PostgresDestination` | `transferred.postgres.types` | type name; extension types validated by `::regtype` |
- Type registry — coverage extended beyond the 0.1.0/0.2.0 direct-mapping baseline.

**Tasks:**

- [ ] Source schema introspection trait surface.
- [ ] Destination schema validation trait surface (replaces 0.1.0/0.2.0 ad-hoc per-connector mapping).
- [ ] Coercion engine: Arrow `cast` with `safe=true`, Tier-aware reporting wired into `RunReport`.
- [ ] User schema API in Python: `schema=`, `columns=`, `skip_columns=`.
  - PG type names come from `postgres_types::Type` — `type_gen.rs` is marked "Autogenerated file - DO NOT EDIT" and generated from PostgreSQL's own catalog. 185 consts at 0.2.14, ranges and arrays included (`TS_RANGE`, `INT4_RANGE`, `NUM_RANGE`, `INT4_ARRAY`), with `Kind::{Array, Range, Multirange}` carrying the element type. Already a transitive dep through `tokio-postgres`, so unlike the BQ enum this needs no new dependency and no proto compile.
  - Extension types (`geometry`, `hstore`, `ltree`) get OIDs at `CREATE EXTENSION` time and cannot be in a static list. One escape hatch, `pg.Raw("hstore")`, validated by `::regtype` — the same split `postgres_types` draws with `Kind::Other`.
  - Typmod stays ours: `numeric(18, 4)` is `Type::NUMERIC` plus a typmod int, so `pg.Numeric(18, 4)` is a `transferred` constructor. Mirrors BQ, where precision/scale are separate `TableFieldSchema` fields.
  - BQ type names come from the `TableFieldSchema.Type` enum 0.2.0 already compiles, in its GoogleSQL spelling — the legacy v2 one (`INTEGER`, `RECORD`) stays internal. `transferred-bigquery` re-exports the prost enum; `transferred-py` wraps it in `#[pyclass(eq, eq_int)]`. pyo3 can't be a dep of the connector crate, so the wrapper is a hand-written exhaustive `match` — which is the point: it stops compiling when Google adds a variant. `pyo3-stub-gen` 0.23 ships `gen_stub_pyclass_enum` / `gen_stub_pyclass_complex_enum`, so the existing stub-drift CI gate covers the Python side.
  - Neither vocabulary is borrowed from a Python SDK — probed the alternatives:
  - `google-cloud-bigquery` rejected. 63 MB installed (38 MB grpc, 12 MB cryptography, 25 packages), and it buys no checking anyway: `SchemaField("x", "INT65").to_api_repr()` constructs fine and fails only server-side, because `field_type` is a bare `str`. A slim `--no-deps` install doesn't factor out — dropping grpc lands at 6.1 MB then `ImportError: google.rpc`, adding `googleapis-common-protos` + `grpcio-status` lands at 7.5 MB then `requests`, and each step is an unsupported combo that breaks on the next SDK bump. `types-google-cloud-bigquery` is not published, and stubs wouldn't help — `schema=` needs runtime objects.
  - `sqlglot` rejected. Cheap (3.1 MB, pure Python, no deps) and validates at construction — `DataType.build("INT65", dialect="bigquery")` raises `ParseError`, and it even knows PG's tail (`hstore`, `tsrange`, `geometry`, `jsonb`, `int4range`; not `ltree`). But it normalises to one cross-dialect vocabulary — BQ `INT64` becomes `DType.BIGINT` — which is the cross-destination DSL DESIGN rules out, it still takes strings so there's no autocomplete, and its checking is structural only: `NUMERIC(18, 4, 5)` parses clean.
  - Same call the non-SDK tools make (sqlglot, DuckDB's BQ extension, ADBC) — dbt-bigquery and dlt eat the full SDK because they are heavy apps already. The difference here is that the proto gives us the list for free, so nothing is hand-maintained.
- [ ] Migrate Parquet, PG, BQ connectors to new trait surface.

## Backlog

- Format dispatch — moot while Parquet is the only format, so deferred until a second exists. File source no `format`: inherit source's (path extension, byte-sniff on ambiguity); explicit `format`: convert. Non-file source: default `Parquet()` or convert if explicit.
- Postgres source `query=` — an arbitrary SELECT in place of `table=`, compiled to the same COPY.
- Cross-connector `batch_size` / byte-based memory budget (`set_max_row_group_bytes` + reader batch). Design against ≥2 connectors (PG in 0.1.0, BQ in 0.2.0); don't pin to one connector's shape.
- Airflow / Dagster / whatever is popular operators
- `sslrootcert=` DSN parameter — pin a CA file instead of the platform store, for `verify-full` against RDS or Cloud SQL. Needs stripping the key before `tokio_postgres::Config` sees it.
- Time out a dead Postgres connection. A perf leg was seen parked in tokio's IO driver on a socket the server had already let go — client `ESTABLISHED`, no backend left — and it waited 40 minutes until killed. `tokio-postgres` defaults `keepalives_idle` to 2 hours and we set no read timeout, so a half-open connection hangs a transfer instead of failing it. Wants an idle in the tens of seconds, and a decision on whether a stalled server counts as one.
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
