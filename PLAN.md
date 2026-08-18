# `transferred` — Plan

What is left to build. Shipped versions move to [DONE.md](./DONE.md); architecture and contracts are in [DESIGN.md](./docs/design/DESIGN.md).

Legend: `[x]` done · `[~]` in progress · `[ ]` pending.

## 0.2.0 — BigQuery source + destination

Goal: add BigQuery source + destination. Atomic full load PG ↔ BQ. Direct type mapping; formal schema/coercion still deferred to 0.6.

**Scope:**

- `transferred-bigquery` destination: Storage Write API in `pending` mode against transient staging table → server-side copy job `WRITE_TRUNCATE` from staging into final → `DROP TABLE staging`. No GCS staging.
- `transferred-bigquery` source: Storage Read API.
- Destination table-creation options bag (additive over the source-derived DDL): BQ `partition_by=`/`cluster_by=`, both set-at-create and cost-relevant. A PG `primary_key=` waits for incremental loads, which is the only thing that reads one.
- Auth via `gcp_auth` (ADC, service-account JSON, gcloud, workload identity).
- Direct Arrow ↔ BQ type mapping: `geography(_, 4326)` → BQ `GEOGRAPHY`, `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY`. Unsupported types error. Tiered coercion (auto/warn/fail) deferred to 0.6.
- BQ `GEOGRAPHY` exists only in WGS84, so the mapping has to *decide* whether a `geoarrow.wkb` column is WGS84, not merely carry its CRS. `crs: "EPSG:4326"` is a string compare; a PROJJSON or WKT2 CRS needs PROJ, and no geoarrow crate supplies it — `geoarrow-schema` only carries the value and delegates conversion to a `CrsTransform` the caller writes, its own default silently dropping the CRS. So refusing anything but an authority code is the 0.2.0 answer. BQ reads the tag and its metadata for itself; where a shared `Wkb` ends up living is the interchange contract's call.
- Decide there whether `transferred.pg_range` becomes `transferred.range`. BQ `RANGE<DATE|DATETIME|TIMESTAMP>` is always `[lower, upper)` with NULL for an infinite bound and no empty range at all, so a BQ range fits the same five-field struct — at which point the `pg_` in the name is a lie, and `empty` reads as the PG-only field it is. Renaming is one constant plus the metadata every reader compares against, so it is a 0.2.0 decision, not a 0.1.0 hedge. The name generalises further than the mapping does: `int4range` and `numrange` have no BQ range to land in, so they go as the struct we already store or as a simpler type, decided per range when the mapping is written.
- `Timestamp(_, None)` → BQ `DATETIME`, never `TIMESTAMP`. Both Arrow `None` and PG `timestamp` mean wall-clock without a zone, and `TIMESTAMP` is an instant, so reaching for it would invent a zone for the commonest column type in a PG schema. Users who want an instant must name the zone the naive values are read in, which is 0.6's `schema=`.
- `TableFieldSchema.timestamp_precision` lets a BQ column hold picoseconds, which no Arrow unit reaches. Reading one truncates to `Nanosecond`.

**Tasks:**

- [ ] `transferred-bigquery` Storage Write client (tonic + googleapis).
- [ ] Atomic staging-table + copy-replace + drop-staging flow.
- [ ] `transferred-bigquery` source — Storage Read API.
- [ ] Auth integration (`gcp_auth`).
- [ ] Arrow ↔ BQ type mapping, internal only. The schema comes from the source, so no BQ type is ever named in Python here — that vocabulary arrives with `schema=` in 0.6. Type names come from the `TableFieldSchema.Type` enum in `google/cloud/bigquery/storage/v1/table.proto` — a proto we compile anyway for Storage Write, so prost generates the list and upstream owns it. Verified against googleapis master: `STRING, INT64, DOUBLE, STRUCT, BYTES, BOOL, TIMESTAMP, DATE, TIME, DATETIME, GEOGRAPHY, NUMERIC, BIGNUMERIC, INTERVAL, JSON, RANGE`, plus `Mode: NULLABLE/REQUIRED/REPEATED`.
  - Two vocabularies exist: Storage Write v1 is GoogleSQL (`INT64`, `BOOL`, `STRUCT`), the v2 REST jobs API is legacy (`INTEGER`, `BOOLEAN`, `RECORD`). Staging-table create + copy job go through v2, so both get touched.
  - Precision/scale are separate `TableFieldSchema` fields, `ARRAY` is `mode=REPEATED`, `STRUCT` carries `fields` — the enum names only the type.
- [ ] BQ env-gated integration test.
- [ ] Round-trip integration tests (PG ↔ BQ).

## 0.3 — S3 + GCS

- S3 destination (Parquet) via `object_store`.
- GCS destination (Parquet) — nearly free once S3 works.

## 0.4 — Incremental Postgres -> BQ

Model decided — see [INCREMENTAL.md](./docs/design/INCREMENTAL.md), D1–D10.

## 0.5 — Arrow interchange contract

Goal: state what the Arrow layer between a source and a destination *is*, so a connector author learns it from a document rather than from reading `pg_to_arrow.rs`.

**Scope:**

- The contract is implicit today. DESIGN.md §Type system records what 0.1 happens to do; the rules themselves live in each connector's match arms, so a new connector cannot tell which Arrow types it must accept, which it may emit, or what the tags oblige it to.
- Spell out the supported `DataType` set per direction, and what is deliberately outside it (`Union` — no Parquet encoding, `Dictionary`, `Duration`, `Interval` past Parquet's reach).
- Promote the extension tiers from prose to normative: canonical (`arrow.uuid`, `arrow.json`, `arrow.opaque`), community (`geoarrow.wkb`), ours (`transferred.*`, with 0.2.0's `transferred.pg_range` → `transferred.range` decision settled first).
- Say what a destination owes a tag it does not know. Files writes the metadata verbatim, Postgres refuses and names the type — both are defensible and neither is written down as the rule.
- Say where a shared extension type lives, and move it there. `Wkb` and the range type sit in `transferred-postgres` while BQ reads the same tags, so by here the duplication is real; the contract decides whether `transferred-core` owns every tag or connectors keep their own. `geoarrow-schema` is not the way out — 0.8.0 (March 2026) still requires `arrow-schema` 58 against our 59, so its `ExtensionType` impls are for a different crate's `Field`.
- Decide which of it is public Rust API. `Wkb`, `PgRange` and `range_fields` are `pub` so a caller can declare such a column in a hand-built Arrow schema — and because `tests/` is a separate crate that sees nothing else. `#[doc(hidden)]` is the alternative, for all of them together, and the contract is what makes the choice answerable.
- Conformance is a shared test corpus — one `RecordBatch` per contract row that a connector crate round-trips through itself. A trait with no behaviour would only restate the type signatures.

Not before here: hiding a `pub` type is a breaking change, so this cannot be a patch, and S3/GCS then incremental are wanted first. Landing it right before the schema redesign also gives that work a written target instead of a moving one.

## 0.6 — schema redesign

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
