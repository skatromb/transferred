# `transferred` — Plan

What is left to build. Shipped versions move to [DONE.md](./DONE.md); architecture and contracts are in [DESIGN.md](./docs/design/DESIGN.md).

Legend: `[x]` done · `[~]` in progress · `[ ]` pending.

## 0.2.0 — BigQuery destination

Goal: atomic full load PG → BQ. Direct type mapping, no coercion engine.

**Scope:**

- `transferred-bigquery` destination: Storage Write API in `pending` mode against a per-load staging table → `CREATE OR REPLACE TABLE target AS SELECT * FROM staging` → `DROP TABLE staging`. No GCS staging. Why a query and not a copy job is in DESIGN's runtime contract.
- Client: the official `google-cloud-bigquery` 0.16.x, with `google-cloud-bigquery-v2` for table and dataset metadata. Preview, and taken anyway — its write layer is the shape this load needs, the BQ source comes from the same family, and it carries no `arrow` of its own to collide with ours.
- Destination table-creation options bag (additive over the source-derived DDL): BQ `partition_by=`/`cluster_by=`, both set-at-create and cost-relevant. A PG `primary_key=` waits for incremental loads, which is the only thing that reads one.
- Auth via `google-cloud-auth` (ADC, service-account JSON, gcloud, workload identity).
- Direct Arrow ↔ BQ type mapping: `geography(_, 4326)` → BQ `GEOGRAPHY`, `geometry(_, 4326)` no Z/M → BQ `GEOGRAPHY`. Unsupported types error, no tiered coercion.
- BQ `GEOGRAPHY` exists only in WGS84, so the mapping has to *decide* whether a `geoarrow.wkb` column is WGS84, not merely carry its CRS. `crs: "EPSG:4326"` is a string compare; a PROJJSON or WKT2 CRS needs PROJ, and no geoarrow crate supplies it — `geoarrow-schema` only carries the value and delegates conversion to a `CrsTransform` the caller writes, its own default silently dropping the CRS. So refusing anything but an authority code is the 0.2.0 answer. BQ reads the tag and its metadata for itself; where a shared `Wkb` ends up living is the interchange contract's call.
- Decide there whether `transferred.pg_range` becomes `transferred.range`. BQ `RANGE<DATE|DATETIME|TIMESTAMP>` is always `[lower, upper)` with NULL for an infinite bound and no empty range at all, so a BQ range fits the same five-field struct — at which point the `pg_` in the name is a lie, and `empty` reads as the PG-only field it is. Renaming is one constant plus the metadata every reader compares against, so it is a 0.2.0 decision, not a 0.1.0 hedge. The name generalises further than the mapping does: `int4range` and `numrange` have no BQ range to land in, so they go as the struct we already store or as a simpler type, decided per range when the mapping is written.
- `Timestamp(_, None)` → BQ `DATETIME`, never `TIMESTAMP`. Both Arrow `None` and PG `timestamp` mean wall-clock without a zone, and `TIMESTAMP` is an instant, so reaching for it would invent a zone for the commonest column type in a PG schema. An instant needs the zone named, which nothing in the API can say yet.
- `TableFieldSchema.timestamp_precision` lets a BQ column hold picoseconds, which no Arrow unit reaches. Reading one truncates to `Nanosecond`.

**Tasks:**

- [~] Storage Write client. `write.arrow(schema).pending(table)` opens the stream, `append(batch).send()` hands back one future per batch, `finalize()` then `commit()` close it. Row-level failures arrive as errors already, both `row_errors` and the `Response::Error` that rides inside a successful response.
- [ ] Arrow over the wire as IPC bytes, framed by `ipc.rs`. The crate depends on no `arrow`, so no version of it ever meets ours — Google's own integration tests frame the bytes the same way.
- [ ] Staging table through `TableService::insert_table`, no DDL. Arrays are `mode = REPEATED`, structs are `RECORD` plus nested `fields`, precision and scale are their own fields, and `partition_by`/`cluster_by` land as `TimePartitioning`/`Clustering`. The table name travels as data, so nothing here is spelled into SQL.
- [ ] Swap and drop-staging as query jobs, located from the dataset through `DatasetService::get_dataset`. Both name a table inside SQL text, so `check_identifier` still guards them.
- [ ] Auth. `google-cloud-auth` honours `GOOGLE_APPLICATION_CREDENTIALS` but has no `_JSON` twin, so CI reads the variable itself and builds the credential from the key with `service_account::Builder`.
- [~] Arrow ↔ BQ type mapping, internal only. The schema comes from the source, so no BQ type is ever named in Python. Primitives, `JSON`, `NUMERIC` and `BIGNUMERIC` land; `GEOGRAPHY`, `RANGE`, `uuid` and structs do not yet.
  - Type names are ours: `TableFieldSchema::type` is a bare `String` in the generated client, so nothing checks the spelling before the server does.
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

Goal: state what the Arrow types layer between a source and a destination *is*, so a connector author learns it from a document rather than from reading `pg_to_arrow.rs`.

**Scope:**

- The contract is implicit today. DESIGN.md §Type mapping records what 0.1 happens to do; the rules themselves live in each connector's match arms, so a new connector cannot tell which Arrow types it must accept, which it may emit, or what the tags oblige it to.
- Spell out the supported `DataType` set per direction, and what is deliberately outside it (`Union` — no Parquet encoding, `Dictionary`, `Duration`, `Interval` past Parquet's reach).
- Promote the extension tiers from prose to normative: canonical (`arrow.uuid`, `arrow.json`, `arrow.opaque`), community (`geoarrow.wkb`), ours (`transferred.*`, with 0.2.0's `transferred.pg_range` → `transferred.range` decision settled first).
- Say what a destination owes a tag it does not know. Files writes the metadata verbatim, Postgres refuses and names the type — both are defensible and neither is written down as the rule.
- Say where a shared extension type lives, and move it there. `Wkb` and the range type sit in `transferred-postgres` while BQ reads the same tags, so by here the duplication is real; the contract decides whether `transferred-core` owns every tag or connectors keep their own. `geoarrow-schema` is not the way out — 0.8.0 (March 2026) still requires `arrow-schema` 58 against our 59, so its `ExtensionType` impls are for a different crate's `Field`.
- Decide which of it is public Rust API. `Wkb`, `PgRange` and `range_fields` are `pub` so a caller can declare such a column in a hand-built Arrow schema — and because `tests/` is a separate crate that sees nothing else. `#[doc(hidden)]` is the alternative, for all of them together, and the contract is what makes the choice answerable.
- Conformance is a shared test corpus — one `RecordBatch` per contract row that a connector crate round-trips through itself. A trait with no behaviour would only restate the type signatures.

Not before here: hiding a `pub` type is a breaking change, so this cannot be a patch, and S3/GCS then incremental are wanted first.

## Backlog

- Postgres source `query=` — an arbitrary SELECT in place of `table=`, compiled to the same COPY.
- Cross-connector `batch_size` / byte-based memory budget (`set_max_row_group_bytes` + reader batch). Design against ≥2 connectors (PG in 0.1.0, BQ in 0.2.0); don't pin to one connector's shape.
- Airflow / Dagster / whatever is popular operators
- `sslrootcert=` DSN parameter — pin a CA file instead of the platform store, for `verify-full` against RDS or Cloud SQL. Needs stripping the key before `tokio_postgres::Config` sees it.
- Retries for the transient errors
- CLI
- Multiple destinations `Transfer`s
- Collapse `Source::stream_partitions` (`Vec<BatchStream>`) → a single `BatchStream`. The destination consumes batches without caring about source partition identity, so output partitioning becomes a destination/format policy. Simplifies the `Source` contract; trade-off is losing the source-partition → part-file mapping (parallel-per-partition write).
- Try to run code in a [dev container](https://zed.dev/blog/dev-containers)

## Never ~~say never~~
- Transformations beyond what type mapping forces.
- YAML/TOML config.
- Streaming and CDC (but who knows... v2.0?)
