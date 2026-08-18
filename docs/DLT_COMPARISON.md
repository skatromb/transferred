# transferred vs dlt

Both `transferred` and `dlt` move a whole table between Postgres and files. This measures the two legs of that job separately — `Postgres → Parquet` and `Parquet → Postgres` — at 10M rows and 22 columns. `duckdb` is here as the engine to compete in performance to, even though it's more of an analytics/connector tool.

Reproduce the timings with `make perf-full` and the type table with `make fidelity`. Every
number comes from `perf/` in this repo; how it was measured is at the bottom.

## Postgres → Parquet

| Engine | wall s | avg CPU % | RSS avg/peak MB | out MB |
|---|---|---|---|---|
| **transferred** | 14.66 | **86** | **195/261** | 243 |
| duckdb `postgres_scanner` | **9.84** | 98 | 1455/1788 | **123** |
| dlt, tuned | 21.32 | 94 | 2340/3977 | 243 |
| dlt, defaults (*10x less rows*) | 71.66 | 97 | 3738/6579 | 33 |

- **duckdb reads 1.5x faster and writes half the bytes** — 123 MB against our 243, because
  the ranges, `jsonb` and `geography` it flattens to text compress far better than the
  struct and extension types we keep.
- We are the only engine that stays inside a few hundred megabytes: 261 MB peak against
  1788 and 3977, on one core.
- dlt tuned is 1.45x slower than us. On its defaults it manages only 14k rows/s, which is more than 10 times worse than `transferred`.

## Parquet → Postgres

| Engine | wall s | avg CPU % | RSS avg/peak MB | target MB |
|---|---|---|---|---|
| **transferred** | 14.28 | 40 | **129/133** | **2719** |
| duckdb `postgres_scanner` | **11.39** | **33** | 251/271 | 2972 |
| dlt, tuned | 229.75 | 95 | 2433/2816 | 3366 |
| dlt, defaults (*10x less rows*) | 95.03 (x10) | 86 | 3816/5547 | 415 |

- **duckdb writes 1.25x faster** and lands a slightly bigger table doing it — 2972 MB against our 2719, because the text forms it carries cost more on disk than the native types we send.
- **Neither of us is CPU-bound.** Both sit near `CPU/wall` 0.45: one Postgres backend
  saturates one core parsing the `COPY` stream while the client waits.
- **dlt is 16x slower with 21x the peak memory.**

## Types coercion

All three engines carry all 22 columns, each in its own way — and dlt only once we wrote
code for it. Both halves are in `perf/workloads/_dlt.py`. duckdb and `transferred` were
handed the table as it stands.

| Source type | transferred | duckdb | dlt |
|---|---|---|---|
| `bigint`, `boolean`, `smallint`, `integer`, `double precision`, `date`, `timestamptz` | kept | kept | kept |
| `real` | `real` | `real` | `double precision` |
| `numeric(12,4)` | `numeric(12,4)` | `numeric(12,4)` | `numeric(38,10)` |
| `text` | `text` | `character varying` | `character varying` |
| `character varying(16)` | `text` | `character varying` | `character varying` |
| `enum` | **`text`** | **`character varying`** | **`character varying`** |
| `citext` | **`text`** | **`character varying`** | **`character varying`** |
| `bytea` | `bytea` | `bytea` | `bytea`, via caller-side hex |
| `timestamp` | `timestamp` | `timestamp` | **`timestamptz`** |
| `uuid` | `uuid` | `uuid` | **`character varying`** |
| `jsonb` | **`json`** | **`character varying`** | **`character varying`** |
| `daterange` | `daterange` | **`character varying`** | **`character varying`** |
| `int8range` | `int8range` | **`character varying`** | **`character varying`** |
| `geometry` | `geometry` | **`geometry`, SRID 0** | **`character varying`** |
| `geography` | `geography` | **`character varying`** | **`character varying`** |

- Bold is a change that is sensible: 3 for `transferred`, 7 for `duckdb`, 9 for `dlt`. Left
  plain is everything that doesn't affect field's usage — `character varying` with no length
  *is* `text` in Postgres, and `real` → `double precision`, `numeric(12,4)` →
  `numeric(38,10)`, `character varying(16)` → `text` only widen the type, preserving its properties.
- **Two losses land quietly**, the column looking typed while its meaning has moved:
  - **dlt turns a naive `timestamp` into an instant** by loading it as UTC, so the same row
    reads `07:00` in a UTC session and `08:00` in `Europe/Stockholm`, and
    `date_trunc('day', …)` moves rows across midnight. CI in UTC sees nothing wrong.
  - **duckdb keeps `geometry` and loses its SRID**, its GeoParquet metadata carrying no CRS.
    `ST_Transform` and anything mixing it with a 4326 geometry then error out; only
    `::geography` hides it, by assuming 4326. Its text-carried `geography` keeps 4326 in the
    EWKB hex — the typed column loses what the untyped one preserves.


## Why such a difference

- **Reading, no dlt backend uses binary `COPY ... TO STDOUT`.** Only `connectorx` speaks the
  binary protocol at all; the rest read SQLAlchemy rows
  (`dlt/sources/sql_database/helpers.py:69`). We stream binary `COPY` straight into Arrow.
- **Writing, dlt prefers batched `INSERT`**
  (`dlt/destinations/impl/postgres/factory.py:155`), and its faster CSV path is still text
  on the wire. We write binary `COPY` into a staging table and swap it in one transaction.
- **duckdb's architecture is not the problem** — it's faster than any other tool, but pays on fidelity instead, flattening to  text whatever it cannot type.

## What dlt does that transferred does not

`transferred` is 0.1.2. It moves a whole table, once, between two systems. dlt is a
framework, and on every axis below it has something and we have nothing:

- **Incremental loading.** Cursor-based extraction with state persisted between runs.
  `transferred` reloads the whole table every time.
- **Write dispositions.** `merge`, `append`, dedup and multiple replace strategies.
  `transferred` has exactly one: replace, via a staging swap.
- **Schema evolution.** dlt migrates the destination as the source changes and records
  versions. `transferred` recreates the target.
- **Sources.** Dozens of verified sources against our three — Postgres, files, and any
  Arrow PyCapsule producer.
- **Pipeline state.** dlt tracks load packages, retries partial loads and can resume.
  A failed `transferred` run leaves the target untouched and that is the whole story.
- **Transformation.** dlt normalizes nested data into child tables, applies hints and
  maps. `transferred` moves columns as they are.

Pick dlt when you need any of the above. This benchmark says only that when the job is
"move this table now", a Rust binary-`COPY` path costs a fraction of the time and a
fraction of the memory — and that duckdb, if the schema it keeps is enough for you, is
faster than both.


---

## How it was measured

- **Two legs, timed separately.** A round trip hides which engine reads well and which
  writes well.
- **Nobody parallelises the Postgres side.** Every engine holds one backend and one `COPY`,
  polled through a leg with `pg_stat_activity`. Client-side differs: we are yet single-threaded, while duckdb's CPU peaks past 200% on its own decode and encode.
- **The type table is not part of the timed suite.** `make fidelity` loads each engine's own
  dump into Postgres and reads the landed types with `format_type(atttypid, atttypmod)`;
  `information_schema` would report a bare `numeric` for a `numeric(12,4)` and credit
  everyone with a loss nobody makes. It answers the same at any scale, so run it small — and
  after the suite whose numbers you are quoting, since it reseeds the table.
- **Each engine loads back its own extract.** A shared fixture cannot be fair — ours tags
  ranges with a `transferred.pg_range` extension no other engine reads. So what a file
  has already lost was lost in the extract leg, where the type table says so.
- **Round-robin, 3 passes, fastest reported.** This machine slows by roughly half over an
  hour of sustained load, so one workload's repeats back to back would rank engines by
  their position in the suite. Every workload runs once per pass instead; noise is
  one-sided, so the fastest pass is the engine's own cost.
- **A number is only comparable to one from the same suite.** Pass-to-pass drift stayed
  under 1.1x here, except duckdb's read leg, whose first pass ran 2x slow behind disk
  writeback from building the dumps — its 9.84s is the two passes that agree, and a
  separate suite of only these two engines reproduced it at 9.91s.
- **Nothing is done to the server between runs.** Restarting it, `pg_prewarm` and
  `checkpoint` were each tried and each measured as worth nothing.
- **dlt is measured twice**, on its defaults and tuned per its own performance docs, so
  the cost of not knowing the settings is visible instead of blamed on dlt.
- **Postgres is outside the CPU and RSS figures.** It runs in a container; the numbers are
  the engine's own process tree, sampled every 250 ms. Peak RSS comes from `rusage`.
- **The data is synthetic and regular** — every value a function of `i`, no NULLs anywhere
  (`perf/data.py`), so zstd compresses it better than a real table would. Same bytes for
  every engine, but the size columns are optimistic.
- **No `interval` column**: Arrow maps it to `Interval(MonthDayNano)`, which parquet-rs cannot
  write. Our limitation, not a measurement choice.
- **Apple M4 Pro, 12 cores, 48 GiB, macOS 26.6.1**. PostgreSQL 18.1 in Docker
  (`imresamu/postgis:18-3.6`, stock config), zstd on every leg. `transferred` 0.1.2
  (release, thin LTO), `dlt` 1.30.0, `duckdb` 1.5.5.
- **Not measured**: concurrent load, incremental runs.

## What dlt has to be told first

dlt's defaults are not its capabilities: left alone it writes gzipped JSONL, reads row by
row through SQLAlchemy and loads with `INSERT`. The tuned rows have all of the following
applied, taken from
[dlt's own performance docs](https://dlthub.com/docs/reference/performance) and collected
in `perf/workloads/_dlt.py:TUNING`:

- `loader_file_format=parquet` (default `jsonl`), `backend=connectorx` (default
  `sqlalchemy`), `read_parquet(use_pyarrow=True, chunksize=1M)` (defaults `False`, 1000),
  `DATA_WRITER__BUFFER_MAX_ITEMS=1M` (default 5000, also its row-group size),
  `NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS=1M`, `DATA_WRITER__COMPRESSION=zstd` (default
  snappy), `RESTORE_FROM_DESTINATION=false`.
- `add_dlt_id` stays at its Parquet default of `False` on purpose: it is what lets
  normalize hardlink the extracted file instead of rewriting it. Turning lineage columns
  on forces a full read-modify-write and would make the comparison meaningless.
- The types need code, not settings, and it differs by direction. Reading, a
  `query_adapter_callback` casts the four columns connectorx panics on (`daterange`,
  `int8range`, and both PostGIS types) to text — server-side, so free. Writing, an
  `add_map` hex-encodes `bytea` because CSV cannot carry it — inside the measured region.
- A trap: supplying a *type hint* for an unmappable column makes things worse. On the
  Arrow backends it turns a working load into a hard failure, the fallback for SQL types
  Arrow cannot represent being gated on `data_type is None`
  (`dlt/common/libs/pyarrow.py:1205`). The one place a hint is required is `bytea` —
  without `{"data_type": "binary"}` the hex text lands in a `character varying`.
