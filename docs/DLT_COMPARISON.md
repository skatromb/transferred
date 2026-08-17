# transferred vs dlt

`dlt` is the tool `transferred` competes with: both move tables between
Postgres and files, both hand you Arrow. This measures one narrow route — a full-table
Postgres ↔ Parquet load — where `transferred` is faster, and states plainly where `dlt`
does things `transferred` cannot do at all.

Reproduce with `make perf`. Every number below comes from `perf/` in this repo.

## Methodology

| | |
|---|---|
| Rows | 10,000,000 |
| Columns | 22, listed in `perf/data.py:COLUMNS` |
| Types covered | `bigint`, `bool`, `smallint`, `integer`, `real`, `float8`, `numeric(12,4)`, `text`, `varchar(16)`, native enum, `citext`, `bytea`, `date`, `timestamp`, `timestamptz`, `uuid`, `jsonb`, `daterange`, `int8range`, PostGIS `geometry`, `geography` (SRID 4326 points, in columns without a type modifier) |
| Reported | Minimum of 3 runs, with `spread` = slowest / fastest |
| Compression | zstd on every leg, both engines |
| Host | Apple M4 Pro, 12 cores, 48 GiB, macOS 26.6.1 |
| Server | `imresamu/postgis:18-3.6` in Docker — PostgreSQL 18.1, `shared_buffers=128MB`, `max_wal_size=1GB` |
| Versions | `transferred` 0.1.1 (release build), dlt 1.30.0, pyarrow 24.0.0, connectorx 0.4.5, SQLAlchemy 2.0.52, psycopg2 2.9.12, duckdb 1.5.5, ADBC 1.12.0, fastparquet 2026.5.0 |

The minimum is reported rather than the mean because noise on a shared machine is
one-sided: the scheduler, another process and thermal throttling can only add time.
`spread` is what says whether three runs sufficed — and it disqualifies numbers, see
duckdb below. The first of the three doubles as the warm-up.

There is no `interval` column. Arrow maps it to `Interval(MonthDayNano)`, which
parquet-rs cannot write, so a table holding one never reaches a Parquet destination —
a `transferred` limitation, not a measurement choice.

**Not measured:** network latency, cloud object stores, concurrent load, incremental
runs, or anything at cloud-warehouse scale. Both engines run against a container on
localhost, which favours whichever engine spends less time in userspace — us.

## Results

Postgres → Parquet, all 22 columns:

| Engine | wall s | spread | peak RSS MB | rows/s | out MB |
|---|---|---|---|---|---|
| **transferred** | **15.04** | 1.02x | **419** | 664,915 | 244.3 |
| dlt, tuned | 21.47 | 1.07x | 3953 | 465,674 | 243.1 |
| dlt, defaults (1M rows) | 67.96 | 1.03x | 1475 | 14,714 | 107.2 |
| ADBC read + pyarrow write | 15.86 | 1.02x | 262 | 630,650 | 356.4 |
| duckdb `postgres_scanner` | 9.51 | 1.03x | 746 | 1,051,693 | 151.0 |

Parquet → Postgres:

| Engine | wall s | spread | peak RSS MB | rows/s | target MB |
|---|---|---|---|---|---|
| **transferred** | **16.52** | 1.13x | **144** | 605,339 | 2719 |
| dlt, tuned | 255.22 | 1.03x | 4609 | 39,182 | 4884 |
| dlt, defaults (1M rows) | 93.71 | 1.02x | 379 | 10,671 | 364 |
| ADBC ingest (20 columns) | 19.12 | 1.03x | 1612 | 522,952 | 2316 |

**What each engine had to be handed to swallow the 22 columns.** Only `transferred`
takes the fixture as it stands; the rest each needed something, and what they needed is
not the same kind of thing:

- **dlt** needed caller code, different per direction. Reading, a `query_adapter_callback`
  makes Postgres cast the four columns connectorx panics on (`daterange`, `int8range`,
  `geometry`, `geography`) to text — server-side, so free. Writing, an `add_map` rewrites
  every Arrow batch in Python: canonical extension types unwrapped, structs JSON-encoded,
  `bytea` hex-encoded. That one is charged, and it is the 104s broken out below.
- **ADBC** offers no hook at all — it has no Arrow-struct-to-Postgres mapping — so the
  range columns are projected away and its row moves 20 columns, not 22.
- **duckdb** was handed nothing, and offers nothing: it degrades on its own, on the way
  out. In the Parquet it writes, both ranges, `jsonb` and `geography` are `string` where
  ours are a struct and the two canonical extension types, and `uuid` is a bare
  `fixed_size_binary[16]` — the bytes, without the tag that says what they are.
  `geometry` and the enum it writes exactly as we do. Cheaper to move, and decided for you.

Against tuned dlt: **1.4x faster reading, 15x faster writing, and 9–32x less memory.**
Against dlt on its defaults the ratio is 45x reading and 57x writing, but that number
says more about the defaults than about dlt.

Three things the table does not say on its own.

**Not all of dlt's write time is dlt's.** Loading our Parquet needs a caller-side Arrow
rewrite (see *Type fidelity*), and that rewrite is 104s of the 255s — measured
separately by running it over the same fixture with no pipeline attached. dlt's own
extract-normalize-load is therefore ~151s, still 9x our 16.52s. The rewrite is not an
artifact of a lazy implementation: pyarrow has no vectorized kernel for hex-encoding
binary, formatting UUIDs, or serialising a struct, and `to_pylist()` alone — just
materialising the values as Python objects — is 63% of the struct columns' cost. Hand
tuning buys roughly 20%, not an order of magnitude.

**dlt's target table is 1.8x larger** — 4884 MB against 2719 MB for identical input,
because ten columns land as text. A `uuid` costs 16 bytes and its text form 36; EWKB hex
is about half again the size of the geometry it encodes.

**duckdb reads faster than we do**, 9.51s against 15.04s, and it is the only engine here
that reads Postgres in parallel. Its output is 151 MB against our 244 MB, since the
columns it flattens are a simpler payload to move. Both facts are true at once.

## What dlt has to be told before the comparison is fair

dlt's defaults are not its capabilities. Left alone it writes gzipped JSONL, reads
row by row through SQLAlchemy, and loads with `INSERT` statements. Every setting below
is from [dlt's own performance docs](https://dlthub.com/docs/reference/performance);
they are applied in `perf/workloads/_dlt.py:TUNING` and measured separately from the
defaults, so the cost of not knowing them is visible.

| Setting | dlt default | Used here | Why it matters |
|---|---|---|---|
| `loader_file_format` | `jsonl` (filesystem) | `parquet` | Without it there is no Parquet to compare |
| `backend` | `sqlalchemy` | `connectorx` | dlt's docs: connectorx is "2x faster than the PyArrow backend" |
| `read_parquet(use_pyarrow=)` | `False` | `True` | Otherwise Parquet is decoded into Python dicts |
| `read_parquet(chunksize=)` | `1000` | `1_000_000` | 1000-row batches |
| `DATA_WRITER__BUFFER_MAX_ITEMS` | `5000` | `1_000_000` | Also the Parquet row-group size |
| `NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS` | unset — no rotation | `1_000_000` | One table is one file, so `load.workers=20` idles |
| `DATA_WRITER__COMPRESSION` | `snappy` | `zstd` | Matches the fixtures; snappy is the faster codec, so leaving it would flatter dlt on time and penalise it on size |
| `RESTORE_FROM_DESTINATION` | `true` | `false` | Skips a round trip for pipeline state per run |

One default must be left alone: `add_dlt_id` is `False` for Parquet, and that is what
lets `dlt/normalize/items_normalizers/arrow.py:172-206` hardlink the extracted file
instead of rewriting it — the normalize step disappears entirely. Turning on lineage
columns forces a full read-modify-write. Breaking this would have made the comparison
meaningless.

## Why the gap is architectural, not a misconfiguration

**No dlt backend reads with `COPY ... TO STDOUT`.** `transferred` opens a binary
`COPY` stream (`crates/transferred-postgres/src/source.rs:51`) and decodes it straight
into Arrow. dlt's options, per `dlt/sources/sql_database/helpers.py:69`:

| Backend | Mechanism | Wire protocol |
|---|---|---|
| `sqlalchemy` (default) | SQLAlchemy `yield_per`, one dict per row | text |
| `pyarrow` | SQLAlchemy rows, then a Python-level transpose into Arrow | text |
| `pandas` | via `pandas.io.sql` | text |
| `connectorx` | Rust, the only binary one | binary |

dlt's own docs on the `pyarrow` backend:

> It uses `SQLAlchemy` to read rows in batches but then immediately converts them into
> `ndarray`, transposes it, and sets it as columns in an `Arrow` table.

So even dlt's Arrow path pays psycopg2's text protocol plus a Python transpose. Only
`connectorx` avoids both, which is why it is the backend measured here.

**On the write side dlt prefers `insert_values`** — `caps.preferred_loader_file_format`
in `dlt/destinations/impl/postgres/factory.py:155`. Its two faster paths each give
something up, and CSV is the better trade:

| Path | Mechanism | Cannot carry |
|---|---|---|
| `insert_values` (default) | batched `INSERT` | — |
| `csv` | pyarrow → CSV → `COPY ... FROM STDIN (FORMAT CSV)` | `bytea`, `struct` |
| `parquet` | ADBC binary `COPY` | `struct`; and it declares `int16`/`int32`/`float32` widened, then ships the original file, so `COPY` fails with `insufficient data left in message` |

`transferred` writes binary `COPY` into a staging table and swaps it in one
transaction (`crates/transferred-postgres/src/destination.rs:99`).

## Type fidelity

Both engines carry all 22 columns. What arrives differs.

dlt needs code for this, not configuration — and the mechanism differs by direction,
which is dlt's shape, not our framing:

- **Reading**, `query_adapter_callback` casts the four columns connectorx panics on
  (`daterange`, `int8range`, and both PostGIS types) to text. The cast runs in
  Postgres, so it is free at run time.
- **Writing**, `add_map` rewrites each Arrow batch in Python: unwrap the canonical
  extension types, JSON-encode structs, hex-encode `bytea`. This one is not free — it is
  the 104s broken out under *Results* — and it runs inside the measured region.

Both are in `perf/workloads/_dlt.py`. A counterintuitive trap: supplying a *type hint*
for an unmappable column makes things worse, not better. On the arrow backends it turns
a working load into a hard failure, because the fallback that handles SQL types Arrow
cannot represent is gated on `data_type is None`
(`dlt/common/libs/pyarrow.py:1205`). The one place a hint is required is `payload`:
without `{"data_type": "binary"}` the hex text lands in a `varchar` instead of `bytea`.

Postgres → Parquet → Postgres. Column types read with
`format_type(atttypid, atttypmod)`, not `information_schema`, which reports `numeric`
for a `numeric(12,4)` and would credit both engines with a loss neither makes.

| Column | Source | transferred | dlt |
|---|---|---|---|
| `id` | `bigint` | `bigint` | `bigint` |
| `is_active` | `boolean` | `boolean` | `boolean` |
| `small_count` | `smallint` | `smallint` | `smallint` |
| `mid_count` | `integer` | `integer` | `integer` |
| `ratio` | `real` | `real` | **`double precision`** |
| `amount` | `double precision` | `double precision` | `double precision` |
| `price` | `numeric(12,4)` | `numeric(12,4)` | `numeric(12,4)` |
| `name` | `text` | `text` | `character varying` |
| `code` | `character varying(16)` | **`text`** | **`character varying`** |
| `country` | `text` | `text` | `character varying` |
| `status` | enum | **`text`** | **`character varying`** |
| `tag` | `citext` | **`text`** | **`character varying`** |
| `payload` | `bytea` | `bytea` | `bytea`, via caller-side hex |
| `day` | `date` | `date` | `date` |
| `created_at` | `timestamp` | `timestamp` | **`timestamp with time zone`** |
| `updated_at` | `timestamptz` | `timestamptz` | `timestamptz` |
| `session_id` | `uuid` | `uuid` | **`character varying`** |
| `attrs` | `jsonb` | **`json`** | **`character varying`** |
| `valid_days` | `daterange` | `daterange` | **`character varying`** |
| `span` | `int8range` | `int8range` | **`character varying`** |
| `location` | `geometry` | `geometry` | **`character varying`** — EWKB hex |
| `region` | `geography` | `geography` | **`character varying`** |

Both engines preserve `numeric(12,4)`, and both drop a `varchar` length modifier, an
enum down to a string and `citext`'s case-insensitivity. `transferred` additionally
demotes `jsonb` to `json`. That is four losses for us against ten for dlt, and the ones
dlt cannot hold are the expensive ones: `uuid`, `jsonb`, both ranges and both PostGIS
types all arrive as text, which is also why its target table is 1.8x larger.

One dlt entry is worse than a loss. `created_at` is a naive `timestamp` at the source
and arrives as `timestamp with time zone`, so every value is silently reinterpreted as
UTC. Nothing errors, and a later read in a different session time zone returns different
instants.

## What dlt does that transferred does not

`transferred` is 0.1.1. It moves a whole table, once, between two systems. dlt is a
framework, and on every axis below it has something and we have nothing:

- **Incremental loading.** Cursor-based extraction with state persisted between runs.
  `transferred` reloads the whole table every time; incremental loads would be implemented later.
- **Write dispositions.** `merge`, `append`, dedup and multiple replace strategies.
  `transferred` has exactly one: replace, via a staging swap.
- **Schema evolution.** dlt migrates the destination as the source changes and records
  versions. `transferred` recreates the target.
- **Sources.** dozens of verified sources against our three — Postgres, files, and any
  Arrow PyCapsule producer.
- **Pipeline state.** dlt tracks load packages, retries partial loads and can resume.
  A failed `transferred` run leaves the target untouched and that is the whole story.
- **Transformation.** dlt normalizes nested data into child tables, applies hints and
  maps. `transferred` moves columns as they are.

Pick dlt when you need any of the above. This benchmark says only that when the job is
"move this table now", a Rust binary-`COPY` path costs less time and much less memory.
