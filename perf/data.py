"""The dataset every perf workload shares.

One wide Postgres table is the single source of truth for schema and scale — the
Parquet fixtures are dumped from it, so no two generators can drift apart. Its
columns cover what `transferred-postgres` maps: ints, floats, decimals, strings,
temporals, uuid, json, ranges and PostGIS geometry.
"""

from __future__ import annotations

import os

ROWS = int(os.environ.get("PERF_ROWS", "50_000_000").replace("_", ""))
"""Rows in the shared wide dataset. Override via `PERF_ROWS=N`."""

ROWS_PER_GROUP = 1_000_000
"""Row-group size handed to the baselines, to match what our own writer produces.

parquet-rs uses `DEFAULT_MAX_ROW_GROUP_ROW_COUNT = 1024*1024`, so a baseline left on
its own default — 5000 rows for dlt's writer — emits far more, smaller row groups,
which compress worse with zstd and confound output-size comparisons.
"""

SLOW_ROWS = int(os.environ.get("PERF_SLOW_ROWS", "1_000_000").replace("_", ""))
"""Rows for baselines that move every row through Python. Override via `PERF_SLOW_ROWS=N`.

Same bargain as `PYTHON_ROWS`: they are here to show the gap, and the gap is just
as visible at a million rows as it would be after a quarter-hour at `ROWS`.
"""

TABLE = "perf_wide"
"""Table the seed creates and the Postgres workloads read."""

_POINT = "st_point(10 + (i % 1000) * 0.001, 55 + (i % 1000) * 0.001)"
"""Point expression shared by the geometry and geography columns."""

COLUMNS: tuple[tuple[str, str], ...] = (
    ("id", "i::bigint"),
    ("is_active", "i % 7 = 0"),
    ("small_count", "(i % 32767)::smallint"),
    ("mid_count", "(i % 1000000)::integer"),
    ("ratio", "(i * 1.5)::real"),
    ("amount", "(i * 0.000001)::float8"),
    ("price", "((i % 1000000) * 0.0137)::numeric(12, 4)"),
    ("name", "'row-' || i"),
    ("code", "('c' || i % 9999)::varchar(16)"),
    ("country", "chr(65 + i % 26) || chr(65 + i % 17)"),
    ("status", "(array['pending', 'active', 'closed'])[1 + i % 3]::perf_status"),
    ("tag", "('Tag-' || i % 512)::citext"),
    ("payload", "int8send(i)"),
    ("day", "'2020-01-01'::date + i % 2000"),
    ("created_at", "'2020-01-01'::timestamp + (i % 2000) * interval '7 hours'"),
    ("updated_at", "'2020-01-01'::timestamptz + (i % 2000) * interval '11 hours'"),
    ("session_id", "('00000000-0000-4000-8000-' || lpad(to_hex(i), 12, '0'))::uuid"),
    ("attrs", "jsonb_build_object('k', i % 100, 'tags', array['a', 'b'])"),
    (
        "valid_days",
        "daterange('2020-01-01'::date + i % 500, '2020-01-01'::date + i % 500 + 30)",
    ),
    ("span", "int8range(i, i + 100)"),
    ("location", f"st_setsrid({_POINT}, 4326)"),
    ("region", f"st_setsrid({_POINT}, 4326)::geography"),
)
"""Every column of `TABLE`, as `(name, server-side expression)`.

Held as data rather than one SQL blob so the same list generates both the seed and
the narrowed views. Covers what `transferred-postgres` maps: ints, floats, decimals,
strings, temporals, uuid, json, ranges and PostGIS.

Carries no `interval` column: Arrow maps it to `Interval(MonthDayNano)`, which
parquet-rs cannot write, so a table holding one never reaches Parquet.
"""

BINARY_COLUMNS = ("payload",)
"""Columns holding `bytea`, which no text-protocol `COPY` can carry as-is."""

CAST_TO_TEXT = ("valid_days", "span", "location", "region")
"""Columns dlt reads only once Postgres has cast them to text — two ranges, two PostGIS.

connectorx panics in Rust on each of these before dlt sees a row, and no dlt
setting reaches that far. Casting in the query is the documented way out, and it
costs nothing at run time because the server does the work.
"""

CAPPED = f"{TABLE}_capped"
"""`TABLE` capped at `SLOW_ROWS`, for baselines that move every row through Python."""


def views_sql() -> str:
    """SQL creating `CAPPED`.

    It exists for baselines that move every row through Python at tens of
    microseconds each: at `ROWS` one would outlast the rest of the suite combined.
    Capping keeps them in the run and comparable on `rows/s`, the trade the
    iterable workloads already make.

    Dropped rather than replaced: `create or replace view` refuses to change a
    column list, and this one follows `COLUMNS`.
    """
    return (
        f"drop view if exists {CAPPED} cascade;"
        f"create view {CAPPED} as select * from {TABLE} limit {SLOW_ROWS};"
    )


def seed_sql(rows: int) -> str:
    """SQL recreating `TABLE` with `rows` rows, every value computed server-side."""
    selected = ",\n        ".join(
        f"{expression} as {name}" for name, expression in COLUMNS
    )
    return f"""
    create extension if not exists citext;
    create extension if not exists postgis;
    drop table if exists {TABLE} cascade;
    drop type if exists perf_status;
    create type perf_status as enum ('pending', 'active', 'closed');

    create table {TABLE} as
    select
        {selected}
    from generate_series(0, {rows - 1}) as i;
    """
