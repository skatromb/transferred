"""The dataset every perf workload shares.

One wide Postgres table is the single source of truth for schema and scale — the
Parquet fixtures are dumped from it, so no two generators can drift apart. Its
columns cover what `transferred-postgres` maps: ints, floats, decimals, strings,
temporals, uuid, json, ranges and PostGIS geometry.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

ROWS = int(os.environ.get("PERF_ROWS", "50_000_000").replace("_", ""))
"""Rows in the shared wide dataset. Override via `PERF_ROWS=N`."""

PYTHON_ROWS = int(os.environ.get("PERF_PYTHON_ROWS", "4_000_000").replace("_", ""))
"""Rows for the iterable workloads, which build every row in Python.

An order of magnitude below `ROWS` on purpose: the row converter runs at Python
speed, so matching `ROWS` would make these workloads the whole suite's wall clock.
"""

ROWS_PER_GROUP = 1_000_000
"""Row-group size used for the Parquet fixtures and matched baselines.

Matches parquet-rs's `DEFAULT_MAX_ROW_GROUP_ROW_COUNT = 1024*1024`. pyarrow's
`iter_batches` default of 65536 would produce ~16x more (smaller) row groups,
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

RANGE_COLUMNS = ("valid_days", "span")
"""Columns Arrow models as `struct`. No engine but ours writes them to Postgres."""

POSTGIS_COLUMNS = ("location", "region")
"""Columns holding PostGIS types, which most drivers know nothing about."""

EXTENSION_COLUMNS = ("session_id", "attrs")
"""Columns Arrow models as canonical extension types — `arrow.uuid` and `arrow.json`."""

BINARY_COLUMNS = ("payload",)
"""Columns holding `bytea`, which no text-protocol `COPY` can carry as-is."""

CAST_TO_TEXT = RANGE_COLUMNS + POSTGIS_COLUMNS
"""Columns dlt reads only once Postgres has cast them to text.

connectorx panics in Rust on each of these before dlt sees a row, and no dlt
setting reaches that far. Casting in the query is the documented way out, and it
costs nothing at run time because the server does the work.
"""

UNSUPPORTED: dict[str, tuple[str, ...]] = {
    "adbc": RANGE_COLUMNS,
    "duckdb": RANGE_COLUMNS,
}
"""Columns a baseline cannot carry at all, keyed by the system that cannot carry them.

Both entries fall over the same wall from opposite sides: ADBC has no mapping from
an Arrow struct to a Postgres type, and duckdb refuses to create a column of the
unnamed composite type its own STRUCT would need. Neither offers a hook to supply
one. dlt used to be listed here, until every one of its gaps turned out to have a
documented workaround — see `CAST_TO_TEXT` and `perf.workloads._dlt`.

Two systems sharing a list today is a coincidence worth keeping separate: the
views are named after the system that reads them, so a list can move on its own.
"""

CAPPED = f"{TABLE}_capped"
"""`TABLE` capped at `SLOW_ROWS`, for baselines that move every row through Python."""


def view(system: str) -> str:
    """Name of the view prepared for `system`, holding every column it can carry."""
    return f"{TABLE}_{system}"


def views_sql() -> str:
    """SQL creating `CAPPED` plus one view per system in `UNSUPPORTED`.

    `CAPPED` exists for baselines that move every row through Python at tens of
    microseconds each: at `ROWS` one would outlast the rest of the suite combined.
    Capping keeps them in the run and comparable on `rows/s`, the trade the
    iterable workloads already make.

    Views are dropped rather than replaced: `create or replace view` refuses to
    change a column list, and these lists move as a baseline turns out to reject
    one more type.
    """
    statements = [
        f"drop view if exists {CAPPED};"
        f"create view {CAPPED} as select * from {TABLE} limit {SLOW_ROWS};"
    ]
    for system, dropped in UNSUPPORTED.items():
        kept = ", ".join(name for name, _ in COLUMNS if name not in dropped)
        statements.append(
            f"drop view if exists {view(system)} cascade;"
            f"create view {view(system)} as select {kept} from {TABLE};"
        )
    return "".join(statements)


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


def iter_dict_rows() -> Iterator[dict[str, Any]]:
    """Yield `PYTHON_ROWS` narrow dict rows, the shape the row converter sees."""
    for i in range(PYTHON_ROWS):
        yield {"i64": i, "f64": i * 1.5, "str": f"row-{i}"}
