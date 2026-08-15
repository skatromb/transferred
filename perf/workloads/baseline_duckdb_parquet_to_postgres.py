"""Baseline: Parquet → Postgres via duckdb's `postgres_scanner`, no `transferred`.

Shadows `parquet_to_postgres`, and the mirror of `baseline_duckdb_postgres_to_parquet`
— the same one-statement attach, with the `read_only` dropped so the attached
database is a destination.

It reads the projection duckdb can manage: creating a column for its own STRUCT
needs a named composite type in Postgres, which duckdb will not invent, so the
range columns are out of reach. Everything else lands with its type intact, down
to `uuid` and `bytea`.

`create or replace table` is the closest duckdb comes to the staging-table swap
`PostgresDestination` performs.
"""

from __future__ import annotations

import duckdb

from perf.data import view
from perf.fixtures import projection
from perf.postgres import DSN, row_count, table_bytes
from perf.workload import emit_result, measure

NAME = "baseline duckdb parquet→postgres (duckdb projection)"
TARGET = "perf_load_duckdb"


def run() -> None:
    def _transfer() -> None:
        source = projection(view("duckdb"))
        con = duckdb.connect()
        con.execute(f"attach '{DSN}' as pg (type postgres)")
        con.execute(
            f"create or replace table pg.public.{TARGET} as "
            f"select * from read_parquet('{source}')"
        )

    _, wall_seconds = measure(_transfer)
    emit_result(
        rows=row_count(TARGET),
        output_bytes=table_bytes(TARGET),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run()
