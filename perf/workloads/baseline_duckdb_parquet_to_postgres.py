"""Baseline: Parquet → Postgres via duckdb's `postgres_scanner`, no `transferred`.

The mirror of `baseline_duckdb_postgres_to_parquet` — the same one-statement attach
with `read_only` dropped, loading back that leg's own dump. Round-tripping its own
file is what makes the two legs comparable: duckdb cannot read our range structs,
and its dump carries the ranges as the text it read them into.

`create or replace table` is the closest duckdb comes to the staging-table swap
`PostgresDestination` performs.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from perf import dumps
from perf.data import ROWS
from perf.postgres import DSN, row_count, table_bytes
from perf.workload import emit_result, measure, out_path
from perf.workloads import baseline_duckdb_postgres_to_parquet as read_leg

NAME = "baseline duckdb parquet→postgres"
TARGET = "perf_load_duckdb"


def prepare() -> Path:
    """The dump this leg loads, written by duckdb's own read leg unless already cached."""
    return dumps.ensure("duckdb", read_leg.dump, ROWS)


def run(_out: Path) -> None:
    source = prepare()

    def _transfer() -> None:
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
    run(out_path())
