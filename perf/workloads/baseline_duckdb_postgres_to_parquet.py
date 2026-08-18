"""Baseline: Postgres → Parquet via duckdb's `postgres_scanner`, no `transferred`.

A whole query engine doing the job in one statement, and the only baseline here
that reads in parallel — worth knowing what it costs in CPU and memory to match.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from perf.data import ROWS_PER_GROUP, TABLE
from perf.postgres import DSN
from perf.workload import emit_result, file_bytes, measure, out_path

NAME = "baseline duckdb postgres→parquet"


def dump(out: Path) -> int:
    """Write the wide table to `out` as one Parquet file. Returns rows written.

    Split out of `run` so the paired write leg reads a dump duckdb itself wrote —
    with the ranges as text, which is as much of them as it can carry.
    """
    con = duckdb.connect()
    con.execute(f"attach '{DSN}' as pg (type postgres, read_only)")
    con.execute(
        f"copy (select * from pg.public.{TABLE}) to '{out}' "
        f"(format parquet, compression zstd, row_group_size {ROWS_PER_GROUP})"
    )
    return pq.ParquetFile(out).metadata.num_rows


def run(out: Path) -> None:
    rows, wall_seconds = measure(lambda: dump(out))
    emit_result(
        rows=rows,
        output_bytes=file_bytes(out),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
