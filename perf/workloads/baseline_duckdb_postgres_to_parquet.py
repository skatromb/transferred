"""Baseline: Postgres → Parquet via duckdb's `postgres_scanner`, no `transferred`.

A whole query engine doing the job in one statement, and the only baseline here
that reads in parallel — worth knowing what it costs in CPU and memory to match.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from perf.data import TABLE
from perf.postgres import DSN
from perf.workload import emit_result, file_bytes, measure, out_path

NAME = "baseline duckdb postgres→parquet"


def run(out: Path) -> None:
    def _transfer() -> None:
        con = duckdb.connect()
        con.execute(f"attach '{DSN}' as pg (type postgres, read_only)")
        con.execute(
            f"copy (select * from pg.public.{TABLE}) to '{out}' "
            f"(format parquet, compression zstd)"
        )

    _, wall_seconds = measure(_transfer)
    emit_result(
        rows=pq.ParquetFile(out).metadata.num_rows,
        output_bytes=file_bytes(out),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
