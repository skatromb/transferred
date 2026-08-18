"""Parquet → Postgres via `transferred`, loading back what our own read leg wrote.

Measures the whole destination contract, not just the wire: rows land in a staging
table and swap into place in one transaction.

The input is the shared seed, which our own read leg produced — so the ranges arrive
as the `transferred.pg_range` structs they left as, and land as ranges again. Each
baseline round-trips its own dump for the same reason; see `perf.dumps`.
"""

from __future__ import annotations

from pathlib import Path

from perf.fixtures import SEED
from perf.postgres import DSN, table_bytes
from perf.workload import emit_result, measure, out_path
from transferred import FilesSource, PostgresDestination, Transfer

NAME = "transferred parquet→postgres"
TARGET = "perf_load"


def run(_out: Path) -> None:
    report, wall_seconds = measure(
        lambda: Transfer(
            source=FilesSource(str(SEED)),
            destination=PostgresDestination(DSN, table=TARGET),
        ).run()
    )
    emit_result(
        rows=report.rows,
        output_bytes=table_bytes(TARGET),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
