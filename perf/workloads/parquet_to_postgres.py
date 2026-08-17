"""Parquet → Postgres via `transferred`, loading the shared wide fixture.

Measures the whole destination contract, not just the wire: rows land in a
staging table and swap into place in one transaction.
"""

from __future__ import annotations

from perf.fixtures import SEED
from perf.postgres import DSN, table_bytes
from perf.workload import emit_result, measure
from transferred import FilesSource, PostgresDestination, Transfer

NAME = "parquet→postgres"
TARGET = "perf_load"


def run() -> None:
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
    run()
