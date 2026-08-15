"""Baseline: Postgres → Parquet via dlt on its own defaults.

The `sqlalchemy` backend dlt picks by default yields dicts row by row — its docs
call it "the most robust … but also the slowest". It needs no adapter for any of
our types, at the price of flattening each range into four columns.

One default is overridden: `loader_file_format="parquet"`. The filesystem
destination otherwise writes gzipped JSONL, and then there is nothing to compare.
Reads `CAPPED`, since at tens of microseconds a row the full table would outlast
the rest of the suite. Compare it on `rows/s`.
"""

from __future__ import annotations

from pathlib import Path

from perf.data import CAPPED
from perf.workload import emit_result, file_bytes, measure, out_path
from perf.workloads import _dlt

NAME = "baseline dlt postgres→parquet (defaults, capped)"


def run(out: Path) -> None:
    from dlt.sources.sql_database import sql_table

    pipeline, bucket = _dlt.parquet_pipeline("dlt_pg_to_pq", out)
    table = sql_table(_dlt.dsn(), CAPPED, "public")

    _, wall_seconds = measure(lambda: pipeline.run(table, loader_file_format="parquet"))
    emit_result(
        rows=_dlt.main_table_rows(bucket, CAPPED),
        output_bytes=file_bytes(bucket),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
