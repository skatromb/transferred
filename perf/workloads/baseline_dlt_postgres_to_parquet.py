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


def dump(out: Path) -> int:
    """Write `CAPPED` under `out` as Parquet. Returns rows written.

    Split out of `run` so the paired write leg reads a dump this backend wrote, with
    each range flattened into the four columns it turns one into.
    """
    from dlt.sources.sql_database import sql_table

    pipeline = _dlt.parquet_pipeline("dlt_pg_to_pq", out)
    pipeline.run(
        sql_table(_dlt.SQLALCHEMY_DSN, CAPPED, "public"), loader_file_format="parquet"
    )
    return _dlt.main_table_rows(out, CAPPED)


def run(out: Path) -> None:
    rows, wall_seconds = measure(lambda: dump(out))
    emit_result(
        rows=rows,
        output_bytes=file_bytes(_dlt.bucket(out)),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
