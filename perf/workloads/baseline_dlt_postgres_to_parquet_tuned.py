"""Baseline: Postgres → Parquet via dlt with every documented tuning applied.

dlt's fastest read is `backend="connectorx"` — the only one on a binary wire
protocol, and per dlt's own docs "2x faster than the PyArrow backend". It panics
in Rust on ranges and PostGIS, so `query_adapter_callback` has the server cast
those to text; the cast is free at run time and no column is dropped.
"""

from __future__ import annotations

from pathlib import Path

from perf.data import ROWS_PER_GROUP, TABLE
from perf.workload import emit_result, file_bytes, measure, out_path
from perf.workloads import _dlt, _dlt_types

NAME = "baseline dlt postgres→parquet (tuned)"


def dump(out: Path) -> int:
    """Write the wide table under `out` as Parquet. Returns rows written.

    Split out of `run` so the paired write leg reads a dump dlt itself wrote, with
    the range and PostGIS columns as the text this read cast them to.
    """
    _dlt.tune()
    from dlt.sources.sql_database import sql_table

    pipeline = _dlt.parquet_pipeline("dlt_pg_to_pq_tuned", out)
    table = sql_table(
        _dlt.SQLALCHEMY_DSN,
        TABLE,
        "public",
        backend="connectorx",
        chunk_size=ROWS_PER_GROUP,
        query_adapter_callback=_dlt_types.cast_unmappable_to_text,
    )

    # `loader_file_format` is not optional: the filesystem destination prefers jsonl.
    pipeline.run(table, loader_file_format="parquet")
    return _dlt.main_table_rows(out, TABLE)


def run(out: Path) -> None:
    rows, wall_seconds = measure(lambda: dump(out))
    emit_result(
        rows=rows,
        output_bytes=file_bytes(_dlt.bucket(out)),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
