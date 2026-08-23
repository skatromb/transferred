"""Baseline: Parquet → Postgres via dlt on its own defaults.

Nothing here is configured, which is the point. `read_parquet` defaults to
`use_pyarrow=False`, so dlt never inspects an Arrow schema and every type arrives
unaided — in 1000-row batches of Python dicts, rewritten by the JSON normalizer,
loaded as `insert_values` rather than `COPY`.

Loads back its own read leg's dump, which is the capped table: at defaults this leg
is slow enough that the full one would outlast the rest of the suite. Compare it on
`rows/s`.
"""

from __future__ import annotations

from pathlib import Path

from perf import baseline_dumps
from perf.data import CAPPED, SLOW_ROW_NUM
from perf.postgres import row_count, table_bytes
from perf.workload import emit_result, measure, out_path
from perf.workloads import _dlt
from perf.workloads import baseline_dlt_postgres_to_parquet as read_leg

NAME = "baseline dlt parquet→postgres (defaults, capped)"
TARGET = "perf_load_dlt"


def prepare() -> Path:
    """The dump this leg loads, written by dlt's own default read leg unless cached."""
    return _dlt.main_table(
        baseline_dumps.ensure("dlt", read_leg.dump, SLOW_ROW_NUM), CAPPED
    )


def run(out: Path) -> None:
    source = prepare()
    _dlt.reset(TARGET)
    from dlt.sources.filesystem import filesystem, read_parquet

    pipeline = _dlt.postgres_pipeline("dlt_pq_to_pg", out, _dlt.SQLALCHEMY_DSN)
    files = filesystem(bucket_url=source.as_uri(), file_glob="*.parquet")
    rows = (files | read_parquet()).with_name(TARGET)

    _, wall_seconds = measure(lambda: pipeline.run(rows))
    emit_result(
        row_num=row_count(TARGET),
        output_bytes=table_bytes(TARGET),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
