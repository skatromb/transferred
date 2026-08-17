"""Baseline: Parquet → Postgres via dlt on its own defaults.

Nothing here is configured, which is the point. `read_parquet` defaults to
`use_pyarrow=False`, so dlt never inspects an Arrow schema and every type arrives
unaided — in 1000-row batches of Python dicts, rewritten by the JSON normalizer,
loaded as `insert_values` rather than `COPY`.

Reads the capped projection: at defaults this leg is slow enough that the full
fixture would outlast the rest of the suite. Compare it on `rows/s`.
"""

from __future__ import annotations

from pathlib import Path

from perf.data import CAPPED
from perf.fixtures import projection
from perf.postgres import row_count, table_bytes
from perf.workload import emit_result, measure, out_path
from perf.workloads import _dlt

NAME = "baseline dlt parquet→postgres (defaults, capped)"
TARGET = "perf_load_dlt"


def run(out: Path) -> None:
    _dlt.reset(TARGET)
    from dlt.sources.filesystem import filesystem, read_parquet

    pipeline = _dlt.postgres_pipeline("dlt_pq_to_pg", out, _dlt.dsn())
    seed = projection(CAPPED)
    files = filesystem(bucket_url=seed.parent.as_uri(), file_glob=seed.name)
    rows = (files | read_parquet()).with_name(TARGET)

    _, wall_seconds = measure(lambda: pipeline.run(rows))
    emit_result(
        rows=row_count(TARGET),
        output_bytes=table_bytes(TARGET),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
