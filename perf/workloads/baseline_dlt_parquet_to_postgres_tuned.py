"""Baseline: Parquet → Postgres via dlt with every documented tuning applied.

`loader_file_format="csv"` is dlt's documented fast path — pyarrow converts to CSV
and psycopg streams it in with `COPY`. Its parquet/ADBC path is the alternative and
the worse trade: it declares the table with `int16`, `int32` and `float32` widened,
then ships the original file, which `COPY` rejects outright.

Reading the full fixture costs an `add_map`: dlt raises on Arrow extension types,
CSV cannot carry a struct, and `bytea` needs the hex literal. Unlike the read leg's
SQL cast this runs in Python per batch, so the cost is dlt's to bear and is measured.
"""

from __future__ import annotations

from pathlib import Path

from perf.data import BINARY_COLUMNS, ROWS_PER_GROUP
from perf.fixtures import SEED
from perf.postgres import row_count, table_bytes
from perf.workload import emit_result, measure, out_path
from perf.workloads import _dlt

NAME = "baseline dlt parquet→postgres (tuned)"
TARGET = "perf_load_dlt_tuned"


def run(out: Path) -> None:
    _dlt.reset(TARGET)
    _dlt.tune()
    from dlt.sources.filesystem import filesystem, read_parquet

    pipeline = _dlt.postgres_pipeline("dlt_pq_to_pg_tuned", out, _dlt.dsn())
    files = filesystem(bucket_url=SEED.parent.as_uri(), file_glob=SEED.name)
    rows = (files | read_parquet(chunksize=ROWS_PER_GROUP, use_pyarrow=True)).with_name(
        TARGET
    )
    rows.add_map(_dlt.to_loadable_arrow)
    # Hex text still has to land in a `bytea`, which only this hint arranges.
    rows.apply_hints(columns={name: {"data_type": "binary"} for name in BINARY_COLUMNS})

    _, wall_seconds = measure(lambda: pipeline.run(rows, loader_file_format="csv"))
    emit_result(
        rows=row_count(TARGET),
        output_bytes=table_bytes(TARGET),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
