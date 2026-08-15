"""Baseline: Parquet → Parquet via raw pyarrow, no `transferred`.

Shadows `parquet_to_parquet_single` to isolate the cost of `transferred`'s
seam from the underlying arrow/parquet libs.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from perf.data import ROWS_PER_GROUP
from perf.fixtures import SEED
from perf.workload import emit_result, file_bytes, measure, out_path

NAME = "baseline pyarrow parquet→parquet"


def run(out: Path) -> None:
    def _transfer() -> int:
        reader = pq.ParquetFile(SEED)
        rows = 0
        with pq.ParquetWriter(out, reader.schema_arrow, compression="zstd") as writer:
            for batch in reader.iter_batches(batch_size=ROWS_PER_GROUP):
                rows += batch.num_rows
                writer.write_batch(batch)
        return rows

    rows, wall_seconds = measure(_transfer)
    emit_result(
        rows=rows,
        output_bytes=file_bytes(out),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
