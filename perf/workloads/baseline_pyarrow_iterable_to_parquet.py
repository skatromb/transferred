"""Baseline: generator-of-dicts → Parquet via raw pyarrow, no `transferred`.

Shadows `iterable_generator_to_parquet`. Buffers rows into `ROWS_PER_GROUP`-sized
chunks, converts each to a RecordBatch, writes one row group per chunk.
"""

from __future__ import annotations

from itertools import batched
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from perf.data import PYTHON_ROWS, ROWS_PER_GROUP, iter_dict_rows
from perf.workload import emit_result, file_bytes, measure, out_path

NAME = "baseline pyarrow iterable→parquet"


def run(out: Path) -> None:
    def _transfer() -> None:
        chunks = (
            pa.RecordBatch.from_pylist(list(c))
            for c in batched(iter_dict_rows(), ROWS_PER_GROUP)
        )
        first = next(chunks)
        with pq.ParquetWriter(out, first.schema, compression="zstd") as writer:
            writer.write_batch(first)
            for chunk in chunks:
                writer.write_batch(chunk)

    _, wall_seconds = measure(_transfer)
    emit_result(
        rows=PYTHON_ROWS,
        output_bytes=file_bytes(out),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
