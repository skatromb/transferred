"""Synthetic row + batch generators shared across perf workloads.

Single source of truth for row shape (i64 + f64 + str), row count
(via `PERF_ROWS` env, default 4M), and seed-file layout.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = int(os.environ.get("PERF_ROWS", "4_000_000").replace("_", ""))
"""Total rows generated per workload run. Override via `PERF_ROWS=N`."""

ROWS_PER_GROUP = 1_000_000
"""Row-group size used for seed writing and matched baselines.

Matches parquet-rs's `DEFAULT_MAX_ROW_GROUP_ROW_COUNT = 1024*1024`. pyarrow's
`iter_batches` default of 65536 would produce ~16x more (smaller) row groups,
which compress worse with zstd and confound output-size comparisons.
"""


def make_batch(start: int, n: int) -> pa.RecordBatch:
    """Build an n-row batch starting at `start`. Schema: i64 / f64 / str."""
    return pa.RecordBatch.from_pydict(
        {
            "i64": pa.array(range(start, start + n), type=pa.int64()),
            "f64": pa.array(
                [i * 1.5 for i in range(start, start + n)], type=pa.float64()
            ),
            "str": pa.array(
                [f"row-{i}" for i in range(start, start + n)], type=pa.string()
            ),
        }
    )


def iter_dict_rows() -> Iterator[dict[str, Any]]:
    """Yield `ROWS` dict rows with the same shape as `make_batch`."""
    for i in range(ROWS):
        yield {"i64": i, "f64": i * 1.5, "str": f"row-{i}"}


def write_seed_parquet(seed: Path) -> None:
    """Write `ROWS` rows in `ROWS_PER_GROUP`-row groups to `seed` (zstd)."""
    schema = make_batch(0, 1).schema
    with pq.ParquetWriter(seed, schema, compression="zstd") as writer:
        for start in range(0, ROWS, ROWS_PER_GROUP):
            writer.write_batch(make_batch(start, min(ROWS_PER_GROUP, ROWS - start)))
