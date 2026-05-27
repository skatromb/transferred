"""Parquet → Parquet, single seed file, single output file.

Standalone script — invoked as a subprocess by `perf.harness`. Two subcommands:
    setup <seed_path>       — writes seed Parquet (not measured)
    run <seed_path> <out>   — runs Transfer, emits JSON result on stdout
"""

from __future__ import annotations

import gc
import json
import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from transferred import ParquetDestination, ParquetSource, Transfer

NAME = "parquet→parquet (single)"
# Schema: i64 + f64 + variable-length string. Override row count with PERF_ROWS=N.
ROWS = int(os.environ.get("PERF_ROWS", "4_000_000").replace("_", ""))
SEED_BATCH = 1_000_000


def _batch(start: int, n: int) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(
        {
            "i64": pa.array(range(start, start + n), type=pa.int64()),
            "f64": pa.array([i * 1.5 for i in range(start, start + n)], type=pa.float64()),
            "str": pa.array([f"row-{i}" for i in range(start, start + n)], type=pa.string()),
        }
    )


def setup(seed: Path) -> None:
    schema = _batch(0, 1).schema
    with pq.ParquetWriter(seed, schema, compression="zstd") as writer:
        for start in range(0, ROWS, SEED_BATCH):
            writer.write_batch(_batch(start, min(SEED_BATCH, ROWS - start)))


def run(seed: Path, out: Path) -> None:
    gc.collect()
    arrow_before = pa.total_allocated_bytes()
    wall_start = time.monotonic()
    Transfer(
        source=ParquetSource(seed),
        destination=ParquetDestination(out, compression="zstd"),
    ).run()
    wall_seconds = time.monotonic() - wall_start
    arrow_after = pa.total_allocated_bytes()

    result = {
        "rows": ROWS,
        "output_bytes": out.stat().st_size,
        "wall_seconds": wall_seconds,
        "peak_arrow_bytes": max(arrow_before, arrow_after),
    }
    json.dump(result, sys.stdout)


def main(argv: list[str]) -> None:
    cmd = argv[1]
    if cmd == "setup":
        setup(Path(argv[2]))
    elif cmd == "run":
        run(Path(argv[2]), Path(argv[3]))
    else:
        raise SystemExit(f"unknown subcommand: {cmd!r}")


if __name__ == "__main__":
    main(sys.argv)
