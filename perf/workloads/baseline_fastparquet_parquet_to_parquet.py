"""Baseline: Parquet → Parquet via fastparquet, no `transferred`.

fastparquet has no Arrow layer — it goes through a pandas `DataFrame`, so it
prices the round trip out of and back into Python's object model. It also has no
notion of Arrow extension types: struct columns arrive flattened and `numeric`
arrives as `float64`, which is why this baseline compares speed, not fidelity.
"""

from __future__ import annotations

from pathlib import Path

import fastparquet

from perf.workload import emit_result, file_bytes, measure, out_path
from perf.fixtures import SEED

NAME = "baseline fastparquet parquet→parquet"


def run(out: Path) -> None:
    def _transfer() -> int:
        frame = fastparquet.ParquetFile(SEED).to_pandas()
        fastparquet.write(str(out), frame, compression="ZSTD")
        return len(frame)

    rows, wall_seconds = measure(_transfer)
    emit_result(
        rows=rows,
        output_bytes=file_bytes(out),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run(out_path())
