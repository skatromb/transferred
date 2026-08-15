"""Baseline: Postgres → Parquet via the ADBC Postgres driver, no `transferred`.

The fairest read-leg baseline there is — ADBC runs the same binary COPY under a C
driver and hands back Arrow, so what is left is `transferred`'s own seam.
"""

from __future__ import annotations

from pathlib import Path

import adbc_driver_postgresql.dbapi as adbc
import pyarrow.parquet as pq

from perf.data import TABLE
from perf.postgres import DSN
from perf.workload import emit_result, file_bytes, measure, out_path

NAME = "baseline adbc postgres→parquet"


def run(out: Path) -> None:
    def _transfer() -> int:
        with adbc.connect(DSN) as conn, conn.cursor() as cur:
            cur.execute(f"select * from {TABLE}")
            reader = cur.fetch_record_batch()
            rows = 0
            with pq.ParquetWriter(out, reader.schema, compression="zstd") as writer:
                for batch in reader:
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
