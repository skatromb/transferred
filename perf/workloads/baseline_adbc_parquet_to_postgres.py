"""Baseline: Parquet → Postgres via ADBC's `adbc_ingest`, no `transferred`.

Shadows `parquet_to_postgres`, reading the projection ADBC can manage: it has no
mapping from an Arrow struct to a Postgres type, so it cannot ingest the range
columns and refuses the full fixture. It therefore moves slightly less data than
the workload it shadows — the point of comparison is the wire, not the coverage.

`mode="replace"` drops and recreates the target, the closest ADBC comes to the
staging-table swap `PostgresDestination` performs.
"""

from __future__ import annotations

import adbc_driver_postgresql.dbapi as adbc
import pyarrow as pa
import pyarrow.parquet as pq

from perf.data import ROWS_PER_GROUP, view
from perf.fixtures import projection
from perf.postgres import DSN, table_bytes
from perf.workload import emit_result, measure

NAME = "baseline adbc parquet→postgres (adbc projection)"
TARGET = "perf_load_adbc"


def run() -> None:
    def _transfer() -> int:
        seed = pq.ParquetFile(projection(view("adbc")))
        # A reader, not the raw iterator: `adbc_ingest` consumes either, but only
        # this form matches its signature, and it carries the schema along.
        batches = pa.RecordBatchReader.from_batches(
            seed.schema_arrow, seed.iter_batches(batch_size=ROWS_PER_GROUP)
        )
        with adbc.connect(DSN) as conn, conn.cursor() as cur:
            rows = cur.adbc_ingest(TARGET, batches, mode="replace")
            # ADBC does not autocommit; without this the ingest rolls back on exit
            # and the table never appears, while `adbc_ingest` still reports success.
            conn.commit()
            return rows

    rows, wall_seconds = measure(_transfer)
    emit_result(
        rows=rows,
        output_bytes=table_bytes(TARGET),
        wall_seconds=wall_seconds,
    )


if __name__ == "__main__":
    run()
