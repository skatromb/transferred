"""Shared scaffolding for the dlt baselines.

dlt keeps pipeline state under `~/.dlt/pipelines` by default, which would carry
over between repeats and across runs and make a second run cheaper than a first.
Every pipeline here gets a throwaway working directory under the harness's output
path, so each repeat starts from nothing.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import MappingProxyType

import dlt
from dlt.destinations import filesystem
from pyarrow import parquet as pq

from perf.data import ROWS_PER_GROUP
from perf.postgres import DSN, drop_table

DATASET = "perf"

SQLALCHEMY_DSN = DSN.replace("postgres://", "postgresql://", 1)
"""The shared DSN under the scheme SQLAlchemy accepts.

SQLAlchemy dropped the `postgres://` alias, and dlt builds an engine for schema
reflection even when the read itself goes through connectorx.
"""

TUNING = MappingProxyType(
    {
        # Without file rotation a single table is one file, and `load.workers` idles.
        "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS": str(ROWS_PER_GROUP),
        # Also the Parquet row-group size, which pyarrow takes from the batch it is given.
        "DATA_WRITER__BUFFER_MAX_ITEMS": str(ROWS_PER_GROUP),
        # Matches the fixtures; dlt's snappy default would trade file size for wall time.
        "DATA_WRITER__COMPRESSION": "zstd",
        # Skips a round trip to the destination for pipeline state on every run.
        "RESTORE_FROM_DESTINATION": "false",
    }
)
"""Documented performance settings, applied only by the tuned baselines.

From https://dlthub.com/docs/reference/performance. Left at dlt's defaults in the
untuned baselines, which is the point of measuring both.
"""


def reset(target: str) -> None:
    """Drop `target` and dlt's bookkeeping, so a repeated run starts from nothing.

    dlt records which schema versions it has deployed in `_dlt_version` and skips
    the DDL when it finds a table already listed there — including after the harness
    dropped that table between repeats, leaving `COPY` to fail on a missing relation.
    """
    for table in (target, "_dlt_version", "_dlt_loads", "_dlt_pipeline_state"):
        drop_table(table)


def main_table(out: Path, table: str) -> Path:
    """Directory holding the Parquet files dlt wrote for `table` itself.

    The JSON normalizer turns a nested jsonb value into a child table of its own, so
    the whole dataset directory holds more tables than the source had.
    """
    return bucket(out) / DATASET / table


def main_table_rows(out: Path, table: str) -> int:
    """Rows dlt wrote for `table` itself, ignoring the child tables beside it."""
    files = main_table(out, table).rglob("*.parquet")
    return sum(pq.ParquetFile(path).metadata.num_rows for path in files)


def tune() -> None:
    """Apply `TUNING` to this workload's environment, dlt's highest-priority provider."""
    os.environ.update(TUNING)


def bucket(out: Path) -> Path:
    """Where a Parquet pipeline rooted at `out` writes its data files."""
    return out / "data"


def parquet_pipeline(name: str, out: Path) -> dlt.Pipeline:
    """Build a Parquet-writing pipeline rooted at `out`."""
    return dlt.pipeline(
        name,
        destination=filesystem(str(bucket(out))),
        dataset_name=DATASET,
        pipelines_dir=str(out / "pipelines"),
    )


def postgres_pipeline(name: str, out: Path, dsn: str) -> dlt.Pipeline:
    """Build a Postgres-writing pipeline whose working directory lives under `out`."""
    return dlt.pipeline(
        name,
        destination=dlt.destinations.postgres(dsn),
        dataset_name="public",
        pipelines_dir=str(out / "pipelines"),
    )
