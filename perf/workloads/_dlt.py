"""Shared scaffolding for the dlt baselines.

dlt keeps pipeline state under `~/.dlt/pipelines` by default, which would carry
over between repeats and across runs and make a second run cheaper than a first.
Every pipeline here gets a throwaway working directory under the harness's output
path, so each repeat starts from nothing.
"""

from __future__ import annotations

import os
from json import dumps
from pathlib import Path
from typing import Any
from uuid import UUID

import dlt
import pyarrow.parquet as pq
from dlt.destinations import filesystem

from perf.data import CAST_TO_TEXT, ROWS_PER_GROUP
from perf.postgres import DSN, drop_table

DATASET = "perf"

TUNING: dict[str, str] = {
    # Without file rotation a single table is one file, and `load.workers` idles.
    "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS": str(ROWS_PER_GROUP),
    # Also the Parquet row-group size, which pyarrow takes from the batch it is given.
    "DATA_WRITER__BUFFER_MAX_ITEMS": str(ROWS_PER_GROUP),
    # Matches the fixtures. dlt defaults to snappy, which is the faster codec, so
    # leaving it would hand dlt a wall-clock edge and charge it for the file size.
    "DATA_WRITER__COMPRESSION": "zstd",
    # Skips a round trip to the destination for pipeline state on every run.
    "RESTORE_FROM_DESTINATION": "false",
}
"""Documented performance settings, applied only by the tuned baselines.

From https://dlthub.com/docs/reference/performance. Left at dlt's defaults in the
untuned baselines, which is the point of measuring both.
"""


def cast_unmappable_to_text(query: Any, table: Any) -> Any:
    """Rewrite `query` so Postgres casts to text what dlt cannot read natively.

    Passed as `query_adapter_callback`. connectorx panics in Rust on range and
    PostGIS columns before dlt sees a row, so the only reachable fix is in the SQL
    — and it is free, since the server does the casting.
    """
    import sqlalchemy as sa

    columns = [
        sa.cast(column, sa.Text).label(column.name)
        if column.name in CAST_TO_TEXT
        else column
        for column in table.columns
    ]
    return query.with_only_columns(*columns)


def to_loadable_arrow(batch: Any) -> Any:
    """Rewrite `batch` into the Arrow types dlt's Postgres destination accepts.

    Three separate gaps, all of which raise rather than degrade, and none of which
    any dlt setting covers:

    - canonical extension types (`arrow.uuid`, `arrow.json`) are rejected outright,
      so each is unwrapped to its storage type;
    - a `struct` cannot reach Postgres over CSV, so ranges become JSON text;
    - `bytea` needs the `\\x` hex literal `COPY` expects, while the column keeps its
      `binary` hint so the destination still creates a `bytea`.
    """
    import pyarrow as pa

    for index, field in enumerate(batch.schema):
        column = batch.column(index)
        # `BaseExtensionType`, not `ExtensionType`: canonical types like `pa.uuid()`
        # subclass only the former, so the narrower check silently matches nothing.
        if isinstance(field.type, pa.BaseExtensionType):
            column = column.cast(field.type.storage_type)
        if pa.types.is_fixed_size_binary(column.type):
            column = pa.array([str(UUID(bytes=v)) for v in column.to_pylist()])
        elif pa.types.is_struct(column.type):
            column = pa.array([dumps(v, default=str) for v in column.to_pylist()])
        elif pa.types.is_binary(column.type) or pa.types.is_large_binary(column.type):
            column = pa.array([Rf"\x{v.hex()}" for v in column.to_pylist()])
        if column.type != field.type:
            replacement = pa.field(field.name, column.type, field.nullable)
            batch = batch.set_column(index, replacement, column)
    return batch


def reset(target: str) -> None:
    """Drop `target` and dlt's bookkeeping, so a repeated run starts from nothing.

    dlt records which schema versions it has deployed in `_dlt_version` and skips
    the DDL when it finds a table already listed there — including after the harness
    dropped that table between repeats, leaving `COPY` to fail on a missing relation.
    """
    for table in (target, "_dlt_version", "_dlt_loads", "_dlt_pipeline_state"):
        drop_table(table)


def main_table_rows(bucket: Path, table: str) -> int:
    """Rows dlt wrote for `table` itself, ignoring child tables it split nested data into.

    The JSON normalizer turns a nested jsonb value into a child table, so counting
    every Parquet file under `bucket` would count rows the source never had.
    """
    files = (bucket / DATASET / table).rglob("*.parquet")
    return sum(pq.ParquetFile(path).metadata.num_rows for path in files)


def dsn() -> str:
    """The shared DSN under the scheme SQLAlchemy accepts.

    SQLAlchemy dropped the `postgres://` alias, and dlt builds an engine for schema
    reflection even when the read itself goes through connectorx.
    """
    return DSN.replace("postgres://", "postgresql://", 1)


def tune() -> None:
    """Apply `TUNING` to this workload's environment, dlt's highest-priority provider."""
    os.environ.update(TUNING)


def parquet_pipeline(name: str, out: Path) -> tuple[dlt.Pipeline, Path]:
    """Build a Parquet-writing pipeline under `out`. Returns it and its output directory."""
    bucket = out / "data"
    return (
        dlt.pipeline(
            name,
            destination=filesystem(str(bucket)),
            dataset_name=DATASET,
            pipelines_dir=str(out / "pipelines"),
        ),
        bucket,
    )


def postgres_pipeline(name: str, out: Path, dsn: str) -> dlt.Pipeline:
    """Build a Postgres-writing pipeline whose working directory lives under `out`."""
    return dlt.pipeline(
        name,
        destination=dlt.destinations.postgres(dsn),
        dataset_name="public",
        pipelines_dir=str(out / "pipelines"),
    )
