"""Rewrites the tuned dlt baselines apply so dlt can carry the wide table's types.

Every one of them works around a gap that raises rather than degrades, and none of
them is reachable through a dlt setting.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

import pyarrow as pa

from perf.data import CAST_TO_TEXT


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
    """Rewrite `batch` into the Arrow types dlt's Postgres destination accepts."""
    for index, field in enumerate(batch.schema):
        column = _loadable_column(batch.column(index), field)
        if column.type != field.type:
            replacement = pa.field(field.name, column.type, field.nullable)
            batch = batch.set_column(index, replacement, column)
    return batch


def _loadable_column(column: Any, field: Any) -> Any:
    r"""One column in a type dlt's Postgres destination accepts.

    Three separate gaps:

    - canonical extension types (`arrow.uuid`, `arrow.json`) are rejected outright,
      so each is unwrapped to its storage type;
    - a `struct` cannot reach Postgres over CSV, so ranges become JSON text;
    - `bytea` needs the `\x` hex literal `COPY` expects, while the column keeps its
      `binary` hint so the destination still creates a `bytea`.
    """
    # `BaseExtensionType`, not `ExtensionType`: canonical types like `pa.uuid()`
    # subclass only the former, so the narrower check silently matches nothing.
    if isinstance(field.type, pa.BaseExtensionType):
        column = column.cast(field.type.storage_type)

    if pa.types.is_fixed_size_binary(column.type):
        return _as_text(column, lambda cell: str(UUID(bytes=cell)))
    if pa.types.is_struct(column.type):
        return _as_text(column, lambda cell: json.dumps(cell, default=str))
    if pa.types.is_binary(column.type) or pa.types.is_large_binary(column.type):
        return _as_text(column, lambda cell: rf"\x{cell.hex()}")
    return column


def _as_text(column: Any, spell: Callable[[Any], str]) -> Any:
    """`column` as strings, `spell` applied cell by cell."""
    return pa.array([spell(cell) for cell in column.to_pylist()])
