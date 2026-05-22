"""Convert a Python iterable of rows into an `ArrowSource`.

Bridges Python-native data (`dict` / `@dataclass` / `pydantic.BaseModel`) to the
Arrow seam. Requires pyarrow — install via `pip install transferred[iterable]`.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable
from itertools import batched, chain
from typing import TYPE_CHECKING, Any

from transferred.arrow import ArrowSource

if TYPE_CHECKING:
    import pyarrow as pa

_BATCH_SIZE = 4096


def iterable_to_arrow(iterable: Iterable[Any]) -> ArrowSource:
    """Convert an iterable of dict / `@dataclass` / `pydantic.BaseModel` rows
    into an `ArrowSource`.

    Schema is inferred from the first batch; subsequent batches are built lazily
    as the source is consumed.

    Memory: prefer a generator over a materialized `list` for large inputs.

    Raises:
        ImportError: pyarrow not installed.
        ValueError: iterable is empty.
        TypeError: rows are none of dict / dataclass / pydantic.BaseModel.
    """
    return ArrowSource(_iterable_to_reader(iterable))


def _iterable_to_reader(iterable: Iterable[Any]) -> pa.RecordBatchReader:
    try:
        import pyarrow as pa
    except ImportError as e:
        raise ImportError(
            "iterable conversion requires `pyarrow`. "
            "Install with: `pip install transferred[iterable]`"
        ) from e

    iterator = iter(iterable)

    try:
        first_row = next(iterator)
    except StopIteration:
        raise ValueError("iterable is empty") from None

    dictifier = _validate_and_pick_converter(first_row)
    dictated = map(dictifier, chain([first_row], iterator))
    rows = batched(dictated, _BATCH_SIZE)

    first_batch = pa.RecordBatch.from_pylist(next(rows))
    schema = first_batch.schema

    remaining = (pa.RecordBatch.from_pylist(chunk, schema=schema) for chunk in rows)

    return pa.RecordBatchReader.from_batches(schema, chain([first_batch], remaining))


def _validate_and_pick_converter(row: Any) -> Callable[[Any], dict[str, Any]]:
    """Sniff `row`'s type once; return a `row` → `dict[str, Any]` converter.

    Supported row types: `dict`, `@dataclass` instance, `pydantic.BaseModel`
    (v1 + v2).

    Raises:
        TypeError: `row` is none of the supported types.
    """
    if isinstance(row, dict):
        return lambda r: r

    if dataclasses.is_dataclass(row):
        fields = dataclasses.fields(row)
        return lambda r, _f=fields: {f.name: getattr(r, f.name) for f in _f}

    if _is_pydantic_model(row):
        # v2: model_dump; v1: dict
        if hasattr(row, "model_dump"):
            return lambda r: r.model_dump()
        return lambda r: r.dict()

    raise TypeError(
        f"unsupported row type {type(row).__name__!r}. "
        "Supported: dict, dataclass, pydantic.BaseModel."
    )


def _is_pydantic_model(row: Any) -> bool:
    """True if `row` is a `pydantic.BaseModel` instance (v1 or v2)."""
    try:
        from pydantic import BaseModel
    except ImportError:
        return False

    return isinstance(row, BaseModel)
