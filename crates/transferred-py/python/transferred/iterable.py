"""Python-native iterable source. Wraps any iterable of dict/dataclass/pydantic rows
and exposes it as a `transferred` source.

Requires pyarrow at runtime — install via `pip install transferred[iterable]`.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable
from itertools import batched, chain
from typing import Any

from transferred._native import _RecordBatchReaderSource

_BATCH_SIZE = 4096


class PyIterableSource:
    """Wrap any Python iterable of rows as a `transferred` source.

    Accepts iterables of `dict`, `@dataclass` instances, or `pydantic.BaseModel`
    instances. Schema is inferred from the first batch.

    Args:
        iterable: Any iterable yielding `dict`, dataclass, or `pydantic.BaseModel` rows.

    Raises:
        ImportError: pyarrow not installed. Install `transferred[iterable]`.
        ValueError: iterable is empty.
        TypeError: rows are none of dict / dataclass / pydantic.

    Example:
        >>> from transferred import ParquetDestination, Transfer, PyIterableSource
        >>> rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        >>> Transfer(
        ...     source=PyIterableSource(rows),
        ...     destination=ParquetDestination("out.parquet"),
        ... ).run()
    """

    _native_source: _RecordBatchReaderSource

    def __init__(self, iterable: Iterable[Any]) -> None:
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError(
                "`PyIterableSource` requires `pyarrow`. "
                "Install with: `pip install transferred[iterable]`"
            ) from e

        iterator = iter(iterable)

        try:
            first_row = next(iterator)
        except StopIteration:
            raise ValueError("`PyIterableSource`: iterable is empty") from None

        dictifier = _pick_converter(first_row)
        dictated = map(dictifier, chain([first_row], iterator))
        rows = batched(dictated, _BATCH_SIZE)

        first_batch = pa.RecordBatch.from_pylist(next(rows))
        schema = first_batch.schema

        remaining = (pa.RecordBatch.from_pylist(chunk, schema=schema) for chunk in rows)

        reader = pa.RecordBatchReader.from_batches(
            schema, chain([first_batch], remaining)
        )

        self._native_source = _RecordBatchReaderSource(reader)


def _pick_converter(row: Any) -> Callable[[Any], dict[str, Any]]:
    """Sniff first row type once; return a `row` → `dict` converter."""
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
        f"PyIterableSource: unsupported row type {type(row).__name__!r}. "
        "Supported: dict, dataclass, pydantic.BaseModel."
    )


def _is_pydantic_model(row: Any) -> bool:
    """True if `row` is a `pydantic.BaseModel` instance (v1 or v2)."""
    try:
        from pydantic import BaseModel
    except ImportError:
        return False

    return isinstance(row, BaseModel)
