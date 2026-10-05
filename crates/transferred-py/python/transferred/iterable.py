"""Convert a Python iterable of rows into an `ArrowSource`.

Bridges Python-native data (`dict` / `@dataclass` / `pydantic.BaseModel`) to the
Arrow seam. Requires pyarrow — install via `pip install transferred[iterable]`.
"""

import dataclasses
from collections.abc import Callable, Iterable
from itertools import batched, chain
from typing import TYPE_CHECKING, Any

from transferred._native import EmptySourceError
from transferred.arrow import ArrowSource

if TYPE_CHECKING:
    import pyarrow as pa

    from transferred.transfer import Row

_BATCH_SIZE = 4096


def _iterable_to_arrow(iterable: Iterable[Row]) -> ArrowSource:
    """Wrap an iterable of dict / dataclass / pydantic rows as an `ArrowSource`.

    Raises:
        ImportError: pyarrow not installed.
        EmptySourceError: iterable is empty.
        TypeError: rows are none of dict / dataclass / pydantic.BaseModel.
    """
    return ArrowSource(_iterable_to_reader(iterable))


def _iterable_to_reader(iterable: Iterable[Row]) -> pa.RecordBatchReader:
    try:
        import pyarrow as pa
    except ImportError as error:
        raise ImportError(
            "iterable conversion requires `pyarrow`. "
            "Install with: `pip install transferred[iterable]`"
        ) from error

    chunks = batched(iterable, _BATCH_SIZE)
    first_chunk = next(chunks, None)
    if first_chunk is None:
        raise EmptySourceError("iterable is empty")

    convert = _converter_for(first_chunk[0])
    first = pa.RecordBatch.from_pylist(list(map(convert, first_chunk)))
    rest = (
        pa.RecordBatch.from_pylist(list(map(convert, chunk)), schema=first.schema)
        for chunk in chunks
    )

    return pa.RecordBatchReader.from_batches(first.schema, chain([first], rest))


def _converter_for(row: Row) -> Callable[[Any], dict[str, Any]]:
    """Return a `row` → `dict[str, Any]` converter for `row`'s type.

    Supported row types: `dict`, `@dataclass` instance, `pydantic.BaseModel`.

    Raises:
        TypeError: `row` is none of the supported types.
    """
    if isinstance(row, dict):
        return lambda mapping: mapping

    if dataclasses.is_dataclass(row):
        field_names = [field.name for field in dataclasses.fields(row)]
        return lambda instance: {name: getattr(instance, name) for name in field_names}

    if hasattr(row, "model_dump"):  # pydantic model
        return lambda model: model.model_dump()

    raise TypeError(
        f"unsupported row type {type(row).__name__!r}. "
        "Supported: dict, dataclass, pydantic.BaseModel."
    )
