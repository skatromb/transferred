"""Convert a Python iterable of rows into an `ArrowSource`.

Bridges Python-native data (`dict` / `@dataclass` / `pydantic.BaseModel`) to the
Arrow seam. Requires pyarrow — install via `pip install transferred[iterable]`.
"""

import dataclasses
from collections.abc import Callable, Iterable, Iterator
from itertools import batched, chain
from typing import TYPE_CHECKING, Any

from transferred.arrow import ArrowSource

if TYPE_CHECKING:
    import pyarrow as pa
    from _typeshed import DataclassInstance
    from pydantic import BaseModel

    type Row = dict[str, Any] | DataclassInstance | BaseModel
    """A single input row: `dict`, `@dataclass` instance, or `pydantic.BaseModel`."""

_BATCH_SIZE = 4096


def _iterable_to_arrow(iterable: Iterable[Row]) -> ArrowSource:
    """Wrap an iterable of dict / dataclass / pydantic rows as an `ArrowSource`.

    Raises:
        ImportError: pyarrow not installed.
        ValueError: iterable is empty.
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

    chunks = batched(_to_dicts(iterable), _BATCH_SIZE)
    first = pa.RecordBatch.from_pylist(next(chunks))
    rest = (pa.RecordBatch.from_pylist(chunk, schema=first.schema) for chunk in chunks)

    return pa.RecordBatchReader.from_batches(first.schema, chain([first], rest))


def _to_dicts(iterable: Iterable[Row]) -> Iterator[dict[str, Any]]:
    """Normalise rows to dicts, sniffing the row type once off the first row."""
    iterator = iter(iterable)

    try:
        first_row = next(iterator)
    except StopIteration:
        raise ValueError("iterable is empty") from None

    convert = _converter_for(first_row)

    return map(convert, chain([first_row], iterator))


def _converter_for(row: Any) -> Callable[[Any], dict[str, Any]]:
    """Return a `row` → `dict[str, Any]` converter for `row`'s type.

    Supported row types: `dict`, `@dataclass` instance, `pydantic.BaseModel`
    (v1 + v2).

    Raises:
        TypeError: `row` is none of the supported types.
    """
    if isinstance(row, dict):
        return lambda mapping: mapping

    if dataclasses.is_dataclass(row):
        field_names = [field.name for field in dataclasses.fields(row)]
        return lambda instance: {name: getattr(instance, name) for name in field_names}

    if _is_pydantic_model(row):
        # v2: model_dump; v1: dict
        if hasattr(row, "model_dump"):
            return lambda model: model.model_dump()
        return lambda model: model.dict()

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
