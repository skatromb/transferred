"""`Transfer` a source → destination."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Protocol, Self, runtime_checkable

from transferred._base import Destination, Source
from transferred._native import _ArrowSource, _Transfer
from transferred.iterable import _iterable_to_reader

if TYPE_CHECKING:
    import pydantic
    from _typeshed import DataclassInstance


type SourceLike = Source | DataFrame | Iterable[Row]
"""Anything `Transfer(source=...)` accepts."""

type Row = dict[str, Any] | DataclassInstance | pydantic.BaseModel
"""A single input row: `dict`, `@dataclass` instance, or `pydantic.BaseModel`."""


class Transfer(_Transfer):
    """Orchestrate a single source → destination run. Single-shot.

    Args:
        source: One of:
            - a `transferred.Source`,
            - an iterable of `dict` / `@dataclass` / `pydantic.BaseModel` rows,
            - a polars or pandas `DataFrame`,
            - a `pyarrow.Table`,
            - a duckdb result.

            Prefer a generator for data larger than RAM.
        destination: A `transferred.Destination`.

    Example:
        >>> from transferred import FilesDestination, Transfer
        >>>
        >>> rows = ({"id": i, "name": f"row-{i}"} for i in range(1000))
        >>> destination = FilesDestination("output_directory")
        >>>
        >>> report = Transfer(
        ...     source=rows,
        ...     destination=destination,
        ... ).run()
        >>>
        >>> print(report)
        RunReport:
          rows: 1,000
          written: 5.18 KiB
          duration: ...
          written objects:
            output_directory/part-00001.parquet
    """

    def __new__(cls, source: SourceLike, destination: Destination) -> Self:
        coerced_source = _coerce_source(source)

        if not isinstance(destination, Destination):
            raise TypeError(
                f"`destination` must be a `transferred.Destination`, "
                f"got `{type(destination).__name__}`"
            )

        return super().__new__(cls, coerced_source, destination)


def _coerce_source(source: SourceLike) -> Source:
    """Normalise anything `Transfer(source=...)` accepts into a `Source`."""
    if isinstance(source, Source):
        return source

    if isinstance(source, DataFrame):
        return _DataFrameSource(source)

    if isinstance(source, Iterable):
        arrow_batch_reader = _iterable_to_reader(source)
        return _DataFrameSource(arrow_batch_reader)

    raise TypeError(
        f"`source` must be a `transferred.Source`, a `DataFrame` or an iterable of rows, "
        f"got `{type(source).__name__}`"
    )


class _DataFrameSource(Source):
    """A `Source` over a `DataFrame`, built by `Transfer` so users never construct it."""

    _native_source: _ArrowSource

    def __init__(self, dataframe: DataFrame) -> None:
        self._native_source = _ArrowSource(dataframe)


@runtime_checkable
class DataFrame(Protocol):
    """A pandas or polars `DataFrame`, `pa.Table`, duckdb result — anything with `__arrow_c_stream__`."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...
