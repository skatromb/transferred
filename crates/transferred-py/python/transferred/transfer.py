"""`Transfer` a source → destination."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Self

from transferred._base import Destination, Source
from transferred._native import _Transfer
from transferred.arrow import ArrowSource, ArrowStream

if TYPE_CHECKING:
    import pydantic
    from _typeshed import DataclassInstance


type SourceLike = (
    Source
    | ArrowStream
    | Iterable[dict[str, Any] | DataclassInstance | pydantic.BaseModel]
)
"""Anything `Transfer(source=...)` accepts."""


class Transfer(_Transfer):
    """Orchestrate a single source → destination run. Single-shot.

    Args:
        source: Any of the `transferred.Source`,
            an iterable of `dict` / `@dataclass` / `pydantic.BaseModel` rows,
            a polars or pandas `DataFrame`,
            a `pa.Table` / `RecordBatch` / `RecordBatchReader`,
            a duckdb result.
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
        source = _coerce_source(source)

        if not isinstance(destination, Destination):
            raise TypeError(
                f"destination must be a transferred destination, "
                f"got {type(destination).__name__!r}"
            )

        return super().__new__(cls, source, destination)


def _coerce_source(source: SourceLike) -> Source:
    """Normalise anything `Transfer(source=...)` accepts into a `Source`."""
    if isinstance(source, Source):
        return source

    if isinstance(source, ArrowStream):
        return ArrowSource(source)

    if isinstance(source, Iterable):
        from transferred.iterable import _iterable_to_arrow

        return _iterable_to_arrow(source)

    raise TypeError(
        f"source must be a transferred source, Arrow data or an iterable of rows, "
        f"got {type(source).__name__!r}"
    )
