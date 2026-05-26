"""`Transfer` a source → destination."""

from collections.abc import Iterable
from typing import TYPE_CHECKING, Self

from transferred._native import Transfer as _NativeTransfer
from transferred.destination import Destination
from transferred.source import Source

if TYPE_CHECKING:
    from transferred.iterable import Row


class Transfer(_NativeTransfer):
    """Orchestrate a single source → destination run. Single-shot.

    Args:
        source: A `transferred.Source` or an iterable of
            `dict` / `@dataclass` / `pydantic.BaseModel` rows.
        destination: A `transferred.Destination`.

    Example:
        >>> from transferred import ParquetDestination, Transfer
        >>>
        >>> rows = ({"id": i, "name": f"row-{i}"} for i in range(1000))
        >>>
        >>> Transfer(
        ...     source=rows,
        ...     destination=ParquetDestination("out.parquet"),
        ... ).run()
    """

    def __new__(cls, source: Source | Iterable[Row], destination: Destination) -> Self:
        if isinstance(source, Source):
            pass
        elif isinstance(source, Iterable):
            from transferred.iterable import _iterable_to_arrow

            source = _iterable_to_arrow(source)
        else:
            raise TypeError(
                f"source must be a transferred source or an iterable of rows, "
                f"got {type(source).__name__!r}"
            )

        if not isinstance(destination, Destination):
            raise TypeError(
                f"destination must be a transferred destination, "
                f"got {type(destination).__name__!r}"
            )

        return super().__new__(cls, source, destination)
