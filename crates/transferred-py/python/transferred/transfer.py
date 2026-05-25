"""Python `Transfer` subclass. Auto-coerces raw row iterables into `ArrowSource`."""

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
        source: A `transferred` source (e.g. `ParquetSource`, `ArrowSource`) or
            any iterable of `dict` / `@dataclass` / `pydantic.BaseModel` rows
            (auto-wrapped via `iterable_to_arrow`).
        destination: A `transferred` destination (e.g. `ParquetDestination`).

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
            from transferred.iterable import iterable_to_arrow

            source = iterable_to_arrow(source)
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
