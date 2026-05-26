"""`Transfer` a source → destination."""

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Self

from transferred._base import Destination, Source
from transferred._native import Transfer as _NativeTransfer

if TYPE_CHECKING:
    import pydantic
    from _typeshed import DataclassInstance


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
        >>> destination = ParquetDestination("out.parquet")
        >>>
        >>> Transfer(
        ...     source=rows,
        ...     destination=destination,
        ... ).run()
    """

    def __new__(
        cls,
        source: Source
        | Iterable[dict[str, Any] | DataclassInstance | pydantic.BaseModel],
        destination: Destination,
    ) -> Self:
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
