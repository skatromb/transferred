"""Python `Transfer` subclass. Auto-coerces raw row iterables into `ArrowSource`."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Self

from transferred._native import Transfer as _NativeTransfer


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

    def __new__(cls, source: Any, destination: Any) -> Self:
        if isinstance(source, Iterable):
            from transferred.iterable import iterable_to_arrow

            source = iterable_to_arrow(source)
        return super().__new__(cls, source, destination)
