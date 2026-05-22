"""Python `Transfer` wrapper. Auto-coerces raw row iterables into `ArrowSource`."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from transferred._native import RunReport
from transferred._native import Transfer as _NativeTransfer


class Transfer:
    """Orchestrate a single source → destination run. Single-shot.

    Args:
        source: A `transferred` source (e.g. `ParquetSource`, `ArrowSource`) or a
            raw row iterable (auto-wrapped via `ArrowSource`). Iterables of
            `dict`, `@dataclass`, or `pydantic.BaseModel` are supported.
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

    def __init__(self, source: Any, destination: Any) -> None:
        if isinstance(source, Iterable):
            from transferred.iterable import iterable_to_arrow

            source = iterable_to_arrow(source)

        self._native = _NativeTransfer(source=source, destination=destination)

    def run(self) -> RunReport:
        """Execute the transfer. Single-shot."""
        return self._native.run()
