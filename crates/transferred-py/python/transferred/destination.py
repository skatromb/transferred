"""`Destination` — union of all `transferred` destination types."""

from typing import TypeAlias

from transferred._native import ParquetDestination

Destination: TypeAlias = ParquetDestination
"""Any `transferred` destination accepted by `Transfer(destination=...)`."""
