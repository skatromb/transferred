"""`Source` — union of all `transferred` source types."""

from typing import TypeAlias

from transferred._native import ParquetSource
from transferred.arrow import ArrowSource

Source: TypeAlias = ParquetSource | ArrowSource
"""Any `transferred` source accepted by `Transfer(source=...)`."""
