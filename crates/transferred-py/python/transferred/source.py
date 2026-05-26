"""All `transferred` sources. Pass any to `Transfer(source=...)`."""

from transferred.arrow import ArrowSource
from transferred.parquet import ParquetSource

__all__ = ["ArrowSource", "ParquetSource"]
