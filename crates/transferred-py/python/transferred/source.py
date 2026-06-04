"""All `transferred` sources. Pass any to `Transfer(source=...)`."""

from transferred.arrow import ArrowSource
from transferred.files import FilesSource

__all__ = ["ArrowSource", "FilesSource"]
