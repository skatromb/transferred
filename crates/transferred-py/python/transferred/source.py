"""`Source` — base for all `transferred` sources."""

from typing import Any


class Source:
    """A `transferred` data source. Subclasses are passed to `Transfer(source=...)`."""

    _native_source: Any
