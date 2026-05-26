"""Base classes for `transferred` sources and destinations."""

from typing import Any


class Source:
    """A `transferred` data source. Subclasses are passed to `Transfer(source=...)`."""

    _native_source: Any


class Destination:
    """A `transferred` data destination. Subclasses are passed to `Transfer(destination=...)`."""

    _native_destination: Any
