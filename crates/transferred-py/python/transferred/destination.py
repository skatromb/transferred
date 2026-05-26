"""`Destination` — base for all `transferred` destinations."""

from typing import Any


class Destination:
    """A `transferred` data destination. Subclasses are passed to `Transfer(destination=...)`."""

    _native_destination: Any
