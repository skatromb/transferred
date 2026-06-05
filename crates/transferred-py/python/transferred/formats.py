"""File formats for file-shaped sources and destinations."""

from typing import Any

from transferred._native import _Parquet


class Format:
    """A file format codec. Pass to `FilesSource`/`FilesDestination(format=...)`."""

    _native_format: Any


class Parquet(Format):
    """Parquet format. Carries encoder knobs; decoding needs none.

    Args:
        compression: One of `"zstd"` (default), `"snappy"`, `"uncompressed"`.

    Example:
        >>> from transferred.formats import Parquet
        >>> fmt = Parquet(compression="snappy")
    """

    def __init__(self, compression: str = "zstd") -> None:
        self._native_format = _Parquet(compression)
