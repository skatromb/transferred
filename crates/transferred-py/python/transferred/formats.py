"""File formats for file-shaped sources and destinations."""

from typing import Any, Literal

from transferred._native import _Parquet


class Format:
    """A file format codec. Pass to `FilesSource`/`FilesDestination(format=...)`."""

    _native_format: Any

    def __setattr__(self, name: str, new_value: object) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")


class Parquet(Format):
    """Parquet format. Carries encoder knobs; decoding needs none.

    Args:
        compression: `"zstd"` (default), `"snappy"`, or `None` (uncompressed).

    Example:
        >>> from transferred.formats import Parquet
        >>>
        >>> parquet_format = Parquet(compression="snappy")
    """

    def __init__(self, compression: Literal["zstd", "snappy"] | None = "zstd") -> None:
        object.__setattr__(self, "_native_format", _Parquet(compression))
