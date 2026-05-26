"""`ParquetSource` and `ParquetDestination` — local single-file Parquet."""

from pathlib import Path

from transferred._base import Destination, Source
from transferred._native import _ParquetDestination, _ParquetSource


class ParquetSource(Source):
    """Local single-file Parquet source. No I/O performed at construction.

    Args:
        path: Filesystem path to the input `.parquet` file.

    Example:
        >>> from transferred import ParquetSource, ParquetDestination, Transfer
        >>> Transfer(
        ...     source=ParquetSource("in.parquet"),
        ...     destination=ParquetDestination("out.parquet"),
        ... ).run()
    """

    _native_source: _ParquetSource

    def __init__(self, path: str | Path) -> None:
        self._native_source = _ParquetSource(path)


class ParquetDestination(Destination):
    """Local single-file Parquet destination. Writes via tmp file + atomic rename.

    Args:
        path: Filesystem path to the output `.parquet` file.
        compression: One of `"zstd"` (default), `"snappy"`, `"uncompressed"`.

    Example:
        >>> ParquetDestination("out.parquet", compression="zstd")
    """

    _native_destination: _ParquetDestination

    def __init__(self, path: str | Path, compression: str = "zstd") -> None:
        self._native_destination = _ParquetDestination(path, compression)
