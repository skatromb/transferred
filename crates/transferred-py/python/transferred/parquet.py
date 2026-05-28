"""`ParquetSource` and `ParquetDestination` — local Parquet."""

import os

from transferred._base import Destination, Source
from transferred._native import _ParquetDestination, _ParquetSource

StrPath = str | os.PathLike[str]


class ParquetSource(Source):
    """Local Parquet source. No I/O performed at construction.

    Accepts a single path, a glob pattern, or a list of paths.

    Args:
        path: One of:
            - Filesystem path to a single `.parquet` file (`str` or `os.PathLike`).
            - Glob pattern containing `*`, `?`, or `[...]` (e.g. `'data/*.parquet'`).
              Expanded at run time; matching zero files raises `SourceError`.
            - List of paths. Each item is treated literally (no per-item glob).

    Example:
        >>> from transferred import ParquetSource, ParquetDestination, Transfer
        >>>
        >>> # Use glob
        >>> source = ParquetSource("partitions/*.parquet")
        >>>
        >>> # Or pass list of files explicitly
        >>> source = ParquetSource(["first.parquet", "second.parquet"])
        >>>
        >>> # Or point to a single file
        >>> source = ParquetSource("small.parquet")
        >>>
        >>> report = Transfer(
        ...     source=source,
        ...     destination=ParquetDestination("out.parquet"),
        ... ).run()
    """

    _native_source: _ParquetSource

    def __init__(self, path: StrPath | list[StrPath]) -> None:
        self._native_source = _ParquetSource(path)


class ParquetDestination(Destination):
    """Local single-file Parquet destination. Writes via tmp file + atomic rename.

    Args:
        path: Filesystem path to the output `.parquet` file.
        compression: One of `"zstd"` (default), `"snappy"`, `"uncompressed"`.

    Example:
        >>> from transferred import ParquetDestination
        >>> destination = ParquetDestination("out.parquet", compression="zstd")
    """

    _native_destination: _ParquetDestination

    def __init__(self, path: StrPath, compression: str = "zstd") -> None:
        self._native_destination = _ParquetDestination(path, compression)
