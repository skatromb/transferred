"""`FilesSource` and `FilesDestination` — local filesystem, any file format."""

import os
from typing import Self, cast

from transferred._native import Format, _FilesDestination, _FilesSource
from transferred.formats import Parquet

StrPath = str | os.PathLike[str]

_PARQUET = Parquet()
"""Default `format=` below. One shared instance; `Format` forbids mutation."""


class FilesSource(_FilesSource):
    """Local file source. No I/O performed at construction.

    Accepts a single path, a glob pattern, or a list of paths.

    Args:
        path: One of:
            - Filesystem path to a single file (`str` or `os.PathLike`).
            - List of paths.
            - Glob pattern containing `*`, `?`, or `[...]` (e.g. `'data/*.parquet'`).
              Expanded at run time; matching zero files raises `SourceError`.
        format: File format codec. Defaults to `Parquet()`.

    Example:
        >>> from transferred import FilesSource, FilesDestination, Transfer
        >>>
        >>> # Use glob
        >>> source = FilesSource("partitions/*.parquet")
        >>>
        >>> # Or pass list of files explicitly
        >>> source = FilesSource(["first.parquet", "second.parquet"])
        >>>
        >>> # Or point to a single file
        >>> source = FilesSource("small.parquet")
        >>>
        >>> report = Transfer(
        ...     source=source,
        ...     destination=FilesDestination("out"),
        ... ).run()
    """

    def __new__(cls, path: StrPath | list[StrPath], format: Format = _PARQUET) -> Self:
        return cast(Self, super().__new__(cls, path, format))


class FilesDestination(_FilesDestination):
    """Local directory destination. Writes atomically via tmp dir + rename.

    Written file paths are returned after `.run()` in `RunReport.written_objects`.

    Args:
        path: Output directory, replacing any existing one.
        format: Output format. Defaults to `Parquet()`.
        single_file: When `False`, outputs to many files,
            improving throughput from parallelization.
            When `True`, writes all partitions to one file.

    Example:
        >>> from transferred import FilesSource, FilesDestination, Transfer
        >>> from transferred.formats import Parquet
        >>>
        >>> destination = FilesDestination("out", format=Parquet(compression="zstd"))
        >>>
        >>> report = Transfer(
        ...     source=FilesSource("small.parquet"),
        ...     destination=destination,
        ... ).run()
    """

    def __new__(
        cls,
        path: StrPath,
        format: Format = _PARQUET,
        *,
        single_file: bool = False,
    ) -> Self:
        return cast(Self, super().__new__(cls, path, format, single_file))
