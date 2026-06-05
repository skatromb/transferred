"""`FilesSource` and `FilesDestination` — local filesystem, any file format."""

import os

from transferred._base import Destination, Source
from transferred._native import _FilesDestination, _FilesSource
from transferred.formats import Format

StrPath = str | os.PathLike[str]


class FilesSource(Source):
    """Local file source. No I/O performed at construction.

    Accepts a single path, a glob pattern, or a list of paths.

    Args:
        path: One of:
            - Filesystem path to a single file (`str` or `os.PathLike`).
            - Glob pattern containing `*`, `?`, or `[...]` (e.g. `'data/*.parquet'`).
              Expanded at run time; matching zero files raises `SourceError`.
            - List of paths. Each item is treated literally (no per-item glob).
        format: File format codec. Defaults to `Parquet()` when omitted.

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

    _native_source: _FilesSource

    def __init__(
        self, path: StrPath | list[StrPath], format: Format | None = None
    ) -> None:
        native_format = None if format is None else format._native_format
        self._native_source = _FilesSource(path, native_format)


class FilesDestination(Destination):
    """Local directory destination. Writes atomically via tmp dir + rename.

    Written file paths are returned after `.run()` in `RunReport.written_objects`.

    Args:
        path: Output directory, replacing any existing one.
        format: Output format. Defaults to `Parquet()` when omitted.
        single_file: When `false`, outputs to many files,
            improving throughput from parallelization.
            When `true`, writes all partitions to one file.

    Example:
        >>> from transferred import FilesDestination
        >>> from transferred.formats import Parquet
        >>> destination = FilesDestination("out", format=Parquet(compression="zstd"))
    """

    _native_destination: _FilesDestination

    def __init__(
        self,
        path: StrPath,
        format: Format | None = None,
        single_file: bool = False,
    ) -> None:
        native_format = None if format is None else format._native_format
        self._native_destination = _FilesDestination(path, native_format, single_file)
