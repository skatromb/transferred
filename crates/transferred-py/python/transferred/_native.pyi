from _typeshed import Incomplete
from collections.abc import Sequence
from os import PathLike
from typing import Any, final

class ArrowError(TransferredError):
    """
    Arrow schema or array conversion failed.
    """
    def __new__(cls, /, *_args) -> ArrowError: ...

class Destination:
    """
    A `transferred` data destination. Subclasses are passed to `Transfer(destination=...)`.
    """

class DestinationError(TransferredError):
    """
    Destination write failed (permission denied, disk full, schema mismatch).
    """
    def __new__(cls, /, *_args) -> DestinationError: ...

class EmptySourceError(SourceError):
    """
    Source produced no batches — nothing to transfer.
    """
    def __new__(cls, /, *_args) -> EmptySourceError: ...

class Format:
    """
    A file format codec. Pass to `FilesSource`/`FilesDestination(format=...)`.
    """

class IoError(TransferredError):
    """
    Filesystem I/O error not attributable to source or destination logic.
    """
    def __new__(cls, /, *_args) -> IoError: ...

@final
class RunReport:
    """
    Post-run statistics returned by `Transfer.run()`.
    
    Attributes:
        `rows`: Total rows written.
        `bytes_written`: Total bytes written to the destination.
        `written_objects`: Identifiers of what was written (paths, URIs, tables).
        `duration_seconds`: Wall-clock duration of the transfer, in seconds.
    
    Example:
        ```py
        >>> report = Transfer(source=..., destination=...).run()
        >>> print(report)
        RunReport:
          rows: 12,481,902
          written: 1.40 GiB
          duration: 4s 218ms
          written objects:
            out/part-00001.parquet
            out/part-00002.parquet
        ```
    """
    def __repr__(self, /) -> str: ...
    def __str__(self, /) -> str: ...
    @property
    def bytes_written(self, /) -> int:
        """
        Total bytes written to the destination.
        """
    @property
    def duration_seconds(self, /) -> float:
        """
        Wall-clock duration of the transfer, in seconds.
        """
    @property
    def rows(self, /) -> int:
        """
        Total rows written.
        """
    @property
    def written_objects(self, /) -> list[str]:
        """
        Identifiers of what the destination wrote (file paths, URIs, tables).
        """

class Source:
    """
    A `transferred` data source. Subclasses are passed to `Transfer(source=...)`.
    """

class SourceError(TransferredError):
    """
    Source read failed (file missing, malformed Parquet, etc.).
    """
    def __new__(cls, /, *_args) -> SourceError: ...

class TransferredError(Exception):
    """
    Base exception for all `transferred` failures.
    
    Subclasses: `SourceError` (and `EmptySourceError`), `DestinationError`, `ArrowError`, `IoError`.
    
    Example:
        ```py
        >>> from transferred import Transfer, TransferredError
        >>> try:
        ...     Transfer(source=..., destination=...).run()
        ... except TransferredError as e:
        ...     print(f"transfer failed: {e}")
        ```
    """
    def __new__(cls, /, *_args) -> TransferredError: ...

class _ArrowSource(Source):
    """
    Internal `PyO3` wrapper around an Arrow C stream, built by `Transfer` from a `DataFrame` or rows.
    """
    def __new__(cls, /, reader: Any) -> _ArrowSource: ...

class _FilesDestination(Destination):
    """
    Internal `PyO3` wrapper around `transferred_files::FilesDestination`.
    """
    def __new__(cls, /, path: str |PathLike[str], format: Format, single_file: bool = False) -> _FilesDestination: ...

class _FilesSource(Source):
    """
    Internal `PyO3` wrapper around `transferred_files::FilesSource`.
    """
    def __new__(cls, /, path: Sequence[str |PathLike[str]] |str |PathLike[str], format: Format) -> _FilesSource: ...

class _Parquet(Format):
    """
    Internal `PyO3` wrapper around `transferred_files::Parquet`.
    """
    def __new__(cls, /, compression: str |None) -> _Parquet: ...

class _PostgresDestination(Destination):
    """
    Internal `PyO3` wrapper around `transferred_postgres::PostgresDestination`.
    """
    def __new__(cls, /, dsn: str, table: str) -> _PostgresDestination: ...

class _PostgresSource(Source):
    """
    Internal `PyO3` wrapper around `transferred_postgres::PostgresSource`.
    """
    def __new__(cls, /, dsn: str, table: str) -> _PostgresSource: ...

class _Transfer:
    """
    Internal `PyO3` wrapper around `transferred_core::Transfer`. Subclassed by the
    user-facing Python `Transfer`; not used directly.
    """
    def __new__(cls, /, source: Source, destination: Destination) -> _Transfer: ...
    def run(self, /) -> RunReport: ...

def __getattr__(name: str) -> Incomplete: ...
