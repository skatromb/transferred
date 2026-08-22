"""`ArrowSource` — the Python-side Arrow seam into Rust.

Accepts any object exposing the Arrow PyCapsule interface and hands its stream to Rust.
"""

from typing import Protocol, runtime_checkable

from transferred._base import Source
from transferred._native import _ArrowSource


@runtime_checkable
class ArrowStream(Protocol):
    """Anything implementing Arrow PyCapsule interface: DataFrame, Arrow.Table, BatchReader."""

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...


class ArrowSource(Source):
    """Make a `transferred.Source` from Arrow data.

    Takes anything implementing the [Arrow PyCapsule interface][1] — a `pyarrow.Table`,
    `RecordBatch` or `RecordBatchReader`, a `polars.DataFrame`, a duckdb result. A table
    is already whole in memory; a reader streams, so prefer one for data larger than RAM.

    [1]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html

    Args:
        arrow_stream: Arrow data exposing `__arrow_c_stream__`.

    Raises:
        TypeError: `arrow_stream` does not implement the Arrow PyCapsule interface.

    Example:
        >>> import pyarrow as pa
        >>> from transferred import ArrowSource, FilesDestination, Transfer
        >>>
        >>> table = pa.table({"id": [1, 2, 3]})
        >>>
        >>> report = Transfer(
        ...     source=ArrowSource(table),
        ...     destination=FilesDestination("output_directory"),
        ... ).run()
        >>>
        >>> print(report)
        RunReport:
          rows: 3
          written: 519 B
          duration: ...
          written objects:
            output_directory/part-00001.parquet
    """

    _native_source: _ArrowSource

    def __init__(self, arrow_stream: ArrowStream) -> None:
        if not isinstance(arrow_stream, ArrowStream):
            raise TypeError(
                f"{type(arrow_stream).__name__!r} does not implement the Arrow `PyCapsule` "
                "interface — pass a pyarrow `Table`, `RecordBatch` or `RecordBatchReader`, "
                "or any object exposing `__arrow_c_stream__`"
            )

        self._native_source = _ArrowSource(arrow_stream)
