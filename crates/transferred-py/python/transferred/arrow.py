"""`ArrowSource` — the Python-side Arrow seam into Rust.

Accepts a pyarrow `RecordBatchReader` and exposes it as a `transferred` source.
Requires pyarrow — install via `pip install transferred[arrow]`.
"""

from typing import TYPE_CHECKING

from transferred._base import Source
from transferred._native import _ArrowSource

if TYPE_CHECKING:
    import pyarrow as pa


class ArrowSource(Source):
    """Make a `transferred.Source` from a `pyarrow.RecordBatchReader`.

    Requires pyarrow — install via `pip install transferred[arrow]`.

    Raises:
        ImportError: pyarrow not installed. Install `transferred[arrow]`.
        TypeError: `reader` is not a `pyarrow.RecordBatchReader`.

    Example:
        >>> import pyarrow as pa
        >>> from transferred import ArrowSource, ParquetDestination, Transfer
        >>>
        >>> schema = pa.schema([("id", pa.int64())])
        >>> batch = pa.record_batch([pa.array([1, 2, 3])], schema=schema)
        >>> reader = pa.RecordBatchReader.from_batches(schema, [batch])
        >>>
        >>> report = Transfer(
        ...     source=ArrowSource(reader),
        ...     destination=ParquetDestination("out.parquet"),
        ... ).run()
        >>>
        >>> print(report)
        RunReport:
          rows:     3
          written:  519 B
          duration: 0s
    """

    _native_source: _ArrowSource

    def __init__(self, reader: pa.RecordBatchReader) -> None:
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError(
                "ArrowSource requires `pyarrow`. "
                "Install with: `pip install transferred[arrow]`"
            ) from e

        if not isinstance(reader, pa.RecordBatchReader):
            raise TypeError(
                f"reader must be a pyarrow.RecordBatchReader, "
                f"got {type(reader).__name__!r}"
            )

        self._native_source = _ArrowSource(reader)
