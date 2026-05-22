"""`ArrowSource` — the Python-side Arrow seam into Rust.

Accepts a pyarrow `RecordBatchReader` and exposes it as a `transferred` source.
Requires pyarrow — install via `pip install transferred[arrow]`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from transferred._native import _ArrowSource

if TYPE_CHECKING:
    import pyarrow as pa


class ArrowSource:
    """Wrap a pyarrow `RecordBatchReader` as a `transferred` source.

    For Python-native iterables (`dict` / `@dataclass` / `pydantic.BaseModel`),
    use `transferred.iterable.iterable_to_arrow` or pass the iterable straight
    to `Transfer(source=...)` and let auto-coercion handle the wrap.

    Raises:
        ImportError: pyarrow not installed. Install `transferred[arrow]`.
        TypeError: `reader` is not a `pyarrow.RecordBatchReader`.

    Example:
        >>> import pyarrow as pa
        >>> from transferred import ArrowSource, ParquetDestination, Transfer
        >>>
        >>> reader = pa.RecordBatchReader.from_batches(...)
        >>>
        >>> Transfer(
        ...     source=ArrowSource(reader),
        ...     destination=ParquetDestination("out.parquet"),
        ... ).run()
    """

    _native_source: _ArrowSource

    def __init__(self, reader: pa.RecordBatchReader) -> None:
        self._native_source = _ArrowSource(reader)
