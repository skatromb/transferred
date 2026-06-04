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
        row_group_size: Max rows per row group. `None` (default) keeps the
            parquet-rs default (1,048,576). The writer buffers one row group in
            memory before flushing, so this is the write-side memory lever.

    Example:
        >>> from transferred.formats import Parquet
        >>> fmt = Parquet(compression="snappy", row_group_size=100_000)
    """

    def __init__(
        self, compression: str = "zstd", row_group_size: int | None = None
    ) -> None:
        self._native_format = _Parquet(compression, row_group_size)
