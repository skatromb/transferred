"""File formats for file-shaped sources and destinations."""

from typing import Literal, Self, cast

from transferred._native import _Parquet


class Parquet(_Parquet):
    """Parquet format. Carries encoder knobs; decoding needs none.

    Args:
        compression: `"zstd"` (default), `"snappy"`, or `None` (uncompressed).

    Example:
        >>> from transferred.formats import Parquet
        >>>
        >>> parquet_format = Parquet(compression="snappy")
    """

    __slots__ = ()

    def __new__(cls, compression: Literal["zstd", "snappy"] | None = "zstd") -> Self:
        return cast(Self, super().__new__(cls, compression))
