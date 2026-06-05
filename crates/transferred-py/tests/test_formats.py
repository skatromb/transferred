from typing import Literal, get_args, get_origin, get_type_hints

import pytest
from transferred.formats import Parquet

_hint = get_type_hints(Parquet.__init__)["compression"]
annotated_compressions = next(a for a in get_args(_hint) if get_origin(a) is Literal)


def test_every_compression_literal_is_accepted():
    for c in get_args(annotated_compressions):
        Parquet(compression=c)  # raises ValueError if Rust rejects


def test_none_compression_is_accepted():
    Parquet(compression=None)


def test_unknown_compression_raises():
    with pytest.raises(ValueError):
        Parquet(compression="lz4")  # ty: ignore[invalid-argument-type]
