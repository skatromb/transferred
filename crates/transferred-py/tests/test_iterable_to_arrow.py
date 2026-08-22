"""`_iterable_to_arrow` on its own — what it refuses and what it returns."""

import pytest
from transferred import ArrowSource
from transferred.iterable import _iterable_to_arrow


def test_empty_iterable_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        _iterable_to_arrow([])


def test_tuple_rows_raise() -> None:
    """Tuples don't have column names."""
    with pytest.raises(TypeError, match="unsupported row type"):
        _iterable_to_arrow([(1, 2, 3)])  # ty: ignore[invalid-argument-type]


def test_returns_arrow_source() -> None:
    src = _iterable_to_arrow([{"id": 1}])
    assert isinstance(src, ArrowSource)
