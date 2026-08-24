"""`_iterable_to_arrow` on its own — what it refuses and what it returns."""

import sys

import pytest
from transferred import ArrowSource, EmptySourceError
from transferred.iterable import _iterable_to_arrow


def test_empty_iterable_raises() -> None:
    """Same failure as a zero-batch Arrow reader, so the same exception."""
    with pytest.raises(EmptySourceError, match="empty"):
        _iterable_to_arrow([])


def test_tuple_rows_raise() -> None:
    """Tuples don't have column names."""
    with pytest.raises(TypeError, match="unsupported row type"):
        _iterable_to_arrow([(1, 2, 3)])  # ty: ignore[invalid-argument-type]


def test_returns_arrow_source() -> None:
    src = _iterable_to_arrow([{"id": 1}])
    assert isinstance(src, ArrowSource)


def test_missing_pyarrow_names_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """A None entry in `sys.modules` is how CPython spells "this import fails"."""
    monkeypatch.setitem(sys.modules, "pyarrow", None)

    with pytest.raises(ImportError, match=r"transferred\[iterable\]"):
        _iterable_to_arrow([{"id": 1}])
