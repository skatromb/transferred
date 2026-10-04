"""`Transfer` over an iterable of rows — what it refuses before the run starts."""

import sys
from pathlib import Path

import pytest
from transferred import EmptySourceError, FilesDestination, Transfer


def test_empty_iterable_raises(out_dir: Path):
    """Same failure as a zero-batch Arrow reader, so the same exception."""
    with pytest.raises(EmptySourceError, match="empty"):
        Transfer(source=[], destination=FilesDestination(out_dir))


def test_tuple_rows_raise(out_dir: Path):
    """Tuples don't have column names."""
    with pytest.raises(TypeError, match="unsupported row type"):
        Transfer(
            source=[(1, 2, 3)],  # ty: ignore[invalid-argument-type]
            destination=FilesDestination(out_dir),
        )


def test_missing_pyarrow_names_the_extra(
    monkeypatch: pytest.MonkeyPatch, out_dir: Path
):
    """A None entry in `sys.modules` is how CPython spells "this import fails"."""
    monkeypatch.setitem(sys.modules, "pyarrow", None)

    with pytest.raises(ImportError, match=r"transferred\[iterable\]"):
        Transfer(source=[{"id": 1}], destination=FilesDestination(out_dir))
