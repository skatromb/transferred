"""Refusals from the Rust extractor, reached past the Python dispatcher's own checks."""

from pathlib import Path

import pytest
from transferred import Destination, FilesDestination, Source, Transfer
from transferred.iterable import _iterable_to_arrow


class _UnwiredSource(Source):
    """Passes `isinstance(source, Source)` with no `_native_source` behind it."""


class _UnwiredDestination(Destination):
    """Passes `isinstance(destination, Destination)` with no native destination."""


def test_source_subclass_without_native(out_dir: Path):
    with pytest.raises(TypeError, match="source must be a transferred source object"):
        Transfer(source=_UnwiredSource(), destination=FilesDestination(out_dir))


def test_destination_subclass_without_native():
    with pytest.raises(
        TypeError, match="destination must be a transferred destination object"
    ):
        Transfer(source=[{"id": 1}], destination=_UnwiredDestination())


def test_source_reused_by_another_transfer(out_dir: Path):
    """The first `Transfer` takes the native source out of the wrapper."""
    source = _iterable_to_arrow([{"id": 1}])
    Transfer(source=source, destination=FilesDestination(out_dir))

    with pytest.raises(RuntimeError, match="already consumed by another Transfer"):
        Transfer(source=source, destination=FilesDestination(out_dir))
