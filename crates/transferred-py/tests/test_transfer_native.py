"""Refusals from the Rust extractor, reached past the Python dispatcher's own checks."""

from pathlib import Path

import pytest
from test_utils import write_seed
from transferred import Destination, FilesDestination, FilesSource, Source, Transfer


class _UnwiredSource(Source):
    """Passes `isinstance(source, Source)` with no `_native_source` behind it."""


class _UnwiredDestination(Destination):
    """Passes `isinstance(destination, Destination)` with no native destination."""


def test_source_subclass_without_native(out_dir: Path):
    with pytest.raises(TypeError, match=r"^`source` must be a `transferred\.Source`$"):
        Transfer(source=_UnwiredSource(), destination=FilesDestination(out_dir))


def test_destination_subclass_without_native():
    with pytest.raises(
        TypeError, match=r"^`destination` must be a `transferred\.Destination`$"
    ):
        Transfer(source=[{"id": 1}], destination=_UnwiredDestination())


def test_source_reused_by_another_transfer(tmp_path: Path, out_dir: Path):
    """The first `Transfer` takes the native source out of the wrapper."""
    seed = tmp_path / "seed.parquet"
    write_seed(seed, [1])
    source = FilesSource(seed)
    Transfer(source=source, destination=FilesDestination(out_dir))

    with pytest.raises(RuntimeError, match="already consumed by another Transfer"):
        Transfer(source=source, destination=FilesDestination(out_dir))
