"""Refusals from the native `Transfer`."""

from pathlib import Path

import pytest
from test_utils import write_seed
from transferred import FilesDestination, FilesSource, Transfer


def test_source_reused_by_another_transfer(tmp_path: Path, out_dir: Path):
    """The first `Transfer` takes the native source out of the wrapper."""
    seed = tmp_path / "seed.parquet"
    write_seed(seed, [1])
    source = FilesSource(seed)
    Transfer(source=source, destination=FilesDestination(out_dir))

    with pytest.raises(RuntimeError, match="already consumed by another Transfer"):
        Transfer(source=source, destination=FilesDestination(out_dir))
