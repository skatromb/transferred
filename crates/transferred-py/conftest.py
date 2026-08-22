"""Doctest working directory, plus the output paths the tests share.

Doctests get `examples/small.parquet` as input in a temp cwd, so their output
writes stay out of the repo.
"""

import shutil
from pathlib import Path

import pytest

_EXAMPLES = Path(__file__).resolve().parent.parent.parent / "examples"


@pytest.fixture(autouse=True)
def _doctest_workdir(request, tmp_path, monkeypatch):
    if isinstance(request.node, pytest.DoctestItem):
        shutil.copy(_EXAMPLES / "small.parquet", tmp_path / "small.parquet")
        monkeypatch.chdir(tmp_path)


@pytest.fixture
def out(tmp_path: Path) -> Path:
    """Parquet file a single-file transfer writes to."""
    return tmp_path / "out.parquet"


@pytest.fixture
def out_dir(tmp_path: Path) -> Path:
    """Output directory a transfer writes its parts to."""
    return tmp_path / "out"
