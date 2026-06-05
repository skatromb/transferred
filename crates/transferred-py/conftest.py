"""Run doctests in a temp cwd seeded with `examples/small.parquet`.

Gives doctests their input fixture while keeping their output writes out of the repo.
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
