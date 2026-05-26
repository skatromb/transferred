"""Run doctests with cwd = repo's `examples/` (gives access to `small.parquet`)."""

from pathlib import Path

import pytest

_EXAMPLES = Path(__file__).resolve().parent.parent.parent / "examples"


@pytest.fixture(autouse=True)
def _doctest_workdir(request, monkeypatch):
    if isinstance(request.node, pytest.DoctestItem):
        monkeypatch.chdir(_EXAMPLES)
