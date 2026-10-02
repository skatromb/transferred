"""The exception stubs are written by hand, so their docstrings must match the runtime ones."""

import ast
import inspect
from pathlib import Path

import pytest
from transferred import (
    ArrowError,
    DestinationError,
    EmptySourceError,
    IoError,
    SourceError,
    TransferredError,
)

_STUB = (
    Path(__file__).resolve().parent.parent
    / "python"
    / "transferred"
    / "_native"
    / "__init__.pyi"
)


def _stub_docstrings() -> dict[str, str | None]:
    classes = ast.parse(_STUB.read_text()).body
    return {
        node.name: ast.get_docstring(node)
        for node in classes
        if isinstance(node, ast.ClassDef)
    }


@pytest.mark.parametrize(
    "exception",
    [
        TransferredError,
        SourceError,
        EmptySourceError,
        DestinationError,
        ArrowError,
        IoError,
    ],
)
def test_stub_docstring_matches_runtime(exception: type[Exception]):
    assert _stub_docstrings()[exception.__name__] == inspect.getdoc(exception)
