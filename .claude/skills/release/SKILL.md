---
name: release
description: |
  Make a release of `transferred` (Rust crates + Python wheel).
  Triggers when user asks to release or ship a new version; PyPI or crates.io.
---

# release — cut a `transferred` version

Ships Rust crates (`transferred-core`, `transferred-files`, `transferred-postgres`, `transferred-py`) to crates.io and the `transferred` wheel to PyPI. CI publishes; this skill is the human pre/post work.

Use `make` targets where they exist. If a step has no target, do it by hand or extend the Makefile.

## Preconditions

- All scope items for the version in `PLAN.md` are `[x]` except `Deploy 0.0.x`
- Every public source/destination/format added or changed this version has an example in `examples/`
- `make pre-release` passes locally

## 1 — Pre-release ergonomics test

Tests passing ≠ API feeling good.
Bad docstrings, awkward signatures, and unclear errors only surface when used like a user would.

```bash
make python-dev-build
cd crates/transferred-py && uv run python
```

Exercise every public class added or changed this version. Example:

```python
from transferred import Transfer, FilesDestination
from transferred.arrow import ArrowSource

help(ArrowSource)
Transfer(source=[{"a": 1}], destination=FilesDestination("/tmp/out")).run()
```

Check:
- Docstrings useful in `help(...)` / IDE hover
- Public classes importable from documented module paths
- Error messages clear when wrong types are passed
- `RunReport.__repr__` reads well
- Every new public class has a committed `examples/*.py`; `make examples` passes

## 2 — Update README

Sync the whole README.md end-to-end with this version's surface,
including the code example (it's not covered by tests) and supported sources and destinations.
Output blocks (e.g. `print(report)`) must match actual output verbatim.
Code should be styled the same way as our Python code.

## 3 — Version bump

Edit `version = "..."` in workspace root `Cargo.toml`, tick `Deploy 0.0.x` in `PLAN.md`, then:

```bash
make bump-lock
make check
```

Commit message — single-line descriptive imperative, no Conventional Commits prefixes:

```
bump version to X.Y.Z
```

## 4 — Hand the diff over

Stop and ask the user to review the working tree before anything is committed.

## 5 — Open PR, ask for merge

Ask about merging to `main`.

## 6 — Tag and push

```bash
git checkout main && git pull
make release-tag
```

Triggers `.github/workflows/release.yml`.
CI's `verify` job rejects tags not on `main` or with version mismatching `transferred-core`'s `Cargo.toml`.

## 7 — Approve environments

Ask the user to approve the release for both environments in the GH Actions tab:
- `crates-io` — publishes core → files → postgres → py
- `pypi` — Trusted Publishers / OIDC

## 8 — Post-release smoke test

Wait for green CI, then install the package from the published wheel:

```bash
cd crates/transferred-py
uv pip install --refresh --force-reinstall "transferred==X.Y.Z"
uv pip install --refresh --force-reinstall "transferred[arrow]==X.Y.Z"

uv run python -c "
import transferred
# Smallest Transfer that exercises the new surface
"
```

Check:
- Binary wheel, not sdist fallback (no compile output)
- `import transferred` works
- Representative `Transfer(...).run()` succeeds

Restore dev install: `make python-dev-build`.

## 9 — Verify pages

- Check `https://pypi.org/project/transferred/X.Y.Z/` renders
- Check each crate page on crates.io
