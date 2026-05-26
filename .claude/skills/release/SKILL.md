---
name: release
description: |
  Cut a release of `transferred` (Rust crates + Python wheel). Triggers when
  user asks to release, deploy, ship, publish, tag, cut version X.Y.Z, PyPI / crates.io.
  Covers pre-release ergonomics, version bump, tagging,
  CI handoff, post-release smoke, README refresh.
---

# release — cut a `transferred` version

Ships Rust crates (`transferred-core`, `transferred-parquet`, `transferred-py`) to crates.io and the `transferred` wheel to PyPI. CI publishes; this skill is the human pre/post work.

Use `make` targets where they exist. If a step has no target, do it by hand or extend the Makefile.

## Preconditions

- All scope items for the version in `PLAN.md` are `[x]` except `Deploy 0.0.x`.
- `main` is green.
- `make pre-release` passes locally.

## 1 — Pre-release ergonomics test

Tests passing ≠ API feeling good. Bad docstrings, awkward signatures, and unclear errors only surface when used like a user would. Caught post-publish = patch release.

```bash
make python-dev-build
cd crates/transferred-py && uv run python
```

Exercise every public class added or changed this version. For 0.0.2 example:

```python
from transferred import Transfer, ParquetDestination
from transferred.arrow import ArrowSource

help(ArrowSource)
Transfer(source=[{"a": 1}], destination=ParquetDestination("/tmp/x.parquet")).run()
```

Check:
- Docstrings useful in `help(...)` / IDE hover
- Public classes importable from documented module paths
- Error messages clear when wrong types passed in
- `RunReport.__repr__` reads well

Fix issues before tagging, even if it means another PR.

## 2 — Version bump

Edit `version = "..."` in workspace root `Cargo.toml`, then:

```bash
make bump-lock
make check
```

Open PR. Commit message — single-line descriptive imperative, no Conventional Commits prefixes:

```
bump version to X.Y.Z
```

## 3 — Tick PLAN.md, merge

Tick `Deploy 0.0.x` in `PLAN.md` in the same PR (or a follow-up). No ticking-only commits.

Merge to `main`.

## 4 — Tag and push

```bash
git checkout main && git pull
make release-tag
```

Triggers `.github/workflows/release.yml`. CI's `verify` job rejects tags not on `main` or with version mismatching `transferred-core`'s `Cargo.toml`.

## 5 — Approve environments

In the Actions tab, approve gates for both environments:
- `crates-io` — publishes core → parquet → py
- `pypi` — Trusted Publishers / OIDC

## 6 — Post-release smoke test

Green CI proves upload, not install. Replace the local maturin install with the published wheel:

```bash
cd crates/transferred-py
uv pip install --force-reinstall "transferred==X.Y.Z"
uv pip install --force-reinstall "transferred[arrow]==X.Y.Z"   # extras for 0.0.2+

uv run python -c "
import transferred
# Smallest Transfer that exercises the new surface
"
```

Check:
- Binary wheel, not sdist fallback (no compile output)
- `import transferred` works
- Representative `Transfer(...).run()` succeeds

Restore dev install: `make python-setup`.

## 7 — README + crates.io

- Check `https://pypi.org/project/transferred/X.Y.Z/` renders
- Check each crate page on crates.io
- Update `Status:` line in `README.md`. Example: `Status: 0.0.1. Parquet source + destination only.` → `Status: 0.0.2. Adds Python-native iterable source.`
