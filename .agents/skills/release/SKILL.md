---
name: release
description: |
  Make a release of `transferred` (Rust crates + Python wheel).
  Triggers when user asks to release or ship a new version; PyPI or crates.io.
---

# release — cut a `transferred` version

CI publishes the crates to crates.io and the wheels to PyPI; this skill is the human work around it. Use `make` targets where they exist; otherwise do the step by hand or extend the Makefile.

## Preconditions

- The version's `PLAN.md` scope is `[x]`.
- Every public source, destination or format added or changed has an `examples/*.py`.
- `make pre-release` passes.

## 1 — Try the API as a user

Tests don't catch bad docstrings, awkward signatures or unclear errors.

```bash
make python-dev-build
cd crates/transferred-py && uv run python
```

Exercise every public class added or changed:

- Docstrings useful in `help(...)` / IDE hover
- Public classes importable from documented module paths
- Error messages clear when wrong types are passed
- `RunReport.__repr__` reads well
- Every new public class has a committed `examples/*.py`; `make examples` passes

## 2 — Sync README

Sync the whole README.md end to end with this version's surface:

- The code example, which no test covers
- Supported sources and destinations
- Output blocks (e.g. `print(report)`) match actual output verbatim
- Code styled the same way as our Python code

## 3 — Bump

Set `version` in the root `Cargo.toml` and tick `Deploy X.Y.Z` in `PLAN.md` if present, then `make bump-lock && make check`. Commit `bump version to X.Y.Z`.

Stop: the user reviews the working tree before anything is committed. Then open a PR and ask to merge.

## 4 — Tag

```bash
git checkout main && git pull
make release-tag
```

Triggers `release.yml`, whose `verify` job rejects a tag off `main` or not matching the `Cargo.toml` version. A fix needed before publishing: merge it, then `make release-retag`.

Ask the user to approve both environments in GH Actions:

- `crates-io` — publishes core → files → postgres → py
- `pypi` — Trusted Publishers / OIDC

## 5 — Smoke test the published package

After CI is green, install from the published wheel:

```bash
cd crates/transferred-py
uv pip install --refresh --force-reinstall "transferred==X.Y.Z"
uv pip install --refresh --force-reinstall "transferred[arrow]==X.Y.Z"

uv run python -c "
import transferred
# Smallest Transfer that exercises the new surface
"
```

And on Linux:

```bash
docker run --rm ghcr.io/astral-sh/uv:python3.14-trixie-slim \
    uv run --no-project --with "transferred[arrow]==X.Y.Z" python -c "..."
```

`--refresh` only skips uv's local cache. PyPI's CDN can lag a few minutes after upload, so a missing version or wheel may just be stale: wait and retry.

Check:

- Binary wheel, not sdist fallback (no compile output)
- `import transferred` works
- A `Transfer(...).run()` exercising the new surface succeeds

Restore dev install: `make python-dev-build`.

## 6 — Verify pages

- `https://pypi.org/project/transferred/X.Y.Z/` renders and lists as many files as the previous release. Same CDN lag applies.
- Each crate's page on crates.io
