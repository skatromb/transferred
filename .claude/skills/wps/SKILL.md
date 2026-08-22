---
name: wps
description: >
  wemake-python-styleguide (WPS) linting. Use when fixing flake8 `WPS` violations,
  or writing/reviewing Python in this repo.
---

# wemake-python-styleguide

**Always** write Python code that will pass all rules from the [violations index](https://wemake-python-styleguide.readthedocs.io/en/latest/pages/usage/violations/index.html).
Edit existing Python code until `make wps` reports nothing.
Fix the code, not the config.

## Loop

1. `make wps` — runs `flake8` from `crates/transferred-py` against `.flake8`, which
   selects `WPS` + `E999` and nothing else. Lint one file with
   `cd crates/transferred-py && uv run --no-sync flake8 <path>`.
2. For each violation, edit the code so its cause is gone — the message names the rule
   and line; make the smallest change that satisfies it. When a rule's intent is
   unclear, call `explain_violation('WPS###')` from the `wps` MCP server, or read its
   page under the violation index.
3. Re-run step 1. Done when it exits 0.

## Guardrails

- Suppress with `# noqa: WPS###` (the code, never bare `# noqa`) only when the user asks,
  or `per-file-ignores` already covers the line. Otherwise fix the code.
- Leave `crates/transferred-py/.flake8` untouched unless the user approves the edit. When
  one rule fires across many files and fixing each is wrong, propose a config change and wait.
- Delete a `# noqa: WPS###` once `flake8 --disable-noqa` shows its code no longer fires.
- Ruff owns formatting, imports and the pycodestyle/pyflakes overlap. A WPS fix that ruff
  then reformats is fine; a WPS fix that fights `make ruff` is the wrong fix.
