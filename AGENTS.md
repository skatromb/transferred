# AGENTS.md

@./docs/design/DESIGN.md
@./PLAN.md
Use ./Makefile for common development cycle commands.
Conventions for AI agents (Claude Code, Cursor, Codex, Aider, …) working in this repo.

Record durable conventions and learnings here, not in agent-private memory.

## Code taste

Open-source library read by humans. Optimise for the next reader, make it beautiful, not for closing the task.

- Write a code as a story: start from top-level and end with details.
- Simple and readable code — best code.
- Prefer removing a concept over adding a flag. Least code that fully does the job wins.
- A new abstraction (trait, enum, config knob, helper module) needs ≥2 real call sites *today*. No "we'll need it later".
- One function, one thing, one level of abstraction. Can't name it without "and"? Split it.
- Match the neighbours — naming, error handling, module layout come from the surrounding code.
- One comment, one line — `///`, `//`, `#` in Makefiles and CI, docstrings alike. Two lines need a strong reason the next reader could not reconstruct from the code.
- Say it plainly. A doc exists to be understood at a glance, not admired: "Field length that means NULL", not "Length standing in for a value Postgres should read as NULL". Prefer the shorter word, the direct clause, the ordinary term. If a sentence needs rereading, rewrite it.

## Rust docs

- A function's doc opens with a verb in third person, as std does.
- Document the thing you are on, not its neighbour. A constant says what it is and why that value; why the module exists belongs to the module, why we wrote our own belongs in PLAN.md until that version ships and DONE.md after. Development history is not a doc comment.
- **Exception — Python-visible PyO3 classes/methods in `crates/transferred-py/` (pyclass names not prefixed with `_`):** full docstring with one-line summary, `Args:` block, `Example:` block with runnable `>>> from transferred import …` snippet. Internal underscore-prefixed pyclasses keep the one-liner rule.

## Python docstrings (public API)

User-facing docstrings describe role + usage. Show an example. Do **not** mention:
- `_native_*` attributes or other underscore-prefixed internals.
- "Rust seam", FFI mechanics, PyO3 conventions.
- "Subclasses expose X" plumbing.
- Concrete subclasses by name in ABC docstrings. Use generic phrasing ("Subclasses are passed to `Transfer(source=…)`").

Internal pyclass docstrings (e.g. `_FilesSource`): one mechanical line. "Internal PyO3 wrapper around `transferred_X::Y`." Do not duplicate the public wrapper's API.

## Design docs (docs/design/DESIGN.md, PLAN.md, etc.)

- Terse. Assume reader is a working engineer.
- Don't explain SemVer, git workflow, REST basics, common patterns — link external standards instead.
- 1–3 short sentences per policy/decision. Bullets/tables for discrete cases.
- No motivational copy when the rule implies the motivation.

## PLAN.md hygiene

PLAN.md holds unshipped work only, each `## X.Y.Z` heading scoped to its version. Moving an item to a later version removes it from the earlier one — no "deferred to" breadcrumbs.

A shipped version's section moves verbatim to the end of [DONE.md](./DONE.md), which is not loaded by default. Grep it for why a decision went the way it did.

## Lints & typechecks

Fix the root cause. Don't sprinkle `#[allow(…)]`, `# noqa`, `# type: ignore`, `// eslint-disable` in production code. If the lint is genuinely wrong for the case, justify before suppressing.

Test code is the documented exception — file-level allows are fine in `tests/`, `#[cfg(test)] mod tests`, `conftest.py`.

Python is linted twice: ruff owns formatting, imports and the pycodestyle/pyflakes overlap;
`wemake-python-styleguide` owns the `WPS` rules via `make wps` and `.flake8`.
The `wps` MCP server in `.mcp.json` explains any `WPS###` offline.

## Throwaway Postgres

Hand-testing against a database (examples, smoke tests) reuses the image the integration suite already pulls — `imresamu/postgis`

## Citing library defaults

When asserting a specific numeric default (row group sizes, batch sizes, timeouts, CPU counts), cite the source — file path, doc URL, or `help()` output — in the same message. Don't recall from memory and let the reader push back. Especially load-bearing for perf claims.

## Commits on feature branches

Small fixup belonging to the last commit? Amend + force-with-lease:

```
git commit --amend --no-edit && git push --force-with-lease
```

Don't stack "fix CI" / "fix lint" follow-ups on a feature branch. Reserve new commits for genuinely separate changes. Never amend commits already on `main`.

## PR descriptions

Default to bullets of what and why was done.

Skip test-related stuff when CI covers the checks.

No AI-attribution trailer (`🤖 Generated with Claude Code`) in PR bodies.

## Merging

`gh pr merge --squash --delete-branch`. One commit per PR on `main`; the PR title becomes its subject. Only switch strategy if explicitly requested for a specific PR.

## Task tracking

`TaskCreate` / `TaskUpdate` (Claude Code) — fine-grained subtasks of current session/PR only. PLAN.md is the source of truth for roadmap items; don't duplicate them as tasks.
