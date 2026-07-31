# AGENTS.md

Read ./docs/design/DESIGN.md and ./PLAN.md first to get grasp on current state of things.
Use ./Makefile for common development cycle commands.
Conventions for AI agents (Claude Code, Cursor, Codex, Aider, …) working in this repo.

Record durable conventions and learnings here, not in agent-private memory.

## Rust docs

- `///` one-liner on top-level pub items + free functions + trait/inherent methods. Describe the *what*.
- No docs on individual struct fields or enum variants.
- No docs on trivial wrappers (e.g. `Into<String>` constructors).
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

Each `## X.Y.Z` heading holds only items scoped to that version. When moving an item to a later version, fully remove it from the earlier section — no "deferred to" breadcrumbs.

## Lints & typechecks

Fix the root cause. Don't sprinkle `#[allow(…)]`, `# noqa`, `# type: ignore`, `// eslint-disable` in production code. If the lint is genuinely wrong for the case, justify before suppressing.

Test code is the documented exception — file-level allows are fine in `tests/`, `#[cfg(test)] mod tests`, `conftest.py`.

## Citing library defaults

When asserting a specific numeric default (row group sizes, batch sizes, timeouts, CPU counts), cite the source — file path, doc URL, or `help()` output — in the same message. Don't recall from memory and let the reader push back. Especially load-bearing for perf claims.

## Commits on feature branches

Small fixup belonging to the last commit? Amend + force-with-lease:

```
git commit --amend --no-edit && git push --force-with-lease
```

Don't stack "fix CI" / "fix lint" follow-ups on a feature branch. Reserve new commits for genuinely separate changes. Never amend commits already on `main`.

## PR descriptions

Default to `## Summary` + bullets. One line per bullet, stating what and why was done.

Skip test-related stuff when CI covers the checks.

No AI-attribution trailer (`🤖 Generated with Claude Code`) in PR bodies.

## Merging

`gh pr merge --squash --delete-branch`. One commit per PR on `main`; the PR title becomes its subject. Only switch strategy if explicitly requested for a specific PR.

## Task tracking

`TaskCreate` / `TaskUpdate` (Claude Code) — fine-grained subtasks of current session/PR only. PLAN.md is the source of truth for roadmap items; don't duplicate them as tasks.
