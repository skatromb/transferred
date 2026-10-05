# AGENTS.md

@./docs/design/DESIGN.md
@./PLAN.md

Rules for AI agents in this repo. Dev commands: `./Makefile`. Record durable learnings here, not in agent memory.

## Code taste

Open-source library. Write for the next reader, not to close the task.

- Code reads as a story: top level first, details last.
- Remove a concept rather than add a flag. Least code that does the whole job wins.
- New abstraction (trait, enum, knob, helper module) needs ≥2 real call sites today.
- One function, one thing, one level of abstraction. Name needs "and"? Split.
- Match the neighbours: naming, error handling, module layout.
- Comments and docstrings: one line. A second line needs a reason the code can't show.
- Plain words: "Field length that means NULL", not "Length standing in for a value Postgres should read as NULL". Needs rereading? Rewrite.

## Rust docs

- Function doc opens with a third-person verb, as std does.
- Document the item you're on. A constant says what it is and why that value. Why the module exists goes on the module; why we built our own goes in PLAN.md, then DONE.md. No dev history.
- Exception: public pyclasses and methods in `crates/transferred-py/` (no `_` prefix) get a summary line, `Args:` and a runnable `Example:` (`>>> from transferred import …`).

## Python docstrings

- Public: role, usage, example. Never mention `_`-prefixed internals, Rust/FFI/PyO3 mechanics, subclass plumbing, or concrete subclasses in ABC docs ("Subclasses are passed to `Transfer(source=…)`").
- Internal pyclass (`_FilesSource`): "Internal PyO3 wrapper around `transferred_X::Y`." Nothing more.

## Design docs

Terse, for a working engineer. Link standards (SemVer, git, REST) instead of explaining them. 1–3 sentences per decision; bullets or tables for cases. No motivation the rule already implies.

## PLAN.md

- Unshipped work only, each item under its version. Moving an item removes it from the old version; no "deferred to" notes.
- A shipped section moves verbatim to the end of [DONE.md](./DONE.md). Grep it for past decisions.
- Tick a box in the commit that delivered the item, never in its own commit.

## Lints

- Fix the root cause. No `#[allow]`, `# noqa`, `# type: ignore` in production code without a justification. `tests/`, `#[cfg(test)] mod tests` and `conftest.py` may use file-level allows.
- Rust: clippy `pedantic` + `nursery` + `restriction` + stable rustc lints, thresholds in `clippy.toml`. Suppress with `#[expect(lint, reason = "…")]`, never `#[allow]`.
- Name generic parameters for what they stand for (`Column`, not `T`); no lint checks it. Lifetimes have `single_char_lifetime_names`.
- Python: ruff owns format, imports, pyflakes/pycodestyle; `make wps` (`.flake8`) owns `WPS`. The `wps` MCP server explains any `WPS###`.

## Coverage

- `make coverage-rust`, `make coverage-python`. Uploaded to Codecov (`rust`, `python` flags) on merge to `main` only.
- pytest covers Rust lines too, via `cargo-llvm-cov` `.profraw`. Stable toolchain: no `#[coverage(off)]`.
- Keep the two reports separate: maturin and `cargo test` build different features, so one report compares counters against the wrong binary. Codecov merges them.

## Hand testing

Throwaway Postgres: `imresamu/postgis`, the image the integration suite uses.

## Citing defaults

Claiming a numeric library default (row group, batch size, timeout, CPU count)? Cite the source (file, doc URL, `help()`) in the same message. Never from memory.

## Git

- Fixup for the last commit: `git commit --amend --no-edit && git push --force-with-lease`. No "fix CI"/"fix lint" commits. Never amend `main`.
- PR body: bullets of what and why. Test plan only for manual checks. No AI-attribution trailer.
- Merge: `gh pr merge --squash --delete-branch`. The PR title becomes the commit subject.

## Task tracking

`TaskCreate`/`TaskUpdate` for this session's subtasks only. Roadmap lives in PLAN.md.
