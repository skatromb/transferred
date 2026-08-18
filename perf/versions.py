"""A/B our own two legs across published `transferred` versions, inside one suite.

A release claims a perf win; this is what checks it. Each version gets a venv holding
that wheel from PyPI, and every leg runs under each of them in turn:

    PERF_VERSIONS=0.1.1,0.1.2 make perf-versions

Interleaved rather than one suite after another, for the reason `perf.run` is
round-robin too: this machine drifts by half over an hour of load, so back-to-back
suites would credit the version that ran first. Nothing here is comparable to a
number from any other run.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType

from perf import fixtures, postgres
from perf.data import ROWS
from perf.harness import Metrics, Repeated, format_table
from perf.run import REPEATS, measure_once
from perf.workloads import parquet_to_postgres, postgres_to_parquet

LEGS: tuple[ModuleType, ...] = (postgres_to_parquet, parquet_to_postgres)
"""Both Postgres legs. The baselines are out: they do not change between our releases."""

VENVS = Path(__file__).resolve().parent / ".venvs"
"""Where each version's venv lives. Kept between runs — building one costs a download."""


def main() -> None:
    versions = _versions()
    postgres.check_disk(ROWS)
    postgres.up()
    postgres.seed(ROWS)
    fixtures.build(ROWS)
    pythons = {version: _install(version) for version in versions}

    with TemporaryDirectory() as tmp:
        metrics = _measure_all(pythons, Path(tmp))

    print(format_table(metrics))
    print(postgres.teardown_hint())


def _versions() -> list[str]:
    """Versions to compare, from `PERF_VERSIONS`. Required — there is no sensible pair.

    Raises:
        SystemExit: the variable is unset or names fewer than two versions.
    """
    versions = [
        v.strip() for v in os.environ.get("PERF_VERSIONS", "").split(",") if v.strip()
    ]
    if len(versions) < 2:
        raise SystemExit(
            "set PERF_VERSIONS to two or more published versions, comma-separated"
        )
    return versions


def _install(version: str) -> str:
    """Return the interpreter of a venv holding `transferred==version` from PyPI.

    Binary wheels only: an sdist would be compiled here, by this toolchain, and so
    would measure the local build rather than what a user installs. pyarrow rides
    along because `perf.fixtures` reads the seed's row count through it.

    The venv takes this suite's own Python version, since `uv venv` otherwise picks
    whatever it finds first — a 3.12 on this machine, which no wheel of ours fits.
    """
    root = VENVS / version
    python = root / "bin" / "python"
    if not python.exists():
        series = f"{sys.version_info.major}.{sys.version_info.minor}"
        subprocess.run(["uv", "venv", "-q", "--python", series, str(root)], check=True)
    print(f"venv: installing transferred=={version}", flush=True)
    subprocess.run(
        ["uv", "pip", "install", "-q", "--only-binary", ":all:", "--python", str(python),
         f"transferred=={version}", "pyarrow"],
        check=True,
    )  # fmt: skip
    return str(python)


def _measure_all(pythons: dict[str, str], workdir: Path) -> list[Repeated]:
    """Run every leg under every version once per pass, `REPEATS` passes."""
    runs: dict[str, list[Metrics]] = {}
    for index in range(REPEATS):
        for version, python in pythons.items():
            for mod in LEGS:
                label = f"{mod.NAME.removeprefix('transferred ')} {version}"
                print(f"pass {index + 1}/{REPEATS}: {label}", flush=True)
                runs.setdefault(label, []).append(
                    measure_once(label, mod, workdir, python)
                )
    return [Repeated(r) for r in runs.values()]


if __name__ == "__main__":
    main()
