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

from perf import console, disk, fixtures, render, results, server
from perf.data import ROW_NUM
from perf.metrics import Metrics, Repeated
from perf.run import REPEATS, measure_once
from perf.workloads import parquet_to_postgres, postgres_to_parquet

LEGS: tuple[ModuleType, ...] = (postgres_to_parquet, parquet_to_postgres)
"""Both Postgres legs. The baselines are out: they do not change between our releases."""

VENVS = Path(__file__).resolve().parent / ".venvs"
"""Where each version's venv lives. Kept between runs — building one costs a download."""

type VersionedPythons = list[tuple[str, str]]
"""Each version paired with the interpreter of the venv holding it."""

_MIN_VERSIONS = 2
"""Fewer than a pair is not a comparison."""


def main() -> None:
    versions = _versions()
    disk.check_disk(ROW_NUM)
    server.up()
    server.seed(ROW_NUM)
    fixtures.build(ROW_NUM)
    pythons = {version: _install(version) for version in versions}

    with TemporaryDirectory() as workdir:
        metrics = _measure_all(pythons, Path(workdir))

    console.report(render.table(metrics))
    console.report(f"\nfull results → {results.results_path()}")
    console.report(server.teardown_hint())


def _versions() -> list[str]:
    """Versions to compare, from `PERF_VERSIONS`. Required — there is no sensible pair.

    Raises:
        SystemExit: the variable is unset or names fewer than two versions.
    """
    requested = os.environ.get("PERF_VERSIONS", "").split(",")
    versions = [name.strip() for name in requested if name.strip()]
    if len(versions) < _MIN_VERSIONS:
        sys.exit("set PERF_VERSIONS to two or more published versions, comma-separated")
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
        series = ".".join(map(str, sys.version_info[:2]))
        create = ["uv", "venv", "-q", "--python", series, str(root)]
        subprocess.run(create, check=True)
    console.progress(f"venv: installing transferred=={version}")
    subprocess.run(
        ["uv", "pip", "install", "-q", "--only-binary", ":all:", "--python", str(python),
         f"transferred=={version}", "pyarrow"],
        check=True,
    )  # fmt: skip
    return str(python)


def _measure_all(pythons: dict[str, str], workdir: Path) -> list[Repeated]:
    """Run every leg under every version once per pass, `REPEATS` passes.

    Leg outermost, so the versions of one leg run back to back and share whatever the
    machine is doing in that minute. Version outermost would put the other leg between
    them, which is the drift the comparison is trying not to charge to a release.
    """
    runs: dict[str, list[Metrics]] = {}
    for current in range(1, REPEATS + 1):
        for leg in LEGS:
            _measure_leg(runs, leg, _ordered(pythons, current), workdir, current)
    return [Repeated(metrics) for metrics in runs.values()]


def _measure_leg(
    runs: dict[str, list[Metrics]],
    leg: ModuleType,
    versions: VersionedPythons,
    workdir: Path,
    current: int,
) -> None:
    """Measure `leg` under each version in turn, recording every run into `runs`."""
    for version, python in versions:
        engine = leg.NAME.removeprefix("transferred ")
        label = f"{engine} {version}"
        console.progress(f"pass {current}/{REPEATS}: {label}")
        runs.setdefault(label, []).append(measure_once(label, leg, workdir, python))
        results.dump_results(runs)


def _ordered(pythons: dict[str, str], current: int) -> VersionedPythons:
    """The versions to run in pass `current`, swapped over on every other one.

    So neither version always pays whatever it costs to be the one that runs first.
    """
    versions = list(pythons.items())
    if current % 2 == 0:
        versions.reverse()
    return versions


if __name__ == "__main__":
    main()
