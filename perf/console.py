"""Where the suite's own words go: progress on stderr, results on stdout.

The split is load-bearing rather than tidy. A workload's stdout is the JSON result
protocol the harness parses, so anything a workload imports that writes a progress
line to stdout breaks the run that reads it.
"""

from __future__ import annotations

import sys


def progress(message: str) -> None:
    """Write one progress line to stderr, flushed.

    Flushed because a seed or a dump runs for minutes: a redirected stderr would
    hold the line until the whole suite finished, leaving the run looking hung.
    """
    sys.stderr.write(f"{message}\n")
    sys.stderr.flush()


def report(message: str) -> None:
    """Write a block of results to stdout, where a pipe or a file can collect it."""
    sys.stdout.write(f"{message}\n")
