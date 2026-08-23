"""The fixed-width table a suite prints, and which columns it holds."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from perf.metrics import MB, Metrics, Repeated


@dataclass(slots=True)
class _Column:
    header: str
    render: Callable[[Metrics, float], str]
    """Renders one cell from the fastest run and the drift across passes.

    Every column takes both even though most want one: a single signature is what
    lets `_cells` resolve each of them once instead of once per column.
    """


_COLUMNS: tuple[_Column, ...] = (
    _Column("workload", lambda best, _: best.workload),
    _Column("wall s", lambda best, _: f"{best.wall_seconds:.2f}"),
    _Column("spread", lambda _, spread: f"{spread:.2f}x"),
    _Column("rows", lambda best, _: f"{best.row_num:,}"),
    _Column("rows/s", lambda best, _: f"{round(best.throughput_rows_per_s):,}"),
    _Column("out MB", lambda best, _: _megabytes(best.output_bytes)),
    _Column("RSS MB avg/peak", lambda best, _: _slashed(best.rss_mb)),
    _Column("CPU % avg/max", lambda best, _: _slashed(best.cpu_percent)),
    _Column("CPU/wall", lambda best, _: f"{best.cpu_wall_ratio:.2f}"),
)


def table(metrics: list[Repeated]) -> str:
    """Render `metrics` as a fixed-width ASCII table, one row per workload."""
    header = [column.header for column in _COLUMNS]
    return grid(header, [_cells(run) for run in metrics])


def _cells(run: Repeated) -> list[str]:
    """One workload's cells, resolving its fastest run once rather than per column."""
    best = run.best
    spread = run.spread
    return [column.render(best, spread) for column in _COLUMNS]


def _megabytes(size_bytes: int) -> str:
    """A byte count as whole megabytes."""
    return f"{round(size_bytes / MB)}"


def _slashed(pair: tuple[float, ...]) -> str:
    """A column holding two numbers, as `avg/peak`."""
    return "/".join(f"{number:.0f}" for number in pair)


def grid(header: list[str], body: list[list[str]]) -> str:
    """Render `body` under `header` as a fixed-width ASCII table."""
    widths = _widths([header, *body])
    row_format = "  ".join(f"{{:<{width}}}" for width in widths)
    separator = row_format.format(*("-" * width for width in widths))
    rendered = [row_format.format(*row) for row in body]
    return "\n".join([row_format.format(*header), separator, *rendered])


def _widths(rows: list[list[str]]) -> list[int]:
    """The width of each column, set by its widest cell."""
    columns = zip(*rows, strict=True)
    return [max(len(cell) for cell in column) for column in columns]
