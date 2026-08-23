"""What one measured run produced, and how a repeated one is reported.

Apart from the harness that fills them: the table and the results JSON read these and
never spawn anything.
"""

from __future__ import annotations

from dataclasses import dataclass, field

MB = 1024 * 1024
"""Bytes in the megabyte every size in the table is reported in."""


@dataclass(slots=True)
class Sample:
    """RSS and CPU of the workload's process tree at one instant."""

    rss_bytes: int
    cpu_percent: float


@dataclass(slots=True)
class Metrics:
    """Aggregate per-workload measurements."""

    workload: str
    wall_seconds: float
    cpu_user_seconds: float
    cpu_system_seconds: float
    peak_rss_bytes: int
    row_num: int
    output_bytes: int
    samples: list[Sample] = field(default_factory=list)

    @property
    def throughput_rows_per_s(self) -> float:
        return self.row_num / self.wall_seconds if self.wall_seconds else 0

    @property
    def cpu_wall_ratio(self) -> float:
        cpu = self.cpu_user_seconds + self.cpu_system_seconds
        return cpu / self.wall_seconds if self.wall_seconds else 0

    @property
    def rss_mb(self) -> tuple[float, float]:
        """Mean RSS over the samples and the peak from rusage, in MB.

        The peak is rusage's because a spike between two samples is missed, and it is
        the peak that decides whether a run fits in RAM. Mixing sources is safe here
        and only here: rusage's peak is the true maximum, so it can never fall below
        a sampled value.
        """
        sampled = [sample.rss_bytes / MB for sample in self.samples]
        return _mean(sampled), self.peak_rss_bytes / MB

    @property
    def cpu_percent(self) -> tuple[float, float]:
        """Mean and max CPU over the samples, as a percentage of one core.

        Both are sampled, unlike `rss_mb`: rusage's mean is spread over the whole run
        and can land above every sample a short bursty leg produced, which would print
        a mean above its own maximum. `cpu_wall_ratio` reports that mean instead.
        """
        sampled = [sample.cpu_percent for sample in self.samples]
        return _mean(sampled), max(sampled, default=0)


def _mean(numbers: list[float]) -> float:
    """Arithmetic mean, or zero when there is nothing to average."""
    return sum(numbers) / len(numbers) if numbers else 0


@dataclass(slots=True)
class Repeated:
    """Every timed run of one workload, reported through its fastest.

    One run per pass, so the fastest is the pass the machine was quietest in — and
    since every engine is measured inside every pass, they all get the same chance
    at it.
    """

    runs: list[Metrics]

    @property
    def best(self) -> Metrics:
        """The fastest run, which is the closest estimate of the real cost.

        Noise on a shared machine is one-sided: the scheduler, another process or
        thermal throttling can only add time, never hand any back. Everything above
        the minimum is someone else's work, so the minimum is the engine's own cost.
        """
        return min(self.runs, key=lambda run: run.wall_seconds)

    @property
    def spread(self) -> float:
        """Slowest wall over fastest — how much the machine moved between passes.

        Not a measure of whether the passes sufficed: since a workload runs once per
        pass, this is pass-to-pass drift rather than scatter, and more passes will not
        shrink it. Read it as the error bar on everything in the row.
        """
        walls = [run.wall_seconds for run in self.runs]
        return max(walls) / min(walls) if min(walls) else 0
