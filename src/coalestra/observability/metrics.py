from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass


@dataclass(frozen=True)
class ObservationSummary:
    count: int
    total: float
    minimum: float
    maximum: float
    average: float


class NullMetrics:
    coalestra_non_blocking = True

    def increment(self, metric: str, value: int = 1, **labels: str) -> None:
        return None

    def observe(self, metric: str, value: float, **labels: str) -> None:
        return None


class InMemoryMetrics:
    coalestra_non_blocking = True

    """Dependency-free metrics sink suitable for tests and local diagnostics."""

    def __init__(self) -> None:
        self._counters: defaultdict[tuple[str, tuple[tuple[str, str], ...]], int] = defaultdict(int)
        self._observations: defaultdict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = (
            defaultdict(list)
        )
        self._lock = threading.Lock()

    def increment(self, metric: str, value: int = 1, **labels: str) -> None:
        key = (metric, tuple(sorted(labels.items())))
        with self._lock:
            self._counters[key] += value

    def observe(self, metric: str, value: float, **labels: str) -> None:
        key = (metric, tuple(sorted(labels.items())))
        with self._lock:
            self._observations[key].append(float(value))

    def counter(self, metric: str, **labels: str) -> int:
        key = (metric, tuple(sorted(labels.items())))
        with self._lock:
            return self._counters.get(key, 0)

    def summary(self, metric: str, **labels: str) -> ObservationSummary | None:
        key = (metric, tuple(sorted(labels.items())))
        with self._lock:
            values = list(self._observations.get(key, []))
        if not values:
            return None
        total = sum(values)
        return ObservationSummary(
            count=len(values),
            total=total,
            minimum=min(values),
            maximum=max(values),
            average=total / len(values),
        )
