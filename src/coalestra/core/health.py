from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True)
class BuilderHealth:
    """Immutable operational state for integration health endpoints.

    Current-state fields describe work observed when the snapshot was captured.
    Counter fields are cumulative since the builder was created.
    """

    closed: bool
    background_refreshes: int
    singleflight_in_flight: int
    source_support_cache_entries: int
    capacity: Mapping[str, Any] = field(default_factory=dict)
    cache: Any | None = None
    circuits: Mapping[Any, Any] = field(default_factory=dict)
    active_dispatch_workers: int = 0
    waiting_for_capacity: int = 0
    queue_timeout_count: int = 0
    source_timeout_count: int = 0
    deadline_exceeded_count: int = 0
    revalidation_attempt_count: int = 0
    revalidation_failure_count: int = 0
    pending_submissions: int = 0
    max_pending_submissions: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "capacity", MappingProxyType(dict(self.capacity)))
        object.__setattr__(self, "circuits", MappingProxyType(dict(self.circuits)))


@dataclass(frozen=True)
class OperationalHealthSnapshot:
    """Internal cumulative and current counters used to build ``BuilderHealth``."""

    active_dispatch_workers: int
    queue_timeout_count: int
    source_timeout_count: int
    deadline_exceeded_count: int
    revalidation_attempt_count: int
    revalidation_failure_count: int


class OperationalHealthTracker:
    """Collect low-cardinality operational counters without performing I/O.

    A regular thread lock keeps updates safe when the asynchronous builder is
    hosted by the synchronous facade's event-loop thread. Counter operations
    never await and remain independent from metrics or event sinks.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_dispatch_workers = 0
        self._queue_timeout_count = 0
        self._source_timeout_count = 0
        self._deadline_exceeded_count = 0
        self._revalidation_attempt_count = 0
        self._revalidation_failure_count = 0

    def dispatch_worker_started(self) -> None:
        with self._lock:
            self._active_dispatch_workers += 1

    def dispatch_worker_finished(self) -> None:
        with self._lock:
            if self._active_dispatch_workers <= 0:
                raise RuntimeError("dispatch worker counter cannot become negative")
            self._active_dispatch_workers -= 1

    def record_queue_timeout(self) -> None:
        with self._lock:
            self._queue_timeout_count += 1

    def record_source_timeout(self) -> None:
        with self._lock:
            self._source_timeout_count += 1

    def record_deadline_exceeded(self) -> None:
        with self._lock:
            self._deadline_exceeded_count += 1

    def record_revalidation(self, *, failed: bool) -> None:
        with self._lock:
            self._revalidation_attempt_count += 1
            if failed:
                self._revalidation_failure_count += 1

    def snapshot(self) -> OperationalHealthSnapshot:
        with self._lock:
            return OperationalHealthSnapshot(
                active_dispatch_workers=self._active_dispatch_workers,
                queue_timeout_count=self._queue_timeout_count,
                source_timeout_count=self._source_timeout_count,
                deadline_exceeded_count=self._deadline_exceeded_count,
                revalidation_attempt_count=self._revalidation_attempt_count,
                revalidation_failure_count=self._revalidation_failure_count,
            )
