from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True)
class PayloadCopyHealth:
    """Immutable operational state for one bounded payload-copy subsystem.

    Current-state fields describe work observed when the snapshot was captured.
    Counter and latency fields are cumulative since the subsystem was created.
    A timed-out caller can later be followed by a completed or failed worker because
    Python cannot safely stop a thread that has already started.
    """

    run_in_thread: bool
    max_concurrency: int
    active_copies: int = 0
    waiting_for_capacity: int = 0
    peak_active_copies: int = 0
    peak_waiting_for_capacity: int = 0
    started_count: int = 0
    completed_count: int = 0
    failure_count: int = 0
    timeout_count: int = 0
    capacity_timeout_count: int = 0
    capacity_wait_count: int = 0
    average_wait_ms: float = 0.0
    max_wait_ms: float = 0.0
    average_duration_ms: float = 0.0
    max_duration_ms: float = 0.0
    accepting_copies: bool = True
    shutdown_started: bool = False
    shutdown_complete: bool = False
    shutdown_incomplete: bool = False
    shutdown_timeout_count: int = 0
    active_at_last_shutdown_timeout: int = 0


class PayloadCopyHealthTracker:
    """Track bounded payload-copy activity without performing I/O or awaiting."""

    def __init__(self, *, run_in_thread: bool, max_concurrency: int) -> None:
        self._run_in_thread = bool(run_in_thread)
        self._max_concurrency = int(max_concurrency)
        self._lock = threading.Lock()
        self._active_copies = 0
        self._waiting_for_capacity = 0
        self._peak_active_copies = 0
        self._peak_waiting_for_capacity = 0
        self._started_count = 0
        self._completed_count = 0
        self._failure_count = 0
        self._timeout_count = 0
        self._capacity_timeout_count = 0
        self._capacity_wait_count = 0
        self._total_wait_ms = 0.0
        self._max_wait_ms = 0.0
        self._total_duration_ms = 0.0
        self._max_duration_ms = 0.0
        self._accepting_copies = True
        self._shutdown_started = False
        self._shutdown_complete = False
        self._shutdown_incomplete = False
        self._shutdown_timeout_count = 0
        self._active_at_last_shutdown_timeout = 0

    def capacity_wait_started(self) -> None:
        with self._lock:
            self._waiting_for_capacity += 1
            self._peak_waiting_for_capacity = max(
                self._peak_waiting_for_capacity,
                self._waiting_for_capacity,
            )

    def capacity_wait_finished(self, elapsed_seconds: float) -> None:
        elapsed_ms = max(0.0, float(elapsed_seconds) * 1000.0)
        with self._lock:
            if self._waiting_for_capacity <= 0:
                raise RuntimeError("payload copy capacity waiter counter cannot become negative")
            self._waiting_for_capacity -= 1
            self._capacity_wait_count += 1
            self._total_wait_ms += elapsed_ms
            self._max_wait_ms = max(self._max_wait_ms, elapsed_ms)

    def copy_started(self) -> None:
        with self._lock:
            self._active_copies += 1
            self._started_count += 1
            self._peak_active_copies = max(self._peak_active_copies, self._active_copies)

    def copy_finished(self, elapsed_seconds: float, *, failed: bool) -> None:
        elapsed_ms = max(0.0, float(elapsed_seconds) * 1000.0)
        with self._lock:
            if self._active_copies <= 0:
                raise RuntimeError("active payload copy counter cannot become negative")
            self._active_copies -= 1
            if failed:
                self._failure_count += 1
            else:
                self._completed_count += 1
            self._total_duration_ms += elapsed_ms
            self._max_duration_ms = max(self._max_duration_ms, elapsed_ms)

    def record_timeout(self, *, waiting_for_capacity: bool = False) -> None:
        with self._lock:
            self._timeout_count += 1
            if waiting_for_capacity:
                self._capacity_timeout_count += 1

    def shutdown_started(self) -> None:
        with self._lock:
            self._accepting_copies = False
            self._shutdown_started = True
            self._shutdown_complete = False

    def shutdown_timed_out(self, *, active_copies: int) -> None:
        with self._lock:
            self._shutdown_incomplete = True
            self._shutdown_timeout_count += 1
            self._active_at_last_shutdown_timeout = max(0, int(active_copies))

    def shutdown_completed(self) -> None:
        with self._lock:
            self._accepting_copies = False
            self._shutdown_started = True
            self._shutdown_complete = True
            self._shutdown_incomplete = False

    def snapshot(self) -> PayloadCopyHealth:
        with self._lock:
            finished_count = self._completed_count + self._failure_count
            average_wait_ms = (
                self._total_wait_ms / self._capacity_wait_count
                if self._capacity_wait_count
                else 0.0
            )
            average_duration_ms = (
                self._total_duration_ms / finished_count if finished_count else 0.0
            )
            return PayloadCopyHealth(
                run_in_thread=self._run_in_thread,
                max_concurrency=self._max_concurrency,
                active_copies=self._active_copies,
                waiting_for_capacity=self._waiting_for_capacity,
                peak_active_copies=self._peak_active_copies,
                peak_waiting_for_capacity=self._peak_waiting_for_capacity,
                started_count=self._started_count,
                completed_count=self._completed_count,
                failure_count=self._failure_count,
                timeout_count=self._timeout_count,
                capacity_timeout_count=self._capacity_timeout_count,
                capacity_wait_count=self._capacity_wait_count,
                average_wait_ms=average_wait_ms,
                max_wait_ms=self._max_wait_ms,
                average_duration_ms=average_duration_ms,
                max_duration_ms=self._max_duration_ms,
                accepting_copies=self._accepting_copies,
                shutdown_started=self._shutdown_started,
                shutdown_complete=self._shutdown_complete,
                shutdown_incomplete=self._shutdown_incomplete,
                shutdown_timeout_count=self._shutdown_timeout_count,
                active_at_last_shutdown_timeout=self._active_at_last_shutdown_timeout,
            )


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
    payload_copy_components: Mapping[str, PayloadCopyHealth] = field(default_factory=dict)
    active_payload_copies: int = 0
    waiting_for_copy_capacity: int = 0
    payload_copy_started_count: int = 0
    payload_copy_completed_count: int = 0
    payload_copy_failure_count: int = 0
    payload_copy_timeout_count: int = 0
    payload_copy_capacity_timeout_count: int = 0
    payload_copy_shutdown_incomplete: bool = False
    payload_copy_shutdown_timeout_count: int = 0
    payload_copy_active_at_last_shutdown_timeout: int = 0
    observability_buffers: Mapping[str, Any] = field(default_factory=dict)
    observability_pending: int = 0
    observability_peak_pending: int = 0
    observability_dropped_count: int = 0
    observability_failure_count: int = 0
    observability_shutdown_incomplete: bool = False
    observability_shutdown_timeout_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "capacity", MappingProxyType(dict(self.capacity)))
        object.__setattr__(self, "circuits", MappingProxyType(dict(self.circuits)))
        object.__setattr__(
            self,
            "payload_copy_components",
            MappingProxyType(dict(self.payload_copy_components)),
        )
        object.__setattr__(
            self,
            "observability_buffers",
            MappingProxyType(dict(self.observability_buffers)),
        )


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
