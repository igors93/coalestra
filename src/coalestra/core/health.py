from __future__ import annotations

import math
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

BUILDER_HEALTH_SCHEMA = "coalestra.builder-health"
BUILDER_HEALTH_SCHEMA_VERSION = 1
BUILDER_HEALTH_ASSESSMENT_SCHEMA = "coalestra.builder-health-assessment"
BUILDER_HEALTH_ASSESSMENT_SCHEMA_VERSION = 1


class BuilderHealthSeverity(str, Enum):
    """Stable severity levels returned by builder health assessment."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"


class BuilderHealthReason(str, Enum):
    """Stable low-cardinality reasons produced by builder health assessment."""

    BUILDER_CLOSED = "builder_closed"
    SOURCE_CAPACITY_WAITING = "source_capacity_waiting"
    PAYLOAD_COPY_CAPACITY_WAITING = "payload_copy_capacity_waiting"
    SUBMISSION_BACKLOG_HIGH = "submission_backlog_high"
    OBSERVABILITY_BACKLOG_HIGH = "observability_backlog_high"
    CIRCUIT_OPEN = "circuit_open"
    CIRCUIT_HALF_OPEN = "circuit_half_open"
    PAYLOAD_COPY_SHUTDOWN_INCOMPLETE = "payload_copy_shutdown_incomplete"
    OBSERVABILITY_SHUTDOWN_INCOMPLETE = "observability_shutdown_incomplete"
    OBSERVABILITY_WORKER_STOPPED = "observability_worker_stopped"
    QUEUE_TIMEOUTS_INCREASED = "queue_timeouts_increased"
    SOURCE_TIMEOUTS_INCREASED = "source_timeouts_increased"
    DEADLINES_EXCEEDED_INCREASED = "deadlines_exceeded_increased"
    REVALIDATION_FAILURES_INCREASED = "revalidation_failures_increased"
    PAYLOAD_COPY_FAILURES_INCREASED = "payload_copy_failures_increased"
    PAYLOAD_COPY_TIMEOUTS_INCREASED = "payload_copy_timeouts_increased"
    PAYLOAD_COPY_SHUTDOWN_TIMEOUTS_INCREASED = "payload_copy_shutdown_timeouts_increased"
    OBSERVABILITY_DROPS_INCREASED = "observability_drops_increased"
    OBSERVABILITY_SHUTDOWN_TIMEOUTS_INCREASED = "observability_shutdown_timeouts_increased"
    OBSERVABILITY_FAILURES_INCREASED = "observability_failures_increased"


@dataclass(frozen=True)
class BuilderHealthAssessmentPolicy:
    """Thresholds used to classify a ``BuilderHealth`` snapshot.

    Current-state thresholds are always evaluated. Cumulative counter thresholds
    are evaluated only when a previous health snapshot is provided, preventing one
    historical failure from making every later point-in-time assessment degraded.
    """

    capacity_waiting_degraded: int = 1
    capacity_waiting_critical_ratio: float = 1.0
    copy_waiting_degraded: int = 1
    copy_waiting_critical_ratio: float = 1.0
    submission_backlog_degraded_ratio: float = 0.5
    submission_backlog_critical_ratio: float = 0.9
    observability_backlog_degraded_ratio: float = 0.5
    observability_backlog_critical_ratio: float = 0.9
    counter_delta_degraded: int = 1
    counter_delta_critical: int = 5

    def __post_init__(self) -> None:
        for name in (
            "capacity_waiting_degraded",
            "copy_waiting_degraded",
            "counter_delta_degraded",
            "counter_delta_critical",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
            if value < 1:
                raise ValueError(f"{name} must be at least 1")

        if self.counter_delta_critical < self.counter_delta_degraded:
            raise ValueError(
                "counter_delta_critical must be greater than or equal to counter_delta_degraded"
            )

        for name in (
            "capacity_waiting_critical_ratio",
            "copy_waiting_critical_ratio",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TypeError(f"{name} must be a number")
            if not math.isfinite(float(value)) or float(value) <= 0:
                raise ValueError(f"{name} must be finite and positive")

        for degraded_name, critical_name in (
            (
                "submission_backlog_degraded_ratio",
                "submission_backlog_critical_ratio",
            ),
            (
                "observability_backlog_degraded_ratio",
                "observability_backlog_critical_ratio",
            ),
        ):
            degraded = getattr(self, degraded_name)
            critical = getattr(self, critical_name)
            for name, value in ((degraded_name, degraded), (critical_name, critical)):
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise TypeError(f"{name} must be a number")
                if not math.isfinite(float(value)) or not 0 < float(value) <= 1:
                    raise ValueError(f"{name} must be greater than 0 and at most 1")
            if float(critical) < float(degraded):
                raise ValueError(
                    f"{critical_name} must be greater than or equal to {degraded_name}"
                )


@dataclass(frozen=True)
class BuilderHealthFinding:
    """One stable and actionable reason contributing to health severity."""

    reason: BuilderHealthReason
    severity: BuilderHealthSeverity
    metric: str
    observed: int | float | bool
    threshold: int | float | bool | None = None
    component: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-safe representation of this finding."""

        return {
            "reason": self.reason.value,
            "severity": self.severity.value,
            "metric": self.metric,
            "observed": self.observed,
            "threshold": self.threshold,
            "component": self.component,
        }


@dataclass(frozen=True)
class BuilderHealthAssessment:
    """Immutable severity assessment derived from one builder health snapshot."""

    severity: BuilderHealthSeverity
    findings: tuple[BuilderHealthFinding, ...] = ()
    baseline_used: bool = False

    @property
    def reasons(self) -> tuple[BuilderHealthReason, ...]:
        """Return unique reasons in deterministic finding order."""

        return tuple(dict.fromkeys(finding.reason for finding in self.findings))

    def to_dict(self) -> dict[str, Any]:
        """Return a versioned JSON-safe assessment payload."""

        return {
            "schema": BUILDER_HEALTH_ASSESSMENT_SCHEMA,
            "schema_version": BUILDER_HEALTH_ASSESSMENT_SCHEMA_VERSION,
            "severity": self.severity.value,
            "baseline_used": self.baseline_used,
            "reasons": [reason.value for reason in self.reasons],
            "findings": [finding.to_dict() for finding in self.findings],
        }


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

    def assess(
        self,
        *,
        previous: BuilderHealth | None = None,
        policy: BuilderHealthAssessmentPolicy | None = None,
    ) -> BuilderHealthAssessment:
        """Classify this snapshot using current state and optional counter deltas."""

        if previous is not None and not isinstance(previous, BuilderHealth):
            raise TypeError("previous must be a BuilderHealth or None")
        if policy is not None and not isinstance(policy, BuilderHealthAssessmentPolicy):
            raise TypeError("policy must be a BuilderHealthAssessmentPolicy or None")
        return _assess_builder_health(
            self,
            previous=previous,
            policy=policy or BuilderHealthAssessmentPolicy(),
        )

    def to_dict(
        self,
        *,
        include_assessment: bool = True,
        previous: BuilderHealth | None = None,
        assessment_policy: BuilderHealthAssessmentPolicy | None = None,
    ) -> dict[str, Any]:
        """Return a versioned JSON-safe representation of the complete health state."""

        if not isinstance(include_assessment, bool):
            raise TypeError("include_assessment must be a boolean")
        if previous is not None and not isinstance(previous, BuilderHealth):
            raise TypeError("previous must be a BuilderHealth or None")
        if assessment_policy is not None and not isinstance(
            assessment_policy,
            BuilderHealthAssessmentPolicy,
        ):
            raise TypeError("assessment_policy must be a BuilderHealthAssessmentPolicy or None")

        payload = {
            "schema": BUILDER_HEALTH_SCHEMA,
            "schema_version": BUILDER_HEALTH_SCHEMA_VERSION,
        }
        for health_field in fields(self):
            payload[health_field.name] = _json_safe(getattr(self, health_field.name))
        payload["assessment"] = (
            self.assess(previous=previous, policy=assessment_policy).to_dict()
            if include_assessment
            else None
        )
        return payload


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


def _assess_builder_health(
    health: BuilderHealth,
    *,
    previous: BuilderHealth | None,
    policy: BuilderHealthAssessmentPolicy,
) -> BuilderHealthAssessment:
    findings: list[BuilderHealthFinding] = []

    def add(
        reason: BuilderHealthReason,
        severity: BuilderHealthSeverity,
        metric: str,
        observed: int | float | bool,
        threshold: int | float | bool | None = None,
        component: str = "",
    ) -> None:
        findings.append(
            BuilderHealthFinding(
                reason=reason,
                severity=severity,
                metric=metric,
                observed=observed,
                threshold=threshold,
                component=component,
            )
        )

    if health.closed:
        add(
            BuilderHealthReason.BUILDER_CLOSED,
            BuilderHealthSeverity.CRITICAL,
            "closed",
            True,
            False,
        )

    capacity_waiting = max(0, int(health.waiting_for_capacity))
    if capacity_waiting >= policy.capacity_waiting_degraded:
        capacity_ratio, capacity_component = _maximum_wait_ratio(health.capacity)
        severity = (
            BuilderHealthSeverity.CRITICAL
            if capacity_ratio >= policy.capacity_waiting_critical_ratio
            else BuilderHealthSeverity.DEGRADED
        )
        add(
            BuilderHealthReason.SOURCE_CAPACITY_WAITING,
            severity,
            "maximum_waiting_to_limit_ratio"
            if severity is BuilderHealthSeverity.CRITICAL
            else "waiting_for_capacity",
            capacity_ratio if severity is BuilderHealthSeverity.CRITICAL else capacity_waiting,
            policy.capacity_waiting_critical_ratio
            if severity is BuilderHealthSeverity.CRITICAL
            else policy.capacity_waiting_degraded,
            capacity_component,
        )

    copy_waiting = max(0, int(health.waiting_for_copy_capacity))
    if copy_waiting >= policy.copy_waiting_degraded:
        copy_ratio, copy_component = _maximum_copy_wait_ratio(health.payload_copy_components)
        severity = (
            BuilderHealthSeverity.CRITICAL
            if copy_ratio >= policy.copy_waiting_critical_ratio
            else BuilderHealthSeverity.DEGRADED
        )
        add(
            BuilderHealthReason.PAYLOAD_COPY_CAPACITY_WAITING,
            severity,
            "maximum_waiting_to_limit_ratio"
            if severity is BuilderHealthSeverity.CRITICAL
            else "waiting_for_copy_capacity",
            copy_ratio if severity is BuilderHealthSeverity.CRITICAL else copy_waiting,
            policy.copy_waiting_critical_ratio
            if severity is BuilderHealthSeverity.CRITICAL
            else policy.copy_waiting_degraded,
            copy_component,
        )

    submission_ratio = _safe_ratio(health.pending_submissions, health.max_pending_submissions)
    if submission_ratio >= policy.submission_backlog_degraded_ratio:
        severity = (
            BuilderHealthSeverity.CRITICAL
            if submission_ratio >= policy.submission_backlog_critical_ratio
            else BuilderHealthSeverity.DEGRADED
        )
        add(
            BuilderHealthReason.SUBMISSION_BACKLOG_HIGH,
            severity,
            "submission_backlog_ratio",
            submission_ratio,
            policy.submission_backlog_critical_ratio
            if severity is BuilderHealthSeverity.CRITICAL
            else policy.submission_backlog_degraded_ratio,
        )

    observability_ratio, observability_component = _maximum_observability_ratio(
        health.observability_buffers
    )
    if observability_ratio >= policy.observability_backlog_degraded_ratio:
        severity = (
            BuilderHealthSeverity.CRITICAL
            if observability_ratio >= policy.observability_backlog_critical_ratio
            else BuilderHealthSeverity.DEGRADED
        )
        add(
            BuilderHealthReason.OBSERVABILITY_BACKLOG_HIGH,
            severity,
            "observability_backlog_ratio",
            observability_ratio,
            policy.observability_backlog_critical_ratio
            if severity is BuilderHealthSeverity.CRITICAL
            else policy.observability_backlog_degraded_ratio,
            observability_component,
        )

    stopped_workers = sum(
        1
        for stats in health.observability_buffers.values()
        if not bool(getattr(stats, "worker_alive", False))
        and not bool(getattr(stats, "closed", False))
    )
    if stopped_workers:
        add(
            BuilderHealthReason.OBSERVABILITY_WORKER_STOPPED,
            BuilderHealthSeverity.CRITICAL,
            "stopped_observability_workers",
            stopped_workers,
            0,
        )

    circuit_states = [
        str(getattr(getattr(snapshot, "state", ""), "value", getattr(snapshot, "state", "")))
        for snapshot in health.circuits.values()
    ]
    open_circuits = sum(state == "open" for state in circuit_states)
    if open_circuits:
        add(
            BuilderHealthReason.CIRCUIT_OPEN,
            BuilderHealthSeverity.DEGRADED,
            "open_circuits",
            open_circuits,
            0,
        )
    half_open_circuits = sum(state == "half_open" for state in circuit_states)
    if half_open_circuits:
        add(
            BuilderHealthReason.CIRCUIT_HALF_OPEN,
            BuilderHealthSeverity.DEGRADED,
            "half_open_circuits",
            half_open_circuits,
            0,
        )

    if health.payload_copy_shutdown_incomplete:
        add(
            BuilderHealthReason.PAYLOAD_COPY_SHUTDOWN_INCOMPLETE,
            BuilderHealthSeverity.CRITICAL,
            "payload_copy_shutdown_incomplete",
            True,
            False,
        )
    if health.observability_shutdown_incomplete:
        add(
            BuilderHealthReason.OBSERVABILITY_SHUTDOWN_INCOMPLETE,
            BuilderHealthSeverity.CRITICAL,
            "observability_shutdown_incomplete",
            True,
            False,
        )

    if previous is not None:
        counter_specs = (
            (
                "queue_timeout_count",
                BuilderHealthReason.QUEUE_TIMEOUTS_INCREASED,
            ),
            (
                "source_timeout_count",
                BuilderHealthReason.SOURCE_TIMEOUTS_INCREASED,
            ),
            (
                "deadline_exceeded_count",
                BuilderHealthReason.DEADLINES_EXCEEDED_INCREASED,
            ),
            (
                "revalidation_failure_count",
                BuilderHealthReason.REVALIDATION_FAILURES_INCREASED,
            ),
            (
                "payload_copy_failure_count",
                BuilderHealthReason.PAYLOAD_COPY_FAILURES_INCREASED,
            ),
            (
                "payload_copy_timeout_count",
                BuilderHealthReason.PAYLOAD_COPY_TIMEOUTS_INCREASED,
            ),
            (
                "payload_copy_shutdown_timeout_count",
                BuilderHealthReason.PAYLOAD_COPY_SHUTDOWN_TIMEOUTS_INCREASED,
            ),
            (
                "observability_dropped_count",
                BuilderHealthReason.OBSERVABILITY_DROPS_INCREASED,
            ),
            (
                "observability_shutdown_timeout_count",
                BuilderHealthReason.OBSERVABILITY_SHUTDOWN_TIMEOUTS_INCREASED,
            ),
            (
                "observability_failure_count",
                BuilderHealthReason.OBSERVABILITY_FAILURES_INCREASED,
            ),
        )
        for field_name, reason in counter_specs:
            current = max(0, int(getattr(health, field_name)))
            prior = max(0, int(getattr(previous, field_name)))
            delta = max(0, current - prior)
            if delta < policy.counter_delta_degraded:
                continue
            severity = (
                BuilderHealthSeverity.CRITICAL
                if delta >= policy.counter_delta_critical
                else BuilderHealthSeverity.DEGRADED
            )
            add(
                reason,
                severity,
                f"delta_{field_name}",
                delta,
                policy.counter_delta_critical
                if severity is BuilderHealthSeverity.CRITICAL
                else policy.counter_delta_degraded,
            )

    severity = BuilderHealthSeverity.HEALTHY
    if any(finding.severity is BuilderHealthSeverity.CRITICAL for finding in findings):
        severity = BuilderHealthSeverity.CRITICAL
    elif findings:
        severity = BuilderHealthSeverity.DEGRADED

    return BuilderHealthAssessment(
        severity=severity,
        findings=tuple(findings),
        baseline_used=previous is not None,
    )


def _safe_ratio(numerator: int, denominator: int | None) -> float:
    if denominator is None or int(denominator) <= 0:
        return 0.0
    return max(0.0, float(numerator) / float(denominator))


def _maximum_wait_ratio(capacity: Mapping[str, Any]) -> tuple[float, str]:
    maximum = 0.0
    component = ""
    for name, snapshot in capacity.items():
        ratio = _safe_ratio(
            int(getattr(snapshot, "waiting", 0)),
            int(getattr(snapshot, "limit", 0)),
        )
        if ratio > maximum:
            maximum = ratio
            component = str(name)
    return maximum, component


def _maximum_copy_wait_ratio(
    components: Mapping[str, PayloadCopyHealth],
) -> tuple[float, str]:
    maximum = 0.0
    component = ""
    for name, snapshot in components.items():
        ratio = _safe_ratio(snapshot.waiting_for_capacity, snapshot.max_concurrency)
        if ratio > maximum:
            maximum = ratio
            component = str(name)
    return maximum, component


def _maximum_observability_ratio(buffers: Mapping[str, Any]) -> tuple[float, str]:
    maximum = 0.0
    component = ""
    for name, stats in buffers.items():
        ratio = _safe_ratio(
            int(getattr(stats, "pending", 0)),
            int(getattr(stats, "max_pending", 0)),
        )
        if ratio > maximum:
            maximum = ratio
            component = str(name)
    return maximum, component


def _json_safe(value: Any, *, _seen: set[int] | None = None) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isnan(value):
            return "nan"
        return "infinity" if value > 0 else "-infinity"
    if isinstance(value, Enum):
        return _json_safe(value.value, _seen=_seen)

    seen = set() if _seen is None else _seen
    identity = id(value)
    if identity in seen:
        return {"type": _qualified_type_name(value), "cycle": True}

    if isinstance(value, Mapping):
        seen.add(identity)
        try:
            return {
                str(key): _json_safe(item, _seen=seen)
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            }
        finally:
            seen.remove(identity)

    if is_dataclass(value) and not isinstance(value, type):
        seen.add(identity)
        try:
            return {
                item.name: _json_safe(getattr(value, item.name), _seen=seen)
                for item in fields(value)
            }
        finally:
            seen.remove(identity)

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        seen.add(identity)
        try:
            return [_json_safe(item, _seen=seen) for item in value]
        finally:
            seen.remove(identity)

    if isinstance(value, (set, frozenset)):
        seen.add(identity)
        try:
            serialized = [_json_safe(item, _seen=seen) for item in value]
            return sorted(serialized, key=repr)
        finally:
            seen.remove(identity)

    return {"type": _qualified_type_name(value)}


def _qualified_type_name(value: Any) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"
