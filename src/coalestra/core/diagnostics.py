from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from coalestra.core.keys import ResourceKey


@dataclass(frozen=True)
class SnapshotDiagnostics:
    """Consolidated acquisition diagnostics for one build or snapshot session."""

    duration_ms: float = 0.0
    requested_resources: int = 0
    resolved_resources: int = 0
    failed_resources: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    cache_batch_reads: int = 0
    cache_batch_writes: int = 0
    stale_values: int = 0
    coalesced_requests: int = 0
    source_calls: int = 0
    batch_calls: int = 0
    derived_calls: int = 0
    refresh_scheduled: int = 0
    refresh_completed: int = 0
    refresh_failed: int = 0
    observation_skew_ms: float = 0.0
    source_calls_by_source: Mapping[str, int] = field(default_factory=dict)
    source_latency_ms_by_source: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_calls_by_source",
            MappingProxyType(dict(self.source_calls_by_source)),
        )
        object.__setattr__(
            self,
            "source_latency_ms_by_source",
            MappingProxyType(dict(self.source_latency_ms_by_source)),
        )


@dataclass
class DiagnosticsCollector:
    """Mutable per-session accumulator used internally by the orchestrator."""

    started_monotonic: float
    requested_keys: set[ResourceKey] = field(default_factory=set)
    cache_hits: int = 0
    cache_misses: int = 0
    cache_batch_reads: int = 0
    cache_batch_writes: int = 0
    stale_values: int = 0
    coalesced_requests: int = 0
    source_calls: int = 0
    batch_calls: int = 0
    derived_calls: int = 0
    refresh_scheduled: int = 0
    refresh_completed: int = 0
    refresh_failed: int = 0
    source_calls_by_source: Counter[str] = field(default_factory=Counter)
    source_latency_ms_by_source: defaultdict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )

    def record_requested(self, keys: tuple[ResourceKey, ...]) -> None:
        self.requested_keys.update(keys)

    def record_source_call(self, source: str, *, kind: str) -> None:
        self.source_calls += 1
        self.source_calls_by_source[source] += 1
        if kind == "batch":
            self.batch_calls += 1
        elif kind == "derived":
            self.derived_calls += 1

    def record_source_latency(self, source: str, latency_ms: float) -> None:
        self.source_latency_ms_by_source[source] += max(0.0, float(latency_ms))

    def snapshot(
        self,
        *,
        now_monotonic: float,
        resolved_resources: int,
        failed_resources: int,
        observed_at_values: tuple[float, ...],
    ) -> SnapshotDiagnostics:
        skew_ms = 0.0
        if len(observed_at_values) > 1:
            skew_ms = max(0.0, (max(observed_at_values) - min(observed_at_values)) * 1000)
        return SnapshotDiagnostics(
            duration_ms=max(0.0, (now_monotonic - self.started_monotonic) * 1000),
            requested_resources=len(self.requested_keys),
            resolved_resources=resolved_resources,
            failed_resources=failed_resources,
            cache_hits=self.cache_hits,
            cache_misses=self.cache_misses,
            cache_batch_reads=self.cache_batch_reads,
            cache_batch_writes=self.cache_batch_writes,
            stale_values=self.stale_values,
            coalesced_requests=self.coalesced_requests,
            source_calls=self.source_calls,
            batch_calls=self.batch_calls,
            derived_calls=self.derived_calls,
            refresh_scheduled=self.refresh_scheduled,
            refresh_completed=self.refresh_completed,
            refresh_failed=self.refresh_failed,
            observation_skew_ms=skew_ms,
            source_calls_by_source=self.source_calls_by_source,
            source_latency_ms_by_source=self.source_latency_ms_by_source,
        )
