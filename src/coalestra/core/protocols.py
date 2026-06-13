from __future__ import annotations

from typing import Any, Protocol

from coalestra.core.models import (
    CacheLookup,
    FetchContext,
    FreshnessPolicy,
    ResourceKey,
    SnapshotValue,
    SourcePayload,
)


class Clock(Protocol):
    def now(self) -> float: ...

    def monotonic(self) -> float: ...


class SnapshotSource(Protocol):
    """Read-only source capable of resolving selected resource keys."""

    name: str
    priority: int
    timeout_seconds: float | None

    def supports(self, key: ResourceKey) -> bool: ...

    async def fetch(self, key: ResourceKey, context: FetchContext) -> SourcePayload[Any]: ...


class AsyncCache(Protocol):
    """Minimal cache contract required by SnapshotBuilder."""

    async def get(
        self,
        key: ResourceKey,
        *,
        now: float,
        policy: FreshnessPolicy,
    ) -> CacheLookup: ...

    async def set(self, value: SnapshotValue[Any]) -> None: ...

    async def invalidate(self, key: ResourceKey) -> None: ...

    async def clear(self) -> None: ...


class EventSink(Protocol):
    def emit(self, event_type: str, **payload: Any) -> None: ...


class MetricsSink(Protocol):
    def increment(self, metric: str, value: int = 1, **labels: str) -> None: ...

    def observe(self, metric: str, value: float, **labels: str) -> None: ...
