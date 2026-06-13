from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any, Protocol, runtime_checkable

from coalestra.core.keys import ResourceKey
from coalestra.core.models import (
    CacheLookup,
    FetchContext,
    FreshnessPolicy,
    Snapshot,
    SnapshotValue,
    SourcePayload,
)


class Clock(Protocol):
    def now(self) -> float: ...

    def monotonic(self) -> float: ...


@runtime_checkable
class SourceBase(Protocol):
    """Common metadata exposed by every Coalestra source."""

    name: str
    priority: int
    timeout_seconds: float | None

    def supports(self, key: ResourceKey) -> bool: ...


@runtime_checkable
class SnapshotSource(SourceBase, Protocol):
    """Read-only source capable of resolving one resource at a time."""

    async def fetch(self, key: ResourceKey, context: FetchContext) -> SourcePayload[Any]: ...


@runtime_checkable
class BatchSnapshotSource(SourceBase, Protocol):
    """Source capable of resolving several resources with one operation."""

    async def fetch_many(
        self,
        keys: Collection[ResourceKey],
        context: FetchContext,
    ) -> Mapping[ResourceKey, SourcePayload[Any]]: ...


@runtime_checkable
class DerivedSource(SourceBase, Protocol):
    """Source that computes a resource from other Coalestra resources."""

    def dependencies(self, key: ResourceKey) -> Collection[ResourceKey]: ...

    async def derive(
        self,
        key: ResourceKey,
        dependencies: Snapshot,
        context: FetchContext,
    ) -> SourcePayload[Any]: ...


@runtime_checkable
class ConcurrencyLimitedSource(Protocol):
    """Optional source capability declaring an independent concurrency ceiling."""

    max_concurrency: int | None


@runtime_checkable
class ResilienceConfiguredSource(Protocol):
    """Optional source capability declaring source-local resilience behavior."""

    resilience_policy: Any


Source = SnapshotSource | BatchSnapshotSource | DerivedSource


class FreshnessPolicyProvider(Protocol):
    """Resolve freshness semantics for a resource."""

    def resolve(self, key: ResourceKey) -> FreshnessPolicy: ...


class AsyncCache(Protocol):
    """Minimal cache contract required by SnapshotBuilder.

    Existing custom caches only need the original single-key methods. Implement
    :class:`BatchAsyncCache` to let the builder perform one cache operation for a complete set of
    resources.
    """

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


@runtime_checkable
class BatchAsyncCache(Protocol):
    """Optional cache capability for efficient multi-key operations."""

    async def get_many(
        self,
        keys: Collection[ResourceKey],
        *,
        now: float,
        policies: Mapping[ResourceKey, FreshnessPolicy],
    ) -> Mapping[ResourceKey, CacheLookup]: ...

    async def set_many(self, values: Collection[SnapshotValue[Any]]) -> None: ...

    async def invalidate_many(self, keys: Collection[ResourceKey]) -> None: ...


class EventSink(Protocol):
    def emit(self, event_type: str, **payload: Any) -> None: ...


class MetricsSink(Protocol):
    def increment(self, metric: str, value: int = 1, **labels: str) -> None: ...

    def observe(self, metric: str, value: float, **labels: str) -> None: ...
