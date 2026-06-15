from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any, Protocol, runtime_checkable

from coalestra.core.authority import SourceAuthorityPolicy
from coalestra.core.keys import ResourceKey
from coalestra.core.models import (
    CacheLookup,
    CacheWriteResult,
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
class BatchSizedSource(Protocol):
    """Optional batch-source capability limiting keys per remote call."""

    max_batch_size: int | None


@runtime_checkable
class ResilienceConfiguredSource(Protocol):
    """Optional source capability declaring source-local resilience behavior."""

    resilience_policy: Any


Source = SnapshotSource | BatchSnapshotSource | DerivedSource


class FreshnessPolicyProvider(Protocol):
    """Resolve freshness semantics for a resource."""

    def resolve(self, key: ResourceKey) -> FreshnessPolicy: ...


class AuthorityPolicyProvider(Protocol):
    """Resolve source-authority semantics for a resource."""

    def resolve(self, key: ResourceKey) -> SourceAuthorityPolicy: ...

    def rank_for(self, key: ResourceKey, source: str) -> int: ...


@runtime_checkable
class AuthorityAwareCache(Protocol):
    """Cache capability declaring atomic source-authority validation."""

    validates_source_authority: bool


class AsyncCache(Protocol):
    """Minimal cache contract required by SnapshotBuilder.

    ``set`` implementations should compare ``authority_rank`` before ``observed_at`` and write
    atomically. Higher-authority revisions win; equal-authority revisions remain monotonic by
    observation time. Legacy caches remain structurally compatible while authority is disabled.
    Implement :class:`AtomicAsyncCache` to expose authoritative write results and force options.
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
    """Optional cache capability for efficient monotonic multi-key operations.

    ``set_many`` should perform each timestamp comparison and write atomically in the backend.
    """

    async def get_many(
        self,
        keys: Collection[ResourceKey],
        *,
        now: float,
        policies: Mapping[ResourceKey, FreshnessPolicy],
    ) -> Mapping[ResourceKey, CacheLookup]: ...

    async def set_many(self, values: Collection[SnapshotValue[Any]]) -> None: ...

    async def invalidate_many(self, keys: Collection[ResourceKey]) -> None: ...


@runtime_checkable
class AtomicAsyncCache(AsyncCache, Protocol):
    """Optional capability for atomic monotonic single-resource writes."""

    async def set_if_newer(
        self,
        value: SnapshotValue[Any],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> CacheWriteResult: ...


@runtime_checkable
class BatchAtomicAsyncCache(BatchAsyncCache, Protocol):
    """Optional capability for atomic monotonic multi-resource writes."""

    async def set_many_if_newer(
        self,
        values: Collection[SnapshotValue[Any]],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> Mapping[ResourceKey, CacheWriteResult]: ...


class EventSink(Protocol):
    def emit(self, event_type: str, **payload: Any) -> None: ...


class MetricsSink(Protocol):
    def increment(self, metric: str, value: int = 1, **labels: str) -> None: ...

    def observe(self, metric: str, value: float, **labels: str) -> None: ...
