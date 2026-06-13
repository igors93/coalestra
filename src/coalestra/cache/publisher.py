from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Collection, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Generic, TypeVar, cast

from coalestra.core.keys import ResourceKey
from coalestra.core.models import FreshnessPolicy, SnapshotValue
from coalestra.core.protocols import (
    AsyncCache,
    BatchAsyncCache,
    Clock,
    EventSink,
    FreshnessPolicyProvider,
    MetricsSink,
)

T = TypeVar("T")


class PublishStatus(str, Enum):
    PUBLISHED = "published"
    IGNORED_OLDER = "ignored_older"
    IGNORED_DUPLICATE = "ignored_duplicate"


@dataclass(frozen=True)
class ResourceUpdate(Generic[T]):
    """One event-driven resource update ready to enter Coalestra's read cache."""

    key: ResourceKey
    value: T
    source: str
    observed_at: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        normalized_source = str(self.source or "").strip()
        if not normalized_source:
            raise ValueError("source name cannot be empty")
        object.__setattr__(self, "source", normalized_source)
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class PublishResult:
    """Outcome of one monotonic cache publication."""

    status: PublishStatus
    value: SnapshotValue[Any]
    previous: SnapshotValue[Any] | None = None

    @property
    def published(self) -> bool:
        return self.status is PublishStatus.PUBLISHED


class ResourcePublisher:
    """Publish event-stream or in-process state directly into a Coalestra cache.

    Publications are monotonic by ``observed_at`` by default. Bulk publication acquires striped
    locks in a stable order and uses cache batch operations when available.
    """

    def __init__(
        self,
        *,
        cache: AsyncCache,
        clock: Clock,
        policy_resolver: FreshnessPolicyProvider,
        metrics: MetricsSink,
        events: EventSink,
        lock_stripes: int = 64,
    ) -> None:
        if lock_stripes < 1:
            raise ValueError("lock_stripes must be at least 1")
        self.cache = cache
        self.clock = clock
        self.policy_resolver = policy_resolver
        self.metrics = metrics
        self.events = events
        self._locks = tuple(asyncio.Lock() for _ in range(lock_stripes))
        self._all_values_policy = FreshnessPolicy(
            ttl_seconds=float("inf"),
            max_stale_seconds=float("inf"),
        )

    async def publish(
        self,
        key: ResourceKey,
        value: Any,
        *,
        source: str,
        observed_at: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        force: bool = False,
        replace_equal: bool = False,
    ) -> PublishResult:
        return await self.publish_update(
            ResourceUpdate(
                key=key,
                value=value,
                source=source,
                observed_at=observed_at,
                metadata=metadata or {},
            ),
            force=force,
            replace_equal=replace_equal,
        )

    async def publish_update(
        self,
        update: ResourceUpdate[Any],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> PublishResult:
        results = await self.publish_many(
            (update,),
            force=force,
            replace_equal=replace_equal,
        )
        return results[update.key]

    async def publish_many(
        self,
        updates: Collection[ResourceUpdate[Any]],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> Mapping[ResourceKey, PublishResult]:
        unique: dict[ResourceKey, ResourceUpdate[Any]] = {}
        for update in updates:
            unique[update.key] = update
        if not unique:
            return MappingProxyType({})

        keys = tuple(unique)
        async with self._locked_keys(keys):
            now = self.clock.now()
            previous_values = await self._get_existing(keys, now=now)
            results: dict[ResourceKey, PublishResult] = {}
            pending_writes: list[SnapshotValue[Any]] = []

            for key, update in unique.items():
                observed_at = now if update.observed_at is None else float(update.observed_at)
                previous = previous_values.get(key)
                ignored_status = self._ignored_status(
                    previous,
                    observed_at=observed_at,
                    force=force,
                    replace_equal=replace_equal,
                )
                if ignored_status is not None and previous is not None:
                    result = PublishResult(
                        status=ignored_status,
                        value=previous,
                        previous=previous,
                    )
                    results[key] = result
                    self._record_ignored(result, update)
                    continue

                policy = self.policy_resolver.resolve(key)
                published = SnapshotValue(
                    key=key,
                    value=update.value,
                    source=update.source,
                    observed_at=observed_at,
                    fetched_at=now,
                    age_seconds=max(0.0, now - observed_at),
                    stale=max(0.0, now - observed_at) > policy.ttl_seconds,
                    from_cache=False,
                    latency_ms=0.0,
                    attempts=0,
                    metadata={**update.metadata, "published": True},
                )
                pending_writes.append(published)
                results[key] = PublishResult(
                    status=PublishStatus.PUBLISHED,
                    value=published,
                    previous=previous,
                )

            await self._set_many(pending_writes)
            for key, result in results.items():
                if result.published:
                    self._record_published(result, unique[key], force=force)
            return MappingProxyType(results)

    async def invalidate(self, key: ResourceKey, *, reason: str = "") -> None:
        await self.invalidate_many((key,), reason=reason)

    async def invalidate_many(
        self,
        keys: Collection[ResourceKey],
        *,
        reason: str = "",
    ) -> None:
        unique = tuple(dict.fromkeys(keys))
        if not unique:
            return
        async with self._locked_keys(unique):
            if isinstance(self.cache, BatchAsyncCache):
                await self.cache.invalidate_many(unique)
            else:
                await asyncio.gather(*(self.cache.invalidate(key) for key in unique))
        for key in unique:
            self.metrics.increment("resource_invalidation_total", resource=str(key))
            self.events.emit("resource_invalidated", resource=str(key), reason=reason)

    async def _get_existing(
        self,
        keys: tuple[ResourceKey, ...],
        *,
        now: float,
    ) -> dict[ResourceKey, SnapshotValue[Any] | None]:
        policies = dict.fromkeys(keys, self._all_values_policy)
        if isinstance(self.cache, BatchAsyncCache):
            lookups = await self.cache.get_many(keys, now=now, policies=policies)
        else:
            completed = await asyncio.gather(
                *(self.cache.get(key, now=now, policy=self._all_values_policy) for key in keys)
            )
            lookups = dict(zip(keys, completed, strict=True))
        return {key: lookups[key].value for key in keys}

    async def _set_many(self, values: Collection[SnapshotValue[Any]]) -> None:
        if not values:
            return
        if isinstance(self.cache, BatchAsyncCache):
            await self.cache.set_many(values)
        else:
            await asyncio.gather(*(self.cache.set(value) for value in values))

    @staticmethod
    def _ignored_status(
        previous: SnapshotValue[Any] | None,
        *,
        observed_at: float,
        force: bool,
        replace_equal: bool,
    ) -> PublishStatus | None:
        if force or previous is None:
            return None
        if previous.observed_at > observed_at:
            return PublishStatus.IGNORED_OLDER
        if previous.observed_at == observed_at and not replace_equal:
            return PublishStatus.IGNORED_DUPLICATE
        return None

    def _record_published(
        self,
        result: PublishResult,
        update: ResourceUpdate[Any],
        *,
        force: bool,
    ) -> None:
        self.metrics.increment(
            "resource_publish_total",
            status=PublishStatus.PUBLISHED.value,
            source=update.source,
            resource=str(update.key),
        )
        self.events.emit(
            "resource_published",
            resource=str(update.key),
            source=update.source,
            observed_at=result.value.observed_at,
            replaced=result.previous is not None,
            forced=force,
        )

    def _record_ignored(
        self,
        result: PublishResult,
        update: ResourceUpdate[Any],
    ) -> None:
        previous = cast(SnapshotValue[Any], result.previous)
        self.metrics.increment(
            "resource_publish_total",
            status=result.status.value,
            source=update.source,
            resource=str(update.key),
        )
        self.events.emit(
            "resource_publish_ignored",
            resource=str(update.key),
            source=update.source,
            status=result.status.value,
            observed_at=update.observed_at,
            cached_observed_at=previous.observed_at,
        )

    @asynccontextmanager
    async def _locked_keys(self, keys: Collection[ResourceKey]) -> AsyncIterator[None]:
        indexes = sorted({hash(key) % len(self._locks) for key in keys})
        acquired: list[asyncio.Lock] = []
        try:
            for index in indexes:
                lock = self._locks[index]
                await lock.acquire()
                acquired.append(lock)
            yield
        finally:
            for lock in reversed(acquired):
                lock.release()
