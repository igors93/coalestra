from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Generic, TypeVar

from coalestra.core.models import FreshnessPolicy, ResourceKey, SnapshotValue
from coalestra.core.protocols import (
    AsyncCache,
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

    Publications are monotonic by ``observed_at`` by default: an older event cannot overwrite a
    newer cached value. A fixed set of striped locks prevents races without retaining one lock per
    resource forever.
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
        update = ResourceUpdate(
            key=key,
            value=value,
            source=source,
            observed_at=observed_at,
            metadata=metadata or {},
        )
        return await self.publish_update(
            update,
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
        lock = self._lock_for(update.key)
        async with lock:
            now = self.clock.now()
            observed_at = now if update.observed_at is None else float(update.observed_at)
            lookup = await self.cache.get(
                update.key,
                now=now,
                policy=self._all_values_policy,
            )
            previous = lookup.value

            if not force and previous is not None:
                if previous.observed_at > observed_at:
                    return self._ignored_result(
                        PublishStatus.IGNORED_OLDER,
                        previous,
                        update,
                    )
                if previous.observed_at == observed_at and not replace_equal:
                    return self._ignored_result(
                        PublishStatus.IGNORED_DUPLICATE,
                        previous,
                        update,
                    )

            policy = self.policy_resolver.resolve(update.key)
            published = SnapshotValue(
                key=update.key,
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
            await self.cache.set(published)
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
                observed_at=observed_at,
                replaced=previous is not None,
                forced=force,
            )
            return PublishResult(
                status=PublishStatus.PUBLISHED,
                value=published,
                previous=previous,
            )

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
        completed = await asyncio.gather(
            *(
                self.publish_update(
                    update,
                    force=force,
                    replace_equal=replace_equal,
                )
                for update in unique.values()
            )
        )
        return MappingProxyType(dict(zip(unique, completed, strict=True)))

    async def invalidate(self, key: ResourceKey, *, reason: str = "") -> None:
        async with self._lock_for(key):
            await self.cache.invalidate(key)
        self.metrics.increment("resource_invalidation_total", resource=str(key))
        self.events.emit("resource_invalidated", resource=str(key), reason=reason)

    async def invalidate_many(
        self,
        keys: Collection[ResourceKey],
        *,
        reason: str = "",
    ) -> None:
        unique = tuple(dict.fromkeys(keys))
        await asyncio.gather(*(self.invalidate(key, reason=reason) for key in unique))

    def _ignored_result(
        self,
        status: PublishStatus,
        previous: SnapshotValue[Any],
        update: ResourceUpdate[Any],
    ) -> PublishResult:
        self.metrics.increment(
            "resource_publish_total",
            status=status.value,
            source=update.source,
            resource=str(update.key),
        )
        self.events.emit(
            "resource_publish_ignored",
            resource=str(update.key),
            source=update.source,
            status=status.value,
            observed_at=update.observed_at,
            cached_observed_at=previous.observed_at,
        )
        return PublishResult(status=status, value=previous, previous=previous)

    def _lock_for(self, key: ResourceKey) -> asyncio.Lock:
        return self._locks[hash(key) % len(self._locks)]
