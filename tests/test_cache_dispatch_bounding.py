from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from typing import Any

from coalestra import (
    CacheLookup,
    CacheWriteResult,
    CacheWriteStatus,
    CallableBatchSource,
    FreshnessPolicy,
    ResourceKey,
    ResourceUpdate,
    SnapshotBuilder,
    SnapshotValue,
)


class OperationProbe:
    """Track active and peak operations without relying on wall-clock sleeps."""

    def __init__(self) -> None:
        self.active: dict[str, int] = {}
        self.peak: dict[str, int] = {}

    @asynccontextmanager
    async def track(self, operation: str) -> AsyncIterator[None]:
        active = self.active.get(operation, 0) + 1
        self.active[operation] = active
        self.peak[operation] = max(self.peak.get(operation, 0), active)
        try:
            # Yield once so every scheduled worker can enter the operation.
            await asyncio.sleep(0)
            yield
        finally:
            self.active[operation] -= 1


class InstrumentedSingleKeyCache:
    """Minimal non-batch cache that records fallback-operation concurrency."""

    def __init__(self) -> None:
        self.values: dict[ResourceKey, SnapshotValue[Any]] = {}
        self.probe = OperationProbe()

    async def get(
        self,
        key: ResourceKey,
        *,
        now: float,
        policy: FreshnessPolicy,
    ) -> CacheLookup:
        async with self.probe.track("get"):
            value = self.values.get(key)
            if value is None:
                return CacheLookup(None, False, False)
            age = max(0.0, now - value.observed_at)
            return CacheLookup(
                value,
                age <= policy.ttl_seconds,
                age <= policy.max_stale_seconds,
                age,
            )

    async def set(self, value: SnapshotValue[Any]) -> None:
        async with self.probe.track("set"):
            self.values[value.key] = value

    async def invalidate(self, key: ResourceKey) -> None:
        async with self.probe.track("invalidate"):
            self.values.pop(key, None)

    async def clear(self) -> None:
        self.values.clear()


class InstrumentedAtomicCache(InstrumentedSingleKeyCache):
    """Single-key atomic cache used to exercise publisher fallback writes."""

    async def set_if_newer(
        self,
        value: SnapshotValue[Any],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> CacheWriteResult:
        async with self.probe.track("set_if_newer"):
            previous = self.values.get(value.key)
            if force or previous is None:
                self.values[value.key] = value
                return CacheWriteResult(CacheWriteStatus.STORED, value, previous)
            if value.authority_rank < previous.authority_rank:
                return CacheWriteResult(
                    CacheWriteStatus.IGNORED_LOWER_AUTHORITY,
                    previous,
                    previous,
                )
            if value.authority_rank > previous.authority_rank:
                self.values[value.key] = value
                return CacheWriteResult(CacheWriteStatus.STORED, value, previous)
            if value.observed_at < previous.observed_at:
                return CacheWriteResult(
                    CacheWriteStatus.IGNORED_OLDER,
                    previous,
                    previous,
                )
            if value.observed_at == previous.observed_at and not replace_equal:
                return CacheWriteResult(
                    CacheWriteStatus.IGNORED_DUPLICATE,
                    previous,
                    previous,
                )
            self.values[value.key] = value
            return CacheWriteResult(CacheWriteStatus.STORED, value, previous)


def resource_keys(count: int) -> tuple[ResourceKey, ...]:
    return tuple(ResourceKey("test", "value", str(index)) for index in range(count))


def source_for(keys: Collection[ResourceKey]) -> CallableBatchSource:
    supported = frozenset(keys)
    return CallableBatchSource(
        name="batch-source",
        priority=1,
        supports=lambda key: key in supported,
        fetcher=lambda requested, _context: {key: f"value:{key.subject}" for key in requested},
    )


def test_builder_bounds_single_key_cache_reads_and_writes() -> None:
    async def scenario() -> None:
        keys = resource_keys(60)
        cache = InstrumentedSingleKeyCache()
        builder = SnapshotBuilder(
            [source_for(keys)],
            cache=cache,
            max_concurrency=12,
            max_pending_tasks=4,
            default_policy=FreshnessPolicy(60.0, 60.0),
        )

        first = await builder.build(keys)
        second = await builder.build(keys)

        assert tuple(first.resources) == keys
        assert tuple(second.resources) == keys
        assert cache.probe.peak["get"] == 4
        assert cache.probe.peak["set"] == 4

    asyncio.run(scenario())


def test_builder_bounds_dependency_cleanup_for_single_key_cache() -> None:
    async def scenario() -> None:
        keys = resource_keys(48)
        cache = InstrumentedSingleKeyCache()
        missing_dependency = ResourceKey("test", "dependency", "missing")
        for key in keys:
            cache.values[key] = SnapshotValue(
                key=key,
                value="stale-derived",
                source="derived",
                observed_at=1.0,
                fetched_at=1.0,
                age_seconds=0.0,
                stale=False,
                from_cache=False,
                latency_ms=0.0,
                dependency_versions={missing_dependency: "missing-version"},
            )

        builder = SnapshotBuilder(
            [source_for(keys)],
            cache=cache,
            max_concurrency=10,
            max_pending_tasks=3,
            default_policy=FreshnessPolicy(float("inf"), float("inf")),
        )

        snapshot = await builder.build(keys)

        assert tuple(snapshot.resources) == keys
        assert cache.probe.peak["invalidate"] == 3
        assert all(snapshot.value(key, str) == f"value:{key.subject}" for key in keys)

    asyncio.run(scenario())


def test_publisher_bounds_legacy_cache_fallback_operations() -> None:
    async def scenario() -> None:
        keys = resource_keys(45)
        cache = InstrumentedSingleKeyCache()
        builder = SnapshotBuilder(
            [source_for(keys)],
            cache=cache,
            max_concurrency=12,
            max_pending_tasks=5,
        )
        updates = tuple(
            ResourceUpdate(key, f"published:{key.subject}", source="stream") for key in keys
        )

        results = await builder.publisher.publish_many(updates)
        await builder.publisher.invalidate_many(keys, reason="test")

        assert tuple(results) == keys
        assert cache.probe.peak["get"] == 5
        assert cache.probe.peak["set"] == 5
        assert cache.probe.peak["invalidate"] == 5

    asyncio.run(scenario())


def test_publisher_bounds_single_key_atomic_cache_writes() -> None:
    async def scenario() -> None:
        keys = resource_keys(42)
        cache = InstrumentedAtomicCache()
        builder = SnapshotBuilder(
            [source_for(keys)],
            cache=cache,
            max_concurrency=10,
            max_pending_tasks=4,
        )
        updates = tuple(
            ResourceUpdate(key, f"published:{key.subject}", source="stream") for key in keys
        )

        results = await builder.publisher.publish_many(updates)

        assert tuple(results) == keys
        assert cache.probe.peak["get"] == 4
        assert cache.probe.peak["set_if_newer"] == 4
        assert "set" not in cache.probe.peak

    asyncio.run(scenario())


def test_publisher_rejects_invalid_pending_task_limit() -> None:
    keys = resource_keys(1)
    builder = SnapshotBuilder([source_for(keys)])

    try:
        type(builder.publisher)(
            cache=builder.cache,
            clock=builder.clock,
            policy_resolver=builder.policy_resolver,
            metrics=builder.metrics,
            events=builder.events,
            max_pending_tasks=0,
        )
    except ValueError as error:
        assert str(error) == "max_pending_tasks must be at least 1"
    else:
        raise AssertionError("expected invalid max_pending_tasks to be rejected")


def test_cache_fallback_cancels_workers_when_one_operation_fails() -> None:
    class FailingReadCache(InstrumentedSingleKeyCache):
        def __init__(self) -> None:
            super().__init__()
            self.started = 0
            self.all_workers_started = asyncio.Event()
            self.cancelled = 0

        async def get(
            self,
            key: ResourceKey,
            *,
            now: float,
            policy: FreshnessPolicy,
        ) -> CacheLookup:
            del now, policy
            self.started += 1
            if self.started == 4:
                self.all_workers_started.set()

            if key.subject == "0":
                await self.all_workers_started.wait()
                raise RuntimeError("cache read failed")

            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled += 1
                raise

    async def scenario() -> None:
        keys = resource_keys(20)
        cache = FailingReadCache()
        builder = SnapshotBuilder(
            [source_for(keys)],
            cache=cache,
            max_concurrency=8,
            max_pending_tasks=4,
        )

        try:
            await builder.build(keys)
        except RuntimeError as error:
            assert str(error) == "cache read failed"
        else:
            raise AssertionError("expected cache read failure")

        assert cache.started == 4
        assert cache.cancelled == 3

    asyncio.run(scenario())
