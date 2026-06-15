from __future__ import annotations

import asyncio

from coalestra import (
    AsyncMemoryCache,
    CacheWriteStatus,
    FreshnessPolicy,
    ResourceKey,
    SnapshotValue,
)

KEY_A = ResourceKey("test", "value", "A")
KEY_B = ResourceKey("test", "value", "B")
KEY_C = ResourceKey("other", "value", "C")


def value(key: ResourceKey, observed_at: float) -> SnapshotValue[int]:
    return SnapshotValue(
        key=key,
        value=1,
        source="test",
        observed_at=observed_at,
        fetched_at=observed_at,
        age_seconds=0.0,
        stale=False,
        from_cache=False,
        latency_ms=0.0,
    )


def text_value(
    key: ResourceKey,
    payload: str,
    observed_at: float,
) -> SnapshotValue[str]:
    return SnapshotValue(
        key=key,
        value=payload,
        source="test",
        observed_at=observed_at,
        fetched_at=observed_at,
        age_seconds=0.0,
        stale=False,
        from_cache=False,
        latency_ms=0.0,
    )


def test_batch_cache_operations_expiry_and_stats() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache(max_entries=2)
        policy = FreshnessPolicy(5.0, 20.0)
        await cache.set_many((value(KEY_A, 100.0), value(KEY_B, 100.0)))

        lookups = await cache.get_many(
            (KEY_A, KEY_B),
            now=110.0,
            policies={KEY_A: policy, KEY_B: policy},
        )
        assert lookups[KEY_A].fresh is False
        assert lookups[KEY_A].usable_stale is True
        assert lookups[KEY_A].age_seconds == 10.0

        expired = await cache.get_many(
            (KEY_A,),
            now=121.0,
            policies={KEY_A: policy},
        )
        assert expired[KEY_A].value is None

        await cache.set(value(KEY_C, 121.0))
        stats = await cache.stats()
        assert stats.size == 2
        assert stats.sets == 3
        assert stats.expirations == 1
        assert stats.hits == 2
        assert stats.misses == 1
        assert stats.stale_hits == 2

    asyncio.run(scenario())


def test_cache_namespace_invalidation_and_lru_eviction() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache(max_entries=2)
        policy = FreshnessPolicy(100.0, 100.0)
        await cache.set_many((value(KEY_A, 1.0), value(KEY_B, 1.0)))
        await cache.get(KEY_A, now=2.0, policy=policy)
        await cache.set(value(KEY_C, 2.0))

        assert (await cache.get(KEY_B, now=2.0, policy=policy)).value is None
        removed = await cache.invalidate_namespace("test")
        assert removed == (KEY_A,)
        stats = await cache.stats()
        assert stats.evictions == 1
        assert stats.invalidations == 1

    asyncio.run(scenario())


def test_memory_cache_writes_are_atomic_and_monotonic() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        policy = FreshnessPolicy(1000.0, 1000.0)

        newest = text_value(KEY_A, "new", 200.0)
        older = text_value(KEY_A, "old", 100.0)
        duplicate = text_value(KEY_A, "duplicate", 200.0)

        stored = await cache.set_if_newer(newest)
        ignored_older = await cache.set_if_newer(older)
        ignored_duplicate = await cache.set_if_newer(duplicate)

        lookup = await cache.get(KEY_A, now=200.0, policy=policy)

        assert stored.status is CacheWriteStatus.STORED
        assert ignored_older.status is CacheWriteStatus.IGNORED_OLDER
        assert ignored_duplicate.status is CacheWriteStatus.IGNORED_DUPLICATE
        assert lookup.value is newest
        assert (await cache.stats()).sets == 1

        forced = await cache.set_if_newer(older, force=True)
        assert forced.status is CacheWriteStatus.STORED
        assert (await cache.get(KEY_A, now=200.0, policy=policy)).value is older

    asyncio.run(scenario())


def test_batch_write_selects_newest_duplicate_regardless_of_input_order() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        policy = FreshnessPolicy(1000.0, 1000.0)
        older = text_value(KEY_A, "old", 100.0)
        newest = text_value(KEY_A, "new", 200.0)

        await cache.set_many_if_newer((newest, older))
        first = await cache.get(KEY_A, now=200.0, policy=policy)
        assert first.value is newest

        await cache.clear()
        await cache.set_many_if_newer((older, newest))
        second = await cache.get(KEY_A, now=200.0, policy=policy)
        assert second.value is newest

    asyncio.run(scenario())


def test_builder_remains_compatible_with_single_key_custom_cache() -> None:
    from coalestra import CallableSource, SnapshotBuilder

    class SingleKeyCache:
        def __init__(self) -> None:
            self.values: dict[ResourceKey, SnapshotValue[object]] = {}

        async def get(self, key, *, now, policy):
            cached = self.values.get(key)
            if cached is None:
                from coalestra import CacheLookup

                return CacheLookup(None, False, False)
            age = max(0.0, now - cached.observed_at)
            from coalestra import CacheLookup

            return CacheLookup(
                cached, age <= policy.ttl_seconds, age <= policy.max_stale_seconds, age
            )

        async def set(self, item):
            self.values[item.key] = item

        async def invalidate(self, key):
            self.values.pop(key, None)

        async def clear(self):
            self.values.clear()

    async def scenario() -> None:
        cache = SingleKeyCache()
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: 7,
                )
            ],
            cache=cache,
            default_policy=FreshnessPolicy(60.0, 60.0),
        )
        first = await builder.build([KEY_A, KEY_B])
        second = await builder.build([KEY_A, KEY_B])

        assert first.diagnostics.cache_batch_reads == 0
        assert first.diagnostics.cache_batch_writes == 0
        assert second.diagnostics.cache_hits == 2

    asyncio.run(scenario())
