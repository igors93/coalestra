from __future__ import annotations

import asyncio

from coalestra import AsyncMemoryCache, FreshnessPolicy, ResourceKey, SnapshotValue

KEY_A = ResourceKey("test", "value", "A")
KEY_B = ResourceKey("test", "value", "B")


def make_value(key: ResourceKey, *, observed_at: float) -> SnapshotValue[int]:
    return SnapshotValue(
        key=key,
        value=1,
        source="test",
        observed_at=observed_at,
        fetched_at=observed_at,
        age_seconds=0.0,
        stale=False,
        from_cache=False,
        latency_ms=1.0,
    )


def test_memory_cache_reports_fresh_and_stale_windows() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        await cache.set(make_value(KEY_A, observed_at=100.0))
        policy = FreshnessPolicy(ttl_seconds=5.0, max_stale_seconds=20.0)

        fresh = await cache.get(KEY_A, now=104.0, policy=policy)
        stale = await cache.get(KEY_A, now=110.0, policy=policy)
        expired = await cache.get(KEY_A, now=121.0, policy=policy)

        assert fresh.fresh is True
        assert fresh.usable_stale is True
        assert stale.fresh is False
        assert stale.usable_stale is True
        assert expired.fresh is False
        assert expired.usable_stale is False

    asyncio.run(scenario())


def test_memory_cache_evicts_least_recently_used_entry() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache(max_entries=2)
        policy = FreshnessPolicy(ttl_seconds=60.0, max_stale_seconds=60.0)
        await cache.set(make_value(KEY_A, observed_at=100.0))
        await cache.set(make_value(KEY_B, observed_at=100.0))

        # Touch A so B becomes the least-recently-used entry.
        await cache.get(KEY_A, now=101.0, policy=policy)
        key_c = ResourceKey("test", "value", "C")
        await cache.set(make_value(key_c, observed_at=100.0))

        assert (await cache.get(KEY_A, now=101.0, policy=policy)).value is not None
        assert (await cache.get(KEY_B, now=101.0, policy=policy)).value is None
        assert (await cache.get(key_c, now=101.0, policy=policy)).value is not None

    asyncio.run(scenario())
