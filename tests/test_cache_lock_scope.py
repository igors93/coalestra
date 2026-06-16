from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import Any

from coalestra import (
    AsyncMemoryCache,
    CacheWriteStatus,
    FreshnessPolicy,
    ResourceKey,
    SnapshotValue,
)

KEY = ResourceKey("test", "lock-scope")
POLICY = FreshnessPolicy(60.0, 60.0)


def run(coro):
    return asyncio.run(coro)


def snapshot_value(
    payload: Any,
    *,
    observed_at: float = 100.0,
    authority_rank: int = 0,
) -> SnapshotValue[Any]:
    return SnapshotValue(
        key=KEY,
        value=payload,
        source="test",
        observed_at=observed_at,
        fetched_at=observed_at,
        age_seconds=0.0,
        stale=False,
        from_cache=False,
        latency_ms=0.0,
        authority_rank=authority_rank,
        metadata={"nested": {"value": 1}},
    )


def test_memory_cache_releases_lock_before_cloning_read_results() -> None:
    async def scenario() -> None:
        lock_states: list[bool] = []
        record_lock_state = False
        cache: AsyncMemoryCache

        def copier(value: Any) -> Any:
            if record_lock_state:
                lock_states.append(cache._lock.locked())
            return deepcopy(value)

        cache = AsyncMemoryCache(payload_copier=copier)
        await cache.set(snapshot_value({"items": [1]}))

        record_lock_state = True
        lookup = await cache.get(KEY, now=100.0, policy=POLICY)

        assert lookup.value is not None
        assert lookup.value.value == {"items": [1]}
        assert lock_states
        assert not any(lock_states)

    run(scenario())


def test_memory_cache_releases_lock_before_storage_and_result_copies() -> None:
    async def scenario() -> None:
        lock_states: list[bool] = []
        cache: AsyncMemoryCache

        def copier(value: Any) -> Any:
            lock_states.append(cache._lock.locked())
            return deepcopy(value)

        cache = AsyncMemoryCache(payload_copier=copier)
        result = await cache.set_if_newer(snapshot_value({"items": [1]}))

        assert result.status is CacheWriteStatus.STORED
        assert lock_states
        assert not any(lock_states)

    run(scenario())


def test_memory_cache_does_not_copy_a_rejected_candidate() -> None:
    class NonCopyable:
        def __deepcopy__(self, _memo):
            raise TypeError("copy disabled")

    async def scenario() -> None:
        cache = AsyncMemoryCache()
        await cache.set_if_newer(snapshot_value("winner", authority_rank=100))

        result = await cache.set_if_newer(
            snapshot_value(NonCopyable(), observed_at=200.0, authority_rank=10)
        )

        assert result.status is CacheWriteStatus.IGNORED_LOWER_AUTHORITY
        assert result.value.value == "winner"

    run(scenario())


def test_memory_cache_rechecks_write_status_after_preparing_storage_copy() -> None:
    async def scenario() -> None:
        cache: AsyncMemoryCache
        winner = snapshot_value("winner", observed_at=90.0, authority_rank=100)
        injected = False

        def copier(value: Any) -> Any:
            nonlocal injected
            if value == "candidate" and not injected:
                injected = True
                cache._entries[KEY] = winner
            return deepcopy(value)

        cache = AsyncMemoryCache(payload_copier=copier)
        result = await cache.set_if_newer(
            snapshot_value("candidate", observed_at=100.0, authority_rank=10)
        )
        lookup = await cache.get(KEY, now=100.0, policy=POLICY)

        assert injected is True
        assert result.status is CacheWriteStatus.IGNORED_LOWER_AUTHORITY
        assert result.value.value == "winner"
        assert lookup.value is not None
        assert lookup.value.value == "winner"

    run(scenario())
