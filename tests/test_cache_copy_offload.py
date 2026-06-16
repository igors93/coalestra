from __future__ import annotations

import asyncio
import threading
from copy import deepcopy
from typing import Any

import pytest

from coalestra import AsyncMemoryCache, CallableSource, ResourceKey, SnapshotBuilder, SnapshotValue


def run(coro):
    return asyncio.run(coro)


def snapshot_value(
    key: ResourceKey,
    payload: Any,
    *,
    observed_at: float = 100.0,
) -> SnapshotValue[Any]:
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
        metadata={"nested": {"value": 1}},
    )


def test_memory_cache_validates_copy_offload_settings() -> None:
    with pytest.raises(TypeError, match="run_payload_copies_in_thread"):
        AsyncMemoryCache(run_payload_copies_in_thread=1)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="max_copy_concurrency"):
        AsyncMemoryCache(max_copy_concurrency=True)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="max_copy_concurrency"):
        AsyncMemoryCache(max_copy_concurrency=0)


def test_memory_cache_auto_offloads_builtin_copier_only() -> None:
    builtin = AsyncMemoryCache()
    custom = AsyncMemoryCache(payload_copier=deepcopy)

    assert builtin.run_payload_copies_in_thread is True
    assert custom.run_payload_copies_in_thread is False


def test_memory_cache_keeps_custom_copier_inline_by_default() -> None:
    async def scenario() -> None:
        event_loop_thread = threading.get_ident()
        copy_threads: list[int] = []

        def copier(value: Any) -> Any:
            copy_threads.append(threading.get_ident())
            return deepcopy(value)

        cache = AsyncMemoryCache(payload_copier=copier)
        key = ResourceKey("test", "inline-copy")
        await cache.set_if_newer(snapshot_value(key, {"items": [1]}))

        assert copy_threads
        assert set(copy_threads) == {event_loop_thread}

    run(scenario())


def test_memory_cache_offloads_slow_copy_without_blocking_event_loop() -> None:
    async def scenario() -> None:
        event_loop_thread = threading.get_ident()
        copy_threads: list[int] = []
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        copy_started = threading.Event()
        release = threading.Event()
        watchdog = threading.Timer(1.0, release.set)

        def copier(value: Any) -> Any:
            copy_threads.append(threading.get_ident())
            if value == "slow-payload" and not copy_started.is_set():
                copy_started.set()
                loop.call_soon_threadsafe(started.set)
                release.wait(timeout=2.0)
            return deepcopy(value)

        cache = AsyncMemoryCache(
            payload_copier=copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )
        key = ResourceKey("test", "offloaded-copy")
        watchdog.start()
        try:
            write = asyncio.create_task(cache.set_if_newer(snapshot_value(key, "slow-payload")))
            started_at = loop.time()
            await started.wait()
            resumed_after = loop.time() - started_at

            assert resumed_after < 0.5
            assert not write.done()
            assert copy_threads
            assert all(thread_id != event_loop_thread for thread_id in copy_threads)

            release.set()
            await write
        finally:
            release.set()
            watchdog.cancel()

    run(scenario())


def test_memory_cache_bounds_concurrent_copy_workers() -> None:
    async def scenario() -> None:
        active = 0
        maximum_active = 0
        payload_starts = 0
        state_lock = threading.Lock()
        release = threading.Event()

        def copier(value: Any) -> Any:
            nonlocal active, maximum_active, payload_starts
            if isinstance(value, str) and value.startswith("payload-"):
                with state_lock:
                    active += 1
                    payload_starts += 1
                    maximum_active = max(maximum_active, active)
                release.wait(timeout=2.0)
                with state_lock:
                    active -= 1
            return deepcopy(value)

        cache = AsyncMemoryCache(
            payload_copier=copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=2,
        )
        writes = [
            asyncio.create_task(
                cache.set_if_newer(
                    snapshot_value(ResourceKey("test", "bounded", str(index)), f"payload-{index}")
                )
            )
            for index in range(4)
        ]

        try:
            for _ in range(10_000):
                with state_lock:
                    if payload_starts >= 2:
                        break
                await asyncio.sleep(0)
            with state_lock:
                assert payload_starts == 2
                assert maximum_active == 2
            await asyncio.sleep(0.05)
            with state_lock:
                assert payload_starts == 2
                assert maximum_active == 2
        finally:
            release.set()

        await asyncio.gather(*writes)
        assert maximum_active == 2

    run(scenario())


def test_cancelled_copy_keeps_capacity_reserved_until_worker_finishes() -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        first_started = asyncio.Event()
        first_copy_started = threading.Event()
        second_started = asyncio.Event()
        release_first = threading.Event()

        def copier(value: Any) -> Any:
            if value == "first" and not first_copy_started.is_set():
                first_copy_started.set()
                loop.call_soon_threadsafe(first_started.set)
                release_first.wait(timeout=2.0)
            elif value == "second":
                loop.call_soon_threadsafe(second_started.set)
            return deepcopy(value)

        cache = AsyncMemoryCache(
            payload_copier=copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )
        first = asyncio.create_task(
            cache.set_if_newer(snapshot_value(ResourceKey("test", "cancel", "first"), "first"))
        )
        await first_started.wait()

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        second = asyncio.create_task(
            cache.set_if_newer(snapshot_value(ResourceKey("test", "cancel", "second"), "second"))
        )
        await asyncio.sleep(0.05)
        assert not second_started.is_set()

        release_first.set()
        await asyncio.wait_for(second_started.wait(), timeout=1.0)
        await second

    run(scenario())


def test_snapshot_builder_configures_default_cache_copy_offload() -> None:
    key = ResourceKey("test", "builder-copy-offload")
    source = CallableSource(
        name="source",
        priority=1,
        supports=lambda candidate: candidate == key,
        fetcher=lambda _key, _context: "value",
    )

    builder = SnapshotBuilder(
        [source],
        cache_run_payload_copies_in_thread=True,
        cache_max_copy_concurrency=2,
    )

    assert isinstance(builder.cache, AsyncMemoryCache)
    assert builder.cache.run_payload_copies_in_thread is True
    assert builder.cache.max_copy_concurrency == 2


def test_snapshot_builder_rejects_default_cache_settings_with_custom_cache() -> None:
    key = ResourceKey("test", "builder-custom-cache")
    source = CallableSource(
        name="source",
        priority=1,
        supports=lambda candidate: candidate == key,
        fetcher=lambda _key, _context: "value",
    )

    with pytest.raises(ValueError, match="default AsyncMemoryCache"):
        SnapshotBuilder(
            [source],
            cache=AsyncMemoryCache(),
            cache_run_payload_copies_in_thread=True,
        )
