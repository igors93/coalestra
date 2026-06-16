from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from coalestra import (
    AsyncMemoryCache,
    CallableSource,
    FreshnessPolicy,
    PayloadCopyShutdownTimeoutError,
    PayloadCopySubsystemClosedError,
    ResourceKey,
    SnapshotBuilder,
    SnapshotValue,
    SyncSnapshotBuilder,
)

KEY = ResourceKey("shutdown", "copy", "A")


def _builder(*, max_copy_concurrency: int = 1) -> SnapshotBuilder:
    return SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda *_args: 1,
            )
        ],
        max_copy_concurrency=max_copy_concurrency,
        cache_max_copy_concurrency=max_copy_concurrency,
    )


async def _wait_until(predicate, *, attempts: int = 400) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.002)
    raise AssertionError("condition was not reached")


def _blocking_operation(started: threading.Event, release: threading.Event) -> str:
    started.set()
    release.wait(timeout=2.0)
    return "done"


def _snapshot_value(value: Any) -> SnapshotValue[Any]:
    return SnapshotValue(
        key=KEY,
        value=value,
        source="test",
        observed_at=100.0,
        fetched_at=100.0,
        age_seconds=0.0,
        stale=False,
        from_cache=False,
        latency_ms=0.0,
    )


def test_builder_close_waits_for_builder_and_cache_copy_workers() -> None:
    async def scenario() -> None:
        builder = _builder()
        assert isinstance(builder.cache, AsyncMemoryCache)
        builder_started = threading.Event()
        builder_release = threading.Event()
        cache_started = threading.Event()
        cache_release = threading.Event()

        builder_copy = asyncio.create_task(
            builder._async_payload_isolator.run(
                lambda: _blocking_operation(builder_started, builder_release)
            )
        )
        cache_copy = asyncio.create_task(
            builder.cache._async_payload_isolator.run(
                lambda: _blocking_operation(cache_started, cache_release)
            )
        )
        assert await asyncio.to_thread(builder_started.wait, 1.0)
        assert await asyncio.to_thread(cache_started.wait, 1.0)

        close_task = asyncio.create_task(builder.aclose(copy_shutdown_timeout_seconds=1.0))
        await _wait_until(
            lambda: (
                builder._async_payload_isolator.health_snapshot().shutdown_started
                and builder.cache._async_payload_isolator.health_snapshot().shutdown_started
            )
        )
        assert not close_task.done()

        during_close = await builder.health_snapshot()
        assert during_close.closed is True
        assert during_close.active_payload_copies == 2
        assert during_close.payload_copy_components["builder"].accepting_copies is False
        assert during_close.payload_copy_components["cache"].accepting_copies is False

        builder_release.set()
        cache_release.set()
        assert await builder_copy == "done"
        assert await cache_copy == "done"
        await close_task

        closed = await builder.health_snapshot()
        assert closed.payload_copy_shutdown_incomplete is False
        assert closed.payload_copy_components["builder"].shutdown_complete is True
        assert closed.payload_copy_components["cache"].shutdown_complete is True
        assert closed.active_payload_copies == 0

    asyncio.run(scenario())


def test_builder_close_timeout_is_reported_and_late_completion_is_visible() -> None:
    async def scenario() -> None:
        builder = _builder()
        started = threading.Event()
        release = threading.Event()
        copy_task = asyncio.create_task(
            builder._async_payload_isolator.run(lambda: _blocking_operation(started, release))
        )
        assert await asyncio.to_thread(started.wait, 1.0)

        with pytest.raises(PayloadCopyShutdownTimeoutError) as captured:
            await builder.aclose(copy_shutdown_timeout_seconds=0.03)

        error = captured.value
        assert error.active_components == {"builder": 1}
        assert error.active_copies == 1

        timed_out = await builder.health_snapshot()
        component = timed_out.payload_copy_components["builder"]
        assert timed_out.payload_copy_shutdown_incomplete is True
        assert timed_out.payload_copy_shutdown_timeout_count == 1
        assert timed_out.payload_copy_active_at_last_shutdown_timeout == 1
        assert component.shutdown_incomplete is True
        assert component.shutdown_timeout_count == 1
        assert component.active_at_last_shutdown_timeout == 1

        release.set()
        assert await copy_task == "done"
        await _wait_until(
            lambda: builder._async_payload_isolator.health_snapshot().shutdown_complete
        )
        finished = await builder.health_snapshot()
        component = finished.payload_copy_components["builder"]
        assert component.shutdown_complete is True
        assert component.shutdown_incomplete is False
        assert component.shutdown_timeout_count == 1
        assert finished.payload_copy_shutdown_incomplete is False

    asyncio.run(scenario())


def test_shutdown_rejects_new_copies_and_interrupts_capacity_waiters() -> None:
    async def scenario() -> None:
        builder = _builder()
        started = threading.Event()
        release = threading.Event()
        first = asyncio.create_task(
            builder._async_payload_isolator.run(lambda: _blocking_operation(started, release))
        )
        assert await asyncio.to_thread(started.wait, 1.0)

        waiting = asyncio.create_task(builder._async_payload_isolator.run(lambda: "second"))
        await _wait_until(
            lambda: builder._async_payload_isolator.health_snapshot().waiting_for_capacity == 1
        )
        close_task = asyncio.create_task(builder.aclose(copy_shutdown_timeout_seconds=1.0))

        with pytest.raises(PayloadCopySubsystemClosedError):
            await waiting
        with pytest.raises(PayloadCopySubsystemClosedError):
            await builder._async_payload_isolator.run(lambda: "late")

        release.set()
        assert await first == "done"
        await close_task

    asyncio.run(scenario())


def test_standalone_memory_cache_closes_its_copy_workers() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache(max_copy_concurrency=1)
        started = threading.Event()
        release = threading.Event()
        copy_task = asyncio.create_task(
            cache._async_payload_isolator.run(lambda: _blocking_operation(started, release))
        )
        assert await asyncio.to_thread(started.wait, 1.0)

        close_task = asyncio.create_task(cache.aclose_payload_copies(timeout_seconds=1.0))
        await _wait_until(lambda: cache.copy_health_snapshot().shutdown_started)
        release.set()
        assert await copy_task == "done"
        await close_task

        with pytest.raises(PayloadCopySubsystemClosedError):
            await cache.set(_snapshot_value({"value": 1}))
        with pytest.raises(PayloadCopySubsystemClosedError):
            await cache.get(KEY, now=100.0, policy=FreshnessPolicy(60.0, 60.0))

    asyncio.run(scenario())


def test_copy_shutdown_timeout_validation() -> None:
    async def scenario() -> None:
        builder = _builder()
        with pytest.raises(ValueError, match="positive"):
            await builder.aclose(copy_shutdown_timeout_seconds=0)
        assert builder.closed is False
        await builder.aclose()

    asyncio.run(scenario())


def test_sync_close_defers_event_loop_stop_until_late_copy_finishes() -> None:
    builder = _builder()
    sync_builder = SyncSnapshotBuilder(
        builder,
        shutdown_timeout_seconds=0.03,
    )
    started = threading.Event()
    release = threading.Event()
    copy_future = sync_builder._schedule(
        builder._async_payload_isolator.run(lambda: _blocking_operation(started, release))
    )
    assert started.wait(timeout=1.0)

    with pytest.raises(PayloadCopyShutdownTimeoutError):
        sync_builder.close()

    assert sync_builder.closed is True
    assert sync_builder._thread.is_alive()
    release.set()
    assert copy_future.result(timeout=1.0) == "done"
    sync_builder._thread.join(timeout=1.0)
    assert not sync_builder._thread.is_alive()
