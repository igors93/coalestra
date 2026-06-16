from __future__ import annotations

import asyncio
import threading

import pytest

import coalestra.core.isolation as isolation_module
from coalestra import AsyncMemoryCache, CallableSource, ResourceKey, SnapshotBuilder
from coalestra.core.errors import SnapshotDeadlineExceededError

KEY = ResourceKey("health", "copy", "A")


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


def test_copy_health_records_positive_duration_when_timer_does_not_advance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        isolator = isolation_module.AsyncPayloadIsolator(
            isolation_module.PayloadIsolator(),
            run_in_thread=False,
        )
        monkeypatch.setattr(isolation_module, "perf_counter_ns", lambda: 123)

        assert await isolator.run(lambda: "copied") == "copied"
        health = isolator.health_snapshot()
        assert health.completed_count == 1
        assert health.max_duration_ms > 0

    asyncio.run(scenario())


def test_builder_health_exposes_idle_copy_components() -> None:
    async def scenario() -> None:
        builder = _builder(max_copy_concurrency=2)
        try:
            health = await builder.health_snapshot()

            assert set(health.payload_copy_components) == {"builder", "cache"}
            assert health.payload_copy_components["builder"].run_in_thread is True
            assert health.payload_copy_components["builder"].max_concurrency == 2
            assert health.payload_copy_components["cache"].max_concurrency == 2
            assert health.active_payload_copies == 0
            assert health.waiting_for_copy_capacity == 0
            assert health.payload_copy_started_count == 0
            assert health.payload_copy_completed_count == 0
            with pytest.raises(TypeError):
                health.payload_copy_components["other"] = health.payload_copy_components["builder"]  # type: ignore[index]
        finally:
            await builder.aclose()

    asyncio.run(scenario())


def test_builder_health_reports_active_and_waiting_copy_work() -> None:
    async def scenario() -> None:
        builder = _builder(max_copy_concurrency=1)
        started = threading.Event()
        release = threading.Event()

        def blocking_copy() -> str:
            started.set()
            release.wait(timeout=2.0)
            return "first"

        first = asyncio.create_task(builder._async_payload_isolator.run(blocking_copy))
        assert await asyncio.to_thread(started.wait, 1.0)
        second = asyncio.create_task(builder._async_payload_isolator.run(lambda: "second"))

        try:
            await _wait_until(
                lambda: builder._async_payload_isolator.health_snapshot().waiting_for_capacity == 1
            )
            health = await builder.health_snapshot()
            component = health.payload_copy_components["builder"]

            assert component.active_copies == 1
            assert component.waiting_for_capacity == 1
            assert component.peak_active_copies == 1
            assert component.peak_waiting_for_capacity == 1
            assert health.active_payload_copies == 1
            assert health.waiting_for_copy_capacity == 1
        finally:
            release.set()

        assert await first == "first"
        assert await second == "second"
        settled = await builder.health_snapshot()
        component = settled.payload_copy_components["builder"]
        assert component.active_copies == 0
        assert component.waiting_for_capacity == 0
        assert component.started_count == 2
        assert component.completed_count == 2
        assert component.failure_count == 0
        assert component.capacity_wait_count == 2
        assert component.max_duration_ms > 0
        assert settled.payload_copy_started_count == 2
        assert settled.payload_copy_completed_count == 2
        await builder.aclose()

    asyncio.run(scenario())


def test_copy_timeout_remains_visible_until_worker_finishes() -> None:
    async def scenario() -> None:
        builder = _builder(max_copy_concurrency=1)
        started = threading.Event()
        release = threading.Event()

        def blocking_copy() -> str:
            started.set()
            release.wait(timeout=2.0)
            return "late"

        try:
            with pytest.raises(SnapshotDeadlineExceededError):
                await builder._async_payload_isolator.run(
                    blocking_copy,
                    deadline_monotonic=asyncio.get_running_loop().time() + 0.03,
                )
            assert started.is_set()

            timed_out = await builder.health_snapshot()
            component = timed_out.payload_copy_components["builder"]
            assert component.active_copies == 1
            assert component.timeout_count == 1
            assert component.completed_count == 0
            assert timed_out.payload_copy_timeout_count == 1

            release.set()
            await _wait_until(
                lambda: builder._async_payload_isolator.health_snapshot().active_copies == 0
            )
            finished = await builder.health_snapshot()
            component = finished.payload_copy_components["builder"]
            assert component.timeout_count == 1
            assert component.completed_count == 1
            assert component.failure_count == 0
        finally:
            release.set()
            await builder.aclose()

    asyncio.run(scenario())


def test_copy_health_records_capacity_timeout_and_failure() -> None:
    async def scenario() -> None:
        builder = _builder(max_copy_concurrency=1)
        started = threading.Event()
        release = threading.Event()

        def blocking_copy() -> str:
            started.set()
            release.wait(timeout=2.0)
            return "held"

        first = asyncio.create_task(builder._async_payload_isolator.run(blocking_copy))
        assert await asyncio.to_thread(started.wait, 1.0)
        try:
            with pytest.raises(SnapshotDeadlineExceededError):
                await builder._async_payload_isolator.run(
                    lambda: "never-started",
                    deadline_monotonic=asyncio.get_running_loop().time() + 0.03,
                )
            health = await builder.health_snapshot()
            component = health.payload_copy_components["builder"]
            assert component.timeout_count == 1
            assert component.capacity_timeout_count == 1
            assert component.started_count == 1
            assert health.payload_copy_capacity_timeout_count == 1
        finally:
            release.set()
            await first

        def fail() -> None:
            raise ValueError("copy failed")

        with pytest.raises(ValueError, match="copy failed"):
            await builder._async_payload_isolator.run(fail)
        failed = await builder.health_snapshot()
        component = failed.payload_copy_components["builder"]
        assert component.failure_count == 1
        assert failed.payload_copy_failure_count == 1
        await builder.aclose()

    asyncio.run(scenario())


def test_builder_health_aggregates_default_cache_copy_activity() -> None:
    async def scenario() -> None:
        builder = _builder(max_copy_concurrency=1)
        try:
            assert isinstance(builder.cache, AsyncMemoryCache)
            result = await builder.cache._async_payload_isolator.run(lambda: "cached")
            assert result == "cached"

            health = await builder.health_snapshot()
            builder_component = health.payload_copy_components["builder"]
            cache_component = health.payload_copy_components["cache"]
            assert builder_component.started_count == 0
            assert cache_component.started_count == 1
            assert cache_component.completed_count == 1
            assert health.payload_copy_started_count == 1
            assert health.payload_copy_completed_count == 1
        finally:
            await builder.aclose()

    asyncio.run(scenario())
