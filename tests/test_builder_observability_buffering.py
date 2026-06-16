from __future__ import annotations

import asyncio
import threading
import time

import pytest

from coalestra import (
    BufferedEventSink,
    CallableSource,
    InMemoryMetrics,
    ObservabilityShutdownTimeoutError,
    ResourceKey,
    SnapshotBuilder,
    SyncSnapshotBuilder,
)

KEY = ResourceKey("test", "value")


def _source() -> CallableSource:
    return CallableSource(
        name="source",
        priority=1,
        supports=lambda key: key == KEY,
        fetcher=lambda _key, _context: "value",
        run_sync_in_thread=False,
    )


class SlowMetrics:
    def increment(self, _metric: str, _value: int = 1, **_labels: str) -> None:
        time.sleep(0.05)

    def observe(self, _metric: str, _value: float, **_labels: str) -> None:
        time.sleep(0.05)


class SlowEvents:
    def emit(self, _event_type: str, **_payload: object) -> None:
        time.sleep(0.05)


class BlockingEvents:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.delivered = 0
        self.closed = False

    def emit(self, _event_type: str, **_payload: object) -> None:
        self.started.set()
        self.release.wait(timeout=2.0)
        self.delivered += 1

    def close(self) -> None:
        self.closed = True


class CollectingEvents:
    def __init__(self) -> None:
        self.records: list[str] = []
        self.closed_after_delivery = False

    def emit(self, event_type: str, **_payload: object) -> None:
        self.records.append(event_type)

    def close(self) -> None:
        self.closed_after_delivery = bool(self.records)


def test_builder_automatically_buffers_unknown_observability_sinks() -> None:
    async def scenario() -> None:
        builder = SnapshotBuilder(
            [_source()],
            metrics=SlowMetrics(),
            events=SlowEvents(),
        )
        assert builder.metrics is not builder._metrics_downstream
        assert builder.events is not builder._events_downstream

        started_at = time.perf_counter()
        snapshot = await builder.build([KEY])
        elapsed = time.perf_counter() - started_at

        assert snapshot.value(KEY, str) == "value"
        assert elapsed < 0.25
        await builder.aclose()

    asyncio.run(scenario())


def test_auto_mode_keeps_known_non_blocking_metrics_inline() -> None:
    metrics = InMemoryMetrics()
    builder = SnapshotBuilder([_source()], metrics=metrics)
    try:
        assert builder.metrics is metrics
    finally:
        asyncio.run(builder.aclose())


def test_buffer_observability_false_preserves_direct_sink_semantics() -> None:
    events = CollectingEvents()
    builder = SnapshotBuilder(
        [_source()],
        events=events,
        buffer_observability=False,
    )
    try:
        assert builder.events is events
        builder.events.emit("direct")
        assert events.records == ["direct"]
    finally:
        asyncio.run(builder.aclose())


def test_existing_buffered_sink_is_not_wrapped_twice() -> None:
    downstream = CollectingEvents()
    buffered = BufferedEventSink(downstream)
    builder = SnapshotBuilder([_source()], events=buffered)
    try:
        assert builder.events is buffered
    finally:
        asyncio.run(builder.aclose())
        buffered.close()


def test_observability_buffer_health_reports_overflow_and_pending_work() -> None:
    async def scenario() -> None:
        events = BlockingEvents()
        builder = SnapshotBuilder(
            [_source()],
            events=events,
            observability_max_pending=1,
        )
        builder.events.emit("first")
        assert await asyncio.to_thread(events.started.wait, 1.0)
        builder.events.emit("second")
        builder.events.emit("third")

        health = await builder.health_snapshot()
        event_health = health.observability_buffers["events"]
        assert event_health.pending == 2
        assert event_health.dropped == 1
        assert event_health.peak_pending == 2
        assert event_health.max_pending == 1
        assert health.observability_pending == 2
        assert health.observability_dropped_count == 1

        events.release.set()
        await builder.aclose()
        settled = await builder.health_snapshot()
        assert settled.observability_pending == 0
        assert settled.observability_shutdown_incomplete is False

    asyncio.run(scenario())


def test_builder_drains_owned_buffers_before_closing_downstream_sinks() -> None:
    async def scenario() -> None:
        events = CollectingEvents()
        builder = SnapshotBuilder(
            [_source()],
            events=events,
            manage_lifecycle=True,
        )
        builder.events.emit("queued")
        await builder.aclose()

        assert events.records == ["queued"]
        assert events.closed_after_delivery is True

    asyncio.run(scenario())


def test_observability_shutdown_timeout_is_reported_and_can_be_completed_later() -> None:
    async def scenario() -> None:
        events = BlockingEvents()
        builder = SnapshotBuilder(
            [_source()],
            events=events,
            manage_lifecycle=True,
            observability_shutdown_timeout_seconds=0.02,
        )
        builder.events.emit("blocked")
        assert await asyncio.to_thread(events.started.wait, 1.0)

        with pytest.raises(ObservabilityShutdownTimeoutError) as captured:
            await builder.aclose()

        assert captured.value.pending_components == {"events": 1}
        assert events.closed is False
        health = await builder.health_snapshot()
        assert health.observability_shutdown_incomplete is True
        assert health.observability_shutdown_timeout_count == 1

        events.release.set()
        await builder.wait_for_observability_shutdown()
        assert events.closed is True
        settled = await builder.health_snapshot()
        assert settled.observability_shutdown_incomplete is False

    asyncio.run(scenario())


def test_downstream_failures_are_counted_without_failing_snapshot_acquisition() -> None:
    class FailingEvents:
        def emit(self, _event_type: str, **_payload: object) -> None:
            raise RuntimeError("sink unavailable")

    async def scenario() -> None:
        builder = SnapshotBuilder([_source()], events=FailingEvents())
        snapshot = await builder.build([KEY])
        assert snapshot.value(KEY, str) == "value"
        await builder.aclose()
        health = await builder.health_snapshot()
        assert health.observability_failure_count > 0

    asyncio.run(scenario())


def test_sync_close_keeps_loop_alive_until_late_observability_worker_finishes() -> None:
    events = BlockingEvents()
    builder = SnapshotBuilder(
        [_source()],
        events=events,
        manage_lifecycle=True,
        observability_shutdown_timeout_seconds=0.02,
    )
    sync_builder = SyncSnapshotBuilder(builder, shutdown_timeout_seconds=0.03)
    builder.events.emit("blocked")
    assert events.started.wait(timeout=1.0)

    with pytest.raises(ObservabilityShutdownTimeoutError):
        sync_builder.close()

    assert sync_builder.closed is True
    assert sync_builder._thread.is_alive()
    events.release.set()
    sync_builder._thread.join(timeout=1.0)
    assert not sync_builder._thread.is_alive()
    assert events.closed is True
