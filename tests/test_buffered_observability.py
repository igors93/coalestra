from __future__ import annotations

import threading

from coalestra import (
    BufferedEventSink,
    BufferedMetricsSink,
    BufferOverflowPolicy,
    InMemoryMetrics,
)


class CollectingEvents:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, object]]] = []
        self.lock = threading.Lock()

    def emit(self, event_type: str, **payload: object) -> None:
        with self.lock:
            self.records.append((event_type, dict(payload)))


def test_buffered_event_sink_delivers_and_flushes() -> None:
    downstream = CollectingEvents()
    sink = BufferedEventSink(downstream, max_pending=32)
    for index in range(20):
        sink.emit("event", index=index)

    assert sink.flush(timeout=2.0) is True
    stats = sink.stats()
    assert stats.enqueued == 20
    assert stats.delivered == 20
    assert stats.pending == 0
    assert len(downstream.records) == 20
    assert sink.close(timeout=2.0) is True


def test_buffered_sink_drop_newest_is_non_blocking_and_counted() -> None:
    started = threading.Event()
    release = threading.Event()

    class BlockingEvents:
        def emit(self, _event_type: str, **_payload: object) -> None:
            started.set()
            release.wait(timeout=2.0)

    sink = BufferedEventSink(
        BlockingEvents(),
        max_pending=1,
        overflow=BufferOverflowPolicy.DROP_NEWEST,
    )
    sink.emit("first")
    assert started.wait(timeout=1.0)
    sink.emit("second")
    sink.emit("third")
    assert sink.stats().dropped == 1
    release.set()
    assert sink.flush(timeout=2.0) is True
    sink.close(timeout=2.0)


def test_buffered_metrics_sink_delivers_both_metric_operations() -> None:
    downstream = InMemoryMetrics()
    with BufferedMetricsSink(downstream, max_pending=16) as sink:
        sink.increment("requests", status="ok")
        sink.observe("latency", 12.5, source="test")
        assert sink.flush(timeout=2.0) is True

    assert downstream.counter("requests", status="ok") == 1
    summary = downstream.summary("latency", source="test")
    assert summary is not None
    assert summary.average == 12.5


def test_buffered_sink_close_timeout_never_blocks_on_a_full_queue() -> None:
    import time

    started = threading.Event()
    release = threading.Event()

    class HungEvents:
        def emit(self, _event_type: str, **_payload: object) -> None:
            started.set()
            release.wait(timeout=2.0)

    sink = BufferedEventSink(HungEvents(), max_pending=1)
    sink.emit("first")
    assert started.wait(timeout=1.0)
    sink.emit("queued")

    before = time.monotonic()
    closed = sink.close(timeout=0.02, drain=True)
    elapsed = time.monotonic() - before
    release.set()

    assert closed is False
    assert elapsed < 0.5
