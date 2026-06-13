from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar, cast

from coalestra.core.protocols import EventSink, MetricsSink

T = TypeVar("T")
_SENTINEL = object()


class BufferOverflowPolicy(str, Enum):
    """Behavior when an observability buffer reaches capacity."""

    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    RAISE = "raise"


@dataclass(frozen=True)
class BufferedSinkStats:
    enqueued: int
    delivered: int
    dropped: int
    failures: int
    pending: int
    closed: bool


@dataclass(frozen=True)
class EventRecord:
    event_type: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class MetricRecord:
    operation: str
    metric: str
    value: int | float
    labels: dict[str, str]


class _BufferedDispatcher(Generic[T]):
    def __init__(
        self,
        handler: Any,
        *,
        max_pending: int,
        overflow: BufferOverflowPolicy,
        thread_name: str,
    ) -> None:
        if max_pending < 1:
            raise ValueError("max_pending must be at least 1")
        self._handler = handler
        self._overflow = overflow
        self._queue: queue.Queue[T | object] = queue.Queue(maxsize=max_pending)
        self._condition = threading.Condition()
        self._enqueued = 0
        self._delivered = 0
        self._dropped = 0
        self._failures = 0
        self._pending = 0
        self._closed = False
        self._thread = threading.Thread(target=self._run, name=thread_name, daemon=True)
        self._thread.start()

    def submit(self, record: T) -> bool:
        with self._condition:
            if self._closed:
                raise RuntimeError("buffered sink is closed")
            try:
                self._queue.put_nowait(record)
            except queue.Full:
                if self._overflow is BufferOverflowPolicy.DROP_NEWEST:
                    self._dropped += 1
                    return False
                if self._overflow is BufferOverflowPolicy.RAISE:
                    raise
                try:
                    dropped = self._queue.get_nowait()
                except queue.Empty:
                    self._dropped += 1
                    return False
                if dropped is _SENTINEL:
                    self._queue.put_nowait(dropped)
                    self._dropped += 1
                    return False
                self._queue.task_done()
                self._dropped += 1
                self._pending = max(0, self._pending - 1)
                self._queue.put_nowait(record)
            self._enqueued += 1
            self._pending += 1
            return True

    def flush(self, timeout: float | None = None) -> bool:
        """Wait for queued records. Return ``False`` when ``timeout`` expires."""

        if timeout is not None and timeout < 0:
            raise ValueError("timeout cannot be negative")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._pending > 0:
                if deadline is None:
                    self._condition.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
        return True

    def close(self, *, timeout: float | None = 5.0, drain: bool = True) -> bool:
        with self._condition:
            if self._closed:
                return self._pending == 0
            self._closed = True
        drained = self.flush(timeout) if drain else False
        if not drain or not drained:
            self._discard_pending()
        self._enqueue_sentinel()
        self._thread.join(timeout=timeout)
        return drained and not self._thread.is_alive()

    def stats(self) -> BufferedSinkStats:
        with self._condition:
            return BufferedSinkStats(
                enqueued=self._enqueued,
                delivered=self._delivered,
                dropped=self._dropped,
                failures=self._failures,
                pending=self._pending,
                closed=self._closed,
            )

    def _enqueue_sentinel(self) -> None:
        while True:
            try:
                self._queue.put_nowait(_SENTINEL)
                return
            except queue.Full:
                try:
                    record = self._queue.get_nowait()
                except queue.Empty:
                    continue
                if record is _SENTINEL:
                    self._queue.put_nowait(record)
                    return
                self._queue.task_done()
                with self._condition:
                    self._dropped += 1
                    self._pending = max(0, self._pending - 1)
                    self._condition.notify_all()

    def _discard_pending(self) -> None:
        while True:
            try:
                record = self._queue.get_nowait()
            except queue.Empty:
                return
            if record is _SENTINEL:
                self._queue.put_nowait(record)
                return
            self._queue.task_done()
            with self._condition:
                self._dropped += 1
                self._pending = max(0, self._pending - 1)
                self._condition.notify_all()

    def _run(self) -> None:
        while True:
            record = self._queue.get()
            if record is _SENTINEL:
                self._queue.task_done()
                return
            try:
                self._handler(cast(T, record))
            except Exception:
                with self._condition:
                    self._failures += 1
            else:
                with self._condition:
                    self._delivered += 1
            finally:
                self._queue.task_done()
                with self._condition:
                    self._pending = max(0, self._pending - 1)
                    self._condition.notify_all()


class BufferedEventSink:
    """Non-blocking event sink that delivers records on a dedicated worker thread."""

    def __init__(
        self,
        downstream: EventSink,
        *,
        max_pending: int = 10_000,
        overflow: BufferOverflowPolicy = BufferOverflowPolicy.DROP_OLDEST,
    ) -> None:
        self.downstream = downstream
        self._dispatcher = _BufferedDispatcher[EventRecord](
            self._deliver,
            max_pending=max_pending,
            overflow=overflow,
            thread_name="coalestra-event-sink",
        )

    def emit(self, event_type: str, **payload: Any) -> None:
        self._dispatcher.submit(EventRecord(event_type=event_type, payload=dict(payload)))

    def flush(self, timeout: float | None = None) -> bool:
        return self._dispatcher.flush(timeout)

    def close(self, *, timeout: float | None = 5.0, drain: bool = True) -> bool:
        return self._dispatcher.close(timeout=timeout, drain=drain)

    def stats(self) -> BufferedSinkStats:
        return self._dispatcher.stats()

    def __enter__(self) -> BufferedEventSink:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def _deliver(self, record: EventRecord) -> None:
        self.downstream.emit(record.event_type, **record.payload)


class BufferedMetricsSink:
    """Non-blocking metrics sink that delivers updates on a worker thread."""

    def __init__(
        self,
        downstream: MetricsSink,
        *,
        max_pending: int = 10_000,
        overflow: BufferOverflowPolicy = BufferOverflowPolicy.DROP_OLDEST,
    ) -> None:
        self.downstream = downstream
        self._dispatcher = _BufferedDispatcher[MetricRecord](
            self._deliver,
            max_pending=max_pending,
            overflow=overflow,
            thread_name="coalestra-metrics-sink",
        )

    def increment(self, metric: str, value: int = 1, **labels: str) -> None:
        self._dispatcher.submit(
            MetricRecord(operation="increment", metric=metric, value=value, labels=dict(labels))
        )

    def observe(self, metric: str, value: float, **labels: str) -> None:
        self._dispatcher.submit(
            MetricRecord(
                operation="observe", metric=metric, value=float(value), labels=dict(labels)
            )
        )

    def flush(self, timeout: float | None = None) -> bool:
        return self._dispatcher.flush(timeout)

    def close(self, *, timeout: float | None = 5.0, drain: bool = True) -> bool:
        return self._dispatcher.close(timeout=timeout, drain=drain)

    def stats(self) -> BufferedSinkStats:
        return self._dispatcher.stats()

    def __enter__(self) -> BufferedMetricsSink:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def _deliver(self, record: MetricRecord) -> None:
        if record.operation == "increment":
            self.downstream.increment(record.metric, int(record.value), **record.labels)
        else:
            self.downstream.observe(record.metric, float(record.value), **record.labels)
