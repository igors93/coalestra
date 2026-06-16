from __future__ import annotations

import asyncio
from collections.abc import Callable, Collection, Mapping
from copy import deepcopy
from dataclasses import replace
from functools import partial
from time import perf_counter_ns
from typing import Any, TypeVar, cast

from coalestra.core.deadline import remaining_deadline_seconds
from coalestra.core.errors import (
    PayloadCopyShutdownTimeoutError,
    PayloadCopySubsystemClosedError,
    PayloadIsolationError,
    SnapshotDeadlineExceededError,
)
from coalestra.core.health import PayloadCopyHealth, PayloadCopyHealthTracker
from coalestra.core.models import SnapshotValue

T = TypeVar("T")
_CopyItem = TypeVar("_CopyItem")
_CopyResult = TypeVar("_CopyResult")
PayloadCopier = Callable[[Any], Any]


def _health_elapsed_seconds(started_ns: int) -> float:
    """Return a positive elapsed duration using the highest-resolution local timer."""

    elapsed_ns = max(1, perf_counter_ns() - started_ns)
    return elapsed_ns / 1_000_000_000.0


def deepcopy_payload(value: T) -> T:
    """Return a deep copy suitable for crossing a Coalestra ownership boundary."""

    return deepcopy(value)


class PayloadIsolator:
    """Create independent payload copies at source, cache, and snapshot boundaries."""

    def __init__(self, copier: PayloadCopier | None = None) -> None:
        self._uses_default_copier = copier is None
        self._copier = copier or deepcopy_payload

    @property
    def uses_default_copier(self) -> bool:
        """Whether this isolator uses Coalestra's built-in ``copy.deepcopy`` copier."""

        return self._uses_default_copier

    def copy(self, value: T, *, context: str) -> T:
        """Copy one value and raise a stable Coalestra error when copying fails."""

        try:
            return cast(T, self._copier(value))
        except PayloadIsolationError:
            raise
        except Exception as error:
            raise PayloadIsolationError(
                context=context,
                value_type=type(value).__name__,
            ) from error

    def copy_metadata(
        self,
        metadata: Mapping[str, Any],
        *,
        context: str,
    ) -> dict[str, Any]:
        """Copy nested metadata and normalize it to a plain dictionary."""

        copied = self.copy(dict(metadata), context=context)
        if not isinstance(copied, Mapping):
            raise PayloadIsolationError(
                context=context,
                value_type=type(metadata).__name__,
            )
        return dict(copied)

    def clone_snapshot_value(
        self,
        value: SnapshotValue[Any],
        *,
        context: str,
        fetched_at: float | None = None,
        age_seconds: float | None = None,
        stale: bool | None = None,
        from_cache: bool | None = None,
        latency_ms: float | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotValue[Any]:
        """Clone a snapshot value while optionally replacing acquisition fields."""

        changes: dict[str, Any] = {
            "value": self.copy(value.value, context=f"{context} payload"),
            "metadata": self.copy_metadata(
                value.metadata if metadata is None else metadata,
                context=f"{context} metadata",
            ),
        }
        if fetched_at is not None:
            changes["fetched_at"] = fetched_at
        if age_seconds is not None:
            changes["age_seconds"] = age_seconds
        if stale is not None:
            changes["stale"] = stale
        if from_cache is not None:
            changes["from_cache"] = from_cache
        if latency_ms is not None:
            changes["latency_ms"] = latency_ms
        return replace(value, **changes)


class AsyncPayloadIsolator:
    """Run payload isolation with bounded concurrency and controlled shutdown."""

    def __init__(
        self,
        isolator: PayloadIsolator,
        *,
        run_in_thread: bool | None = None,
        max_concurrency: int = 4,
        component_name: str = "payload-copy",
    ) -> None:
        if run_in_thread is not None and not isinstance(run_in_thread, bool):
            raise TypeError("run_in_thread must be a boolean or None")
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise TypeError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        normalized_component = str(component_name).strip()
        if not normalized_component:
            raise ValueError("component_name cannot be empty")

        self.isolator = isolator
        self.run_in_thread = (
            isolator.uses_default_copier if run_in_thread is None else run_in_thread
        )
        self.max_concurrency = max_concurrency
        self.component_name = normalized_component
        self._limiter = asyncio.Semaphore(max_concurrency)
        self._health_tracker = PayloadCopyHealthTracker(
            run_in_thread=self.run_in_thread,
            max_concurrency=max_concurrency,
        )
        self._closing = False
        self._closed = False
        self._workers: set[asyncio.Task[Any]] = set()
        self._capacity_waiters: set[asyncio.Task[bool]] = set()

    @property
    def closing(self) -> bool:
        """Whether shutdown has started and new copies are rejected."""

        return self._closing and not self._closed

    @property
    def closed(self) -> bool:
        """Whether shutdown completed after every tracked worker drained."""

        return self._closed

    def health_snapshot(self) -> PayloadCopyHealth:
        """Return a lock-safe snapshot of copy activity and cumulative outcomes."""

        return self._health_tracker.snapshot()

    async def aclose(self, *, timeout_seconds: float | None = 5.0) -> None:
        """Reject new work and wait for already-started worker copies to finish."""

        if timeout_seconds is not None:
            if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
                raise TypeError("timeout_seconds must be a number or None")
            if timeout_seconds <= 0:
                raise ValueError("timeout_seconds must be positive or None")
            resolved_timeout = float(timeout_seconds)
        else:
            resolved_timeout = None

        if self._closed:
            return
        if not self._closing:
            self._closing = True
            self._health_tracker.shutdown_started()
            for waiter in tuple(self._capacity_waiters):
                waiter.cancel()
            self._mark_closed_if_drained()
        if self._closed:
            return

        workers = tuple(self._workers)
        if resolved_timeout is None:
            await asyncio.gather(*workers, return_exceptions=True)
        else:
            _completed, pending = await asyncio.wait(workers, timeout=resolved_timeout)
            if pending:
                active_copies = len(pending)
                self._health_tracker.shutdown_timed_out(active_copies=active_copies)
                raise PayloadCopyShutdownTimeoutError(
                    timeout_seconds=resolved_timeout,
                    active_components={self.component_name: active_copies},
                )
        await asyncio.sleep(0)
        self._mark_closed_if_drained()

    async def run(
        self,
        operation: Callable[[], _CopyResult],
        *,
        deadline_monotonic: float | None = None,
        monotonic: Callable[[], float] | None = None,
        deadline_context: str = "copying a payload",
    ) -> _CopyResult:
        """Run one copy operation with bounded, deadline-aware capacity accounting."""

        self._ensure_accepting()
        monotonic_clock = monotonic or asyncio.get_running_loop().time
        try:
            remaining_deadline_seconds(
                deadline_monotonic,
                monotonic=monotonic_clock,
                operation=deadline_context,
            )
        except SnapshotDeadlineExceededError:
            self._health_tracker.record_timeout()
            raise

        if not self.run_in_thread:
            started_at = perf_counter_ns()
            self._health_tracker.copy_started()
            try:
                result = operation()
            except BaseException:
                self._health_tracker.copy_finished(
                    _health_elapsed_seconds(started_at),
                    failed=True,
                )
                raise
            else:
                self._health_tracker.copy_finished(
                    _health_elapsed_seconds(started_at),
                    failed=False,
                )
            try:
                remaining_deadline_seconds(
                    deadline_monotonic,
                    monotonic=monotonic_clock,
                    operation=deadline_context,
                )
            except SnapshotDeadlineExceededError:
                self._health_tracker.record_timeout()
                raise
            return result

        await self._acquire_slot(
            deadline_monotonic=deadline_monotonic,
            monotonic=monotonic_clock,
            deadline_context=deadline_context,
        )
        started_at = perf_counter_ns()
        self._health_tracker.copy_started()
        try:
            worker = asyncio.create_task(asyncio.to_thread(operation))
        except BaseException:
            self._health_tracker.copy_finished(
                _health_elapsed_seconds(started_at),
                failed=True,
            )
            self._limiter.release()
            self._mark_closed_if_drained()
            raise

        self._workers.add(worker)
        released = False

        def release_slot(task: asyncio.Task[_CopyResult]) -> None:
            nonlocal released
            if released:
                return
            released = True
            failed = task.cancelled()
            if not failed:
                failed = task.exception() is not None
            self._health_tracker.copy_finished(
                _health_elapsed_seconds(started_at),
                failed=failed,
            )
            self._workers.discard(task)
            self._limiter.release()
            self._mark_closed_if_drained()

        # Python cannot stop a worker thread after it starts. Keep its capacity slot
        # reserved until the underlying operation actually finishes.
        worker.add_done_callback(release_slot)
        try:
            timeout_seconds = remaining_deadline_seconds(
                deadline_monotonic,
                monotonic=monotonic_clock,
                operation=deadline_context,
            )
        except SnapshotDeadlineExceededError:
            self._health_tracker.record_timeout()
            raise

        completed, _pending = await asyncio.wait((worker,), timeout=timeout_seconds)
        if worker not in completed:
            self._health_tracker.record_timeout()
            raise SnapshotDeadlineExceededError(
                f"snapshot deadline exceeded while {deadline_context}"
            )

        try:
            result = await worker
            try:
                remaining_deadline_seconds(
                    deadline_monotonic,
                    monotonic=monotonic_clock,
                    operation=deadline_context,
                )
            except SnapshotDeadlineExceededError:
                self._health_tracker.record_timeout()
                raise
            return result
        finally:
            release_slot(worker)

    async def map(
        self,
        items: Collection[_CopyItem],
        operation: Callable[[_CopyItem], _CopyResult],
        *,
        deadline_monotonic: float | None = None,
        monotonic: Callable[[], float] | None = None,
        deadline_context: str = "copying payloads",
    ) -> tuple[_CopyResult, ...]:
        """Copy an ordered collection through a fixed, deadline-aware worker set."""

        self._ensure_accepting()
        ordered = tuple(items)
        if not ordered:
            return ()

        monotonic_clock = monotonic or asyncio.get_running_loop().time
        if not self.run_in_thread:
            inline_results: list[_CopyResult] = []
            for item in ordered:
                inline_results.append(
                    await self.run(
                        partial(operation, item),
                        deadline_monotonic=deadline_monotonic,
                        monotonic=monotonic_clock,
                        deadline_context=deadline_context,
                    )
                )
            return tuple(inline_results)

        worker_count = min(self.max_concurrency, len(ordered))
        results: dict[int, _CopyResult] = {}
        next_index = 0

        async def worker() -> None:
            nonlocal next_index
            while next_index < len(ordered):
                index = next_index
                next_index += 1
                results[index] = await self.run(
                    partial(operation, ordered[index]),
                    deadline_monotonic=deadline_monotonic,
                    monotonic=monotonic_clock,
                    deadline_context=deadline_context,
                )

        workers = tuple(asyncio.create_task(worker()) for _ in range(worker_count))
        try:
            await asyncio.gather(*workers)
        except BaseException:
            for worker_task in workers:
                worker_task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

        return tuple(results[index] for index in range(len(ordered)))

    async def copy(
        self,
        value: T,
        *,
        context: str,
        deadline_monotonic: float | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> T:
        """Asynchronously isolate one payload value."""

        return await self.run(
            partial(self.isolator.copy, value, context=context),
            deadline_monotonic=deadline_monotonic,
            monotonic=monotonic,
            deadline_context=f"isolating {context}",
        )

    async def copy_metadata(
        self,
        metadata: Mapping[str, Any],
        *,
        context: str,
        deadline_monotonic: float | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> dict[str, Any]:
        """Asynchronously isolate nested metadata."""

        return await self.run(
            partial(self.isolator.copy_metadata, metadata, context=context),
            deadline_monotonic=deadline_monotonic,
            monotonic=monotonic,
            deadline_context=f"isolating {context}",
        )

    async def clone_snapshot_value(
        self,
        value: SnapshotValue[Any],
        *,
        context: str,
        fetched_at: float | None = None,
        age_seconds: float | None = None,
        stale: bool | None = None,
        from_cache: bool | None = None,
        latency_ms: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        deadline_monotonic: float | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> SnapshotValue[Any]:
        """Asynchronously clone a snapshot value."""

        return await self.run(
            partial(
                self.isolator.clone_snapshot_value,
                value,
                context=context,
                fetched_at=fetched_at,
                age_seconds=age_seconds,
                stale=stale,
                from_cache=from_cache,
                latency_ms=latency_ms,
                metadata=metadata,
            ),
            deadline_monotonic=deadline_monotonic,
            monotonic=monotonic,
            deadline_context=f"isolating {context}",
        )

    async def _acquire_slot(
        self,
        *,
        deadline_monotonic: float | None,
        monotonic: Callable[[], float],
        deadline_context: str,
    ) -> None:
        self._ensure_accepting()
        wait_started_at = perf_counter_ns()
        self._health_tracker.capacity_wait_started()
        acquire_task: asyncio.Task[bool] | None = None
        try:
            try:
                timeout_seconds = remaining_deadline_seconds(
                    deadline_monotonic,
                    monotonic=monotonic,
                    operation=f"waiting for copy capacity while {deadline_context}",
                )
            except SnapshotDeadlineExceededError:
                self._health_tracker.record_timeout(waiting_for_capacity=True)
                raise

            acquire_task = asyncio.create_task(self._limiter.acquire())
            self._capacity_waiters.add(acquire_task)
            try:
                completed, _pending = await asyncio.wait(
                    (acquire_task,),
                    timeout=timeout_seconds,
                )
            except BaseException:
                await self._cancel_or_release_acquire(acquire_task)
                raise

            if acquire_task in completed:
                try:
                    await acquire_task
                except asyncio.CancelledError as error:
                    if self._closing:
                        raise PayloadCopySubsystemClosedError(
                            component=self.component_name
                        ) from error
                    raise
                if self._closing:
                    self._limiter.release()
                    raise PayloadCopySubsystemClosedError(component=self.component_name)
                return

            await self._cancel_or_release_acquire(acquire_task)
            self._health_tracker.record_timeout(waiting_for_capacity=True)
            raise SnapshotDeadlineExceededError(
                "snapshot deadline exceeded while waiting for payload copy capacity "
                f"during {deadline_context}"
            )
        finally:
            if acquire_task is not None:
                self._capacity_waiters.discard(acquire_task)
            self._health_tracker.capacity_wait_finished(
                _health_elapsed_seconds(wait_started_at),
            )

    async def _cancel_or_release_acquire(self, task: asyncio.Task[bool]) -> None:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return
        if task.cancelled():
            return
        error = task.exception()
        if error is None and task.result():
            self._limiter.release()

    def _ensure_accepting(self) -> None:
        if self._closing or self._closed:
            raise PayloadCopySubsystemClosedError(component=self.component_name)

    def _mark_closed_if_drained(self) -> None:
        if not self._closing or self._workers or self._closed:
            return
        self._closed = True
        self._health_tracker.shutdown_completed()
