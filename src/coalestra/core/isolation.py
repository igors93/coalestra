from __future__ import annotations

import asyncio
from collections.abc import Callable, Collection, Mapping
from copy import deepcopy
from dataclasses import replace
from functools import partial
from time import monotonic as health_monotonic
from typing import Any, TypeVar, cast

from coalestra.core.deadline import remaining_deadline_seconds
from coalestra.core.errors import PayloadIsolationError, SnapshotDeadlineExceededError
from coalestra.core.health import PayloadCopyHealth, PayloadCopyHealthTracker
from coalestra.core.models import SnapshotValue

T = TypeVar("T")
_CopyItem = TypeVar("_CopyItem")
_CopyResult = TypeVar("_CopyResult")
PayloadCopier = Callable[[Any], Any]


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
    """Run payload isolation without monopolizing the event loop when configured."""

    def __init__(
        self,
        isolator: PayloadIsolator,
        *,
        run_in_thread: bool | None = None,
        max_concurrency: int = 4,
    ) -> None:
        if run_in_thread is not None and not isinstance(run_in_thread, bool):
            raise TypeError("run_in_thread must be a boolean or None")
        if isinstance(max_concurrency, bool) or not isinstance(max_concurrency, int):
            raise TypeError("max_concurrency must be an integer")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")

        self.isolator = isolator
        self.run_in_thread = (
            isolator.uses_default_copier if run_in_thread is None else run_in_thread
        )
        self.max_concurrency = max_concurrency
        self._limiter = asyncio.Semaphore(max_concurrency)
        self._health_tracker = PayloadCopyHealthTracker(
            run_in_thread=self.run_in_thread,
            max_concurrency=max_concurrency,
        )

    def health_snapshot(self) -> PayloadCopyHealth:
        """Return a lock-safe snapshot of copy activity and cumulative outcomes."""

        return self._health_tracker.snapshot()

    async def run(
        self,
        operation: Callable[[], _CopyResult],
        *,
        deadline_monotonic: float | None = None,
        monotonic: Callable[[], float] | None = None,
        deadline_context: str = "copying a payload",
    ) -> _CopyResult:
        """Run one copy operation with bounded, deadline-aware capacity accounting."""

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
            started_at = health_monotonic()
            self._health_tracker.copy_started()
            try:
                result = operation()
            except BaseException:
                self._health_tracker.copy_finished(
                    health_monotonic() - started_at,
                    failed=True,
                )
                raise
            else:
                self._health_tracker.copy_finished(
                    health_monotonic() - started_at,
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
        started_at = health_monotonic()
        self._health_tracker.copy_started()
        try:
            worker = asyncio.create_task(asyncio.to_thread(operation))
        except BaseException:
            self._health_tracker.copy_finished(
                health_monotonic() - started_at,
                failed=True,
            )
            self._limiter.release()
            raise

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
                health_monotonic() - started_at,
                failed=failed,
            )
            self._limiter.release()

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
        wait_started_at = health_monotonic()
        self._health_tracker.capacity_wait_started()
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

            if timeout_seconds is None:
                await self._limiter.acquire()
                return

            acquire_task = asyncio.create_task(self._limiter.acquire())
            try:
                completed, _pending = await asyncio.wait(
                    (acquire_task,),
                    timeout=timeout_seconds,
                )
            except BaseException:
                if acquire_task.done() and not acquire_task.cancelled():
                    error = acquire_task.exception()
                    if error is None and acquire_task.result():
                        self._limiter.release()
                else:
                    acquire_task.cancel()
                    await asyncio.gather(acquire_task, return_exceptions=True)
                raise

            if acquire_task in completed:
                await acquire_task
                return

            acquire_task.cancel()
            await asyncio.gather(acquire_task, return_exceptions=True)
            if acquire_task.done() and not acquire_task.cancelled():
                error = acquire_task.exception()
                if error is None and acquire_task.result():
                    self._limiter.release()
            self._health_tracker.record_timeout(waiting_for_capacity=True)
            raise SnapshotDeadlineExceededError(
                "snapshot deadline exceeded while waiting for payload copy capacity "
                f"during {deadline_context}"
            )
        finally:
            self._health_tracker.capacity_wait_finished(
                health_monotonic() - wait_started_at,
            )
