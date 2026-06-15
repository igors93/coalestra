from __future__ import annotations

import asyncio
import threading
from collections.abc import Collection, Coroutine, Iterable, Mapping
from concurrent.futures import Future, wait
from dataclasses import replace
from typing import Any, TypeVar

from coalestra.cache.publisher import PublishResult, ResourcePublisher, ResourceUpdate
from coalestra.core.errors import SubmissionBacklogFullError
from coalestra.core.health import BuilderHealth
from coalestra.core.models import ResourceKey, Snapshot
from coalestra.core.request import SnapshotRequest
from coalestra.orchestration.builder import SnapshotBuilder
from coalestra.orchestration.session import SnapshotSession

T = TypeVar("T")

_DEFAULT_MAX_PENDING_SUBMISSIONS = 1024


class SyncResourcePublisher:
    """Thread-safe synchronous and bounded non-blocking publisher facade."""

    def __init__(self, owner: SyncSnapshotBuilder, publisher: ResourcePublisher) -> None:
        self._owner = owner
        self._publisher = publisher

    @property
    def max_pending_submissions(self) -> int:
        """Return the maximum number of accepted non-blocking operations."""

        return self._owner.max_pending_submissions

    @property
    def pending_submissions(self) -> int:
        """Return the number of non-blocking operations that have not completed."""

        return self._owner.pending_submissions

    def publish(
        self,
        key: ResourceKey,
        value: Any,
        *,
        source: str,
        observed_at: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        force: bool = False,
        replace_equal: bool = False,
    ) -> PublishResult:
        return self._owner._submit(
            self._publisher.publish(
                key,
                value,
                source=source,
                observed_at=observed_at,
                metadata=metadata,
                force=force,
                replace_equal=replace_equal,
            )
        )

    def submit_publish(
        self,
        key: ResourceKey,
        value: Any,
        *,
        source: str,
        observed_at: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        force: bool = False,
        replace_equal: bool = False,
    ) -> Future[PublishResult]:
        """Schedule one publication without blocking the producer thread."""

        update = self._publisher._prepare_update_for_submission(
            ResourceUpdate(
                key=key,
                value=value,
                source=source,
                observed_at=observed_at,
                metadata=metadata or {},
            )
        )
        return self._owner._schedule_submission(
            self._publisher.publish_update(
                update,
                force=force,
                replace_equal=replace_equal,
            )
        )

    def publish_update(
        self,
        update: ResourceUpdate[Any],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> PublishResult:
        return self._owner._submit(
            self._publisher.publish_update(
                update,
                force=force,
                replace_equal=replace_equal,
            )
        )

    def submit_publish_update(
        self,
        update: ResourceUpdate[Any],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> Future[PublishResult]:
        """Schedule one structured publication without blocking the caller."""

        prepared = self._publisher._prepare_update_for_submission(update)
        return self._owner._schedule_submission(
            self._publisher.publish_update(
                prepared,
                force=force,
                replace_equal=replace_equal,
            )
        )

    def publish_many(
        self,
        updates: Collection[ResourceUpdate[Any]],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> Mapping[ResourceKey, PublishResult]:
        return self._owner._submit(
            self._publisher.publish_many(
                updates,
                force=force,
                replace_equal=replace_equal,
            )
        )

    def submit_publish_many(
        self,
        updates: Collection[ResourceUpdate[Any]],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> Future[Mapping[ResourceKey, PublishResult]]:
        """Schedule a bulk publication without blocking the producer thread."""

        prepared = tuple(
            self._publisher._prepare_update_for_submission(update) for update in updates
        )
        return self._owner._schedule_submission(
            self._publisher.publish_many(
                prepared,
                force=force,
                replace_equal=replace_equal,
            )
        )

    def invalidate(self, key: ResourceKey, *, reason: str = "") -> None:
        self._owner._submit(self._publisher.invalidate(key, reason=reason))

    def submit_invalidate(self, key: ResourceKey, *, reason: str = "") -> Future[None]:
        """Schedule one invalidation without blocking the producer thread."""

        return self._owner._schedule_submission(self._publisher.invalidate(key, reason=reason))

    def invalidate_many(
        self,
        keys: Collection[ResourceKey],
        *,
        reason: str = "",
    ) -> None:
        self._owner._submit(self._publisher.invalidate_many(keys, reason=reason))

    def submit_invalidate_many(
        self,
        keys: Collection[ResourceKey],
        *,
        reason: str = "",
    ) -> Future[None]:
        """Schedule bulk invalidation without blocking the producer thread."""

        prepared = tuple(keys)
        return self._owner._schedule_submission(
            self._publisher.invalidate_many(prepared, reason=reason)
        )

    def flush(self, *, timeout_seconds: float | None = None) -> None:
        """Wait for non-blocking operations accepted before this call."""

        self._owner.flush_submissions(timeout_seconds=timeout_seconds)


class SyncSnapshotSession:
    """Synchronous facade over one asynchronous ``SnapshotSession``."""

    def __init__(self, owner: SyncSnapshotBuilder, session: SnapshotSession) -> None:
        self._owner = owner
        self._session = session
        self._closed = False

    @property
    def snapshot_id(self) -> str:
        return self._session.snapshot_id

    @property
    def created_at(self) -> float:
        return self._session.created_at

    @property
    def closed(self) -> bool:
        return self._closed or self._session.closed

    def resolve(
        self,
        keys: Iterable[ResourceKey],
        *,
        strict: bool = True,
        retry_errors: bool = False,
    ) -> Snapshot:
        self._ensure_open()
        return self._owner._submit(
            self._session.resolve(
                keys,
                strict=strict,
                retry_errors=retry_errors,
            )
        )

    def resolve_request(
        self,
        request: SnapshotRequest,
        *,
        retry_errors: bool = False,
    ) -> Snapshot:
        self._ensure_open()
        return self._owner._submit(
            self._session.resolve_request(request, retry_errors=retry_errors)
        )

    def revalidate(
        self,
        keys: Iterable[ResourceKey],
        *,
        strict: bool = True,
        force_refresh: bool = False,
    ) -> Snapshot:
        """Refresh selected pinned resources transactionally."""

        self._ensure_open()
        return self._owner._submit(
            self._session.revalidate(
                keys,
                strict=strict,
                force_refresh=force_refresh,
            )
        )

    def snapshot(self) -> Snapshot:
        self._ensure_open()
        return self._owner._submit(self._snapshot_async())

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._owner._submit(self._session.close())

    def __enter__(self) -> SyncSnapshotSession:
        self._ensure_open()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    async def _snapshot_async(self) -> Snapshot:
        return self._session.snapshot()

    def _ensure_open(self) -> None:
        if self.closed:
            raise RuntimeError("SyncSnapshotSession is closed")


class SyncSnapshotBuilder:
    """Thread-backed synchronous facade that preserves one event loop across calls."""

    def __init__(
        self,
        builder: SnapshotBuilder,
        *,
        close_builder: bool = True,
        shutdown_timeout_seconds: float = 5.0,
        max_pending_submissions: int = _DEFAULT_MAX_PENDING_SUBMISSIONS,
    ) -> None:
        if shutdown_timeout_seconds <= 0:
            raise ValueError("shutdown_timeout_seconds must be positive")
        if max_pending_submissions < 1:
            raise ValueError("max_pending_submissions must be at least 1")

        self.builder = builder
        self._close_builder = bool(close_builder)
        self._shutdown_timeout_seconds = float(shutdown_timeout_seconds)
        self._max_pending_submissions = int(max_pending_submissions)
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._closed = False
        self._closing = False
        self._accepting_submissions = True
        self._submission_condition = threading.Condition()
        self._pending_submissions: set[Future[Any]] = set()
        self._close_lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="coalestra-sync-loop",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()
        self.publisher = SyncResourcePublisher(self, builder.publisher)

    @property
    def max_pending_submissions(self) -> int:
        """Return the configured non-blocking submission backlog limit."""

        return self._max_pending_submissions

    @property
    def pending_submissions(self) -> int:
        """Return the number of accepted submissions that have not completed."""

        with self._submission_condition:
            return len(self._pending_submissions)

    @property
    def closed(self) -> bool:
        with self._submission_condition:
            return self._closed

    def build(
        self,
        keys: Iterable[ResourceKey],
        *,
        strict: bool = True,
        deadline_seconds: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> Snapshot:
        self._ensure_open()
        return self._submit(
            self.builder.build(
                keys,
                strict=strict,
                deadline_seconds=deadline_seconds,
                metadata=metadata,
                snapshot_id=snapshot_id,
            )
        )

    def build_request(
        self,
        request: SnapshotRequest,
        *,
        deadline_seconds: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> Snapshot:
        self._ensure_open()
        return self._submit(
            self.builder.build_request(
                request,
                deadline_seconds=deadline_seconds,
                metadata=metadata,
                snapshot_id=snapshot_id,
            )
        )

    def health_snapshot(self) -> BuilderHealth:
        self._ensure_open()
        health = self._submit(self.builder.health_snapshot())
        with self._submission_condition:
            pending_submissions = len(self._pending_submissions)
        return replace(
            health,
            pending_submissions=pending_submissions,
            max_pending_submissions=self._max_pending_submissions,
        )

    def session(
        self,
        *,
        deadline_seconds: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> SyncSnapshotSession:
        self._ensure_open()
        session = self._submit(
            self._create_session(
                deadline_seconds=deadline_seconds,
                metadata=metadata,
                snapshot_id=snapshot_id,
            )
        )
        return SyncSnapshotSession(self, session)

    def wait_for_refreshes(self) -> None:
        self._ensure_open()
        self._submit(self.builder.wait_for_refreshes())

    def flush_submissions(self, *, timeout_seconds: float | None = None) -> None:
        """Wait for non-blocking operations accepted before this call."""

        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._ensure_open()
        with self._submission_condition:
            pending = tuple(self._pending_submissions)
        self._wait_for_submissions(pending, timeout_seconds=timeout_seconds)

    def close(self) -> None:
        with self._close_lock:
            if self.closed:
                return

            with self._submission_condition:
                self._closing = True
                self._accepting_submissions = False
                pending = tuple(self._pending_submissions)

            self._drain_or_cancel_submissions(pending)
            close_error: BaseException | None = None
            try:
                if self._close_builder:
                    self._submit_during_close(self.builder.aclose())
                else:
                    self._submit_during_close(self.builder.wait_for_refreshes())
            except BaseException as error:
                close_error = error
            finally:
                with self._submission_condition:
                    self._closed = True
                    self._closing = False
                    self._submission_condition.notify_all()
                self._loop.call_soon_threadsafe(self._loop.stop)
                self._thread.join(timeout=self._shutdown_timeout_seconds)

            if self._thread.is_alive():
                raise RuntimeError("Coalestra synchronous event-loop thread did not stop")
            if close_error is not None:
                raise close_error

    def __enter__(self) -> SyncSnapshotBuilder:
        self._ensure_open()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def _submit(self, coroutine: Coroutine[Any, Any, T]) -> T:
        return self._schedule(coroutine).result()

    def _schedule(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        with self._submission_condition:
            if self._closed or self._closing:
                coroutine.close()
                raise RuntimeError("SyncSnapshotBuilder is closed")
            return asyncio.run_coroutine_threadsafe(coroutine, self._loop)

    def _schedule_submission(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        with self._submission_condition:
            if self._closed or self._closing or not self._accepting_submissions:
                coroutine.close()
                raise RuntimeError("SyncSnapshotBuilder is closed")

            pending = len(self._pending_submissions)
            if pending >= self._max_pending_submissions:
                coroutine.close()
                raise SubmissionBacklogFullError(
                    limit=self._max_pending_submissions,
                    pending=pending,
                )

            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
            except BaseException:
                coroutine.close()
                raise
            self._pending_submissions.add(future)

        future.add_done_callback(self._submission_completed)
        return future

    def _submission_completed(self, future: Future[Any]) -> None:
        with self._submission_condition:
            self._pending_submissions.discard(future)
            self._submission_condition.notify_all()

    def _drain_or_cancel_submissions(self, pending: tuple[Future[Any], ...]) -> None:
        if not pending:
            return

        _done, remaining = wait(pending, timeout=self._shutdown_timeout_seconds)
        if not remaining:
            return

        for future in remaining:
            future.cancel()
        wait(remaining, timeout=self._shutdown_timeout_seconds)

    @staticmethod
    def _wait_for_submissions(
        pending: tuple[Future[Any], ...],
        *,
        timeout_seconds: float | None,
    ) -> None:
        if not pending:
            return

        _done, remaining = wait(pending, timeout=timeout_seconds)
        if remaining:
            raise TimeoutError(f"Timed out waiting for {len(remaining)} non-blocking submission(s)")

    def _submit_during_close(self, coroutine: Coroutine[Any, Any, T]) -> T:
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=self._shutdown_timeout_seconds)
        except BaseException:
            future.cancel()
            raise

    async def _create_session(
        self,
        *,
        deadline_seconds: float | None,
        metadata: Mapping[str, Any] | None,
        snapshot_id: str | None,
    ) -> SnapshotSession:
        return self.builder.session(
            deadline_seconds=deadline_seconds,
            metadata=metadata,
            snapshot_id=snapshot_id,
        )

    def _ensure_open(self) -> None:
        with self._submission_condition:
            if self._closed or self._closing:
                raise RuntimeError("SyncSnapshotBuilder is closed")

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()
