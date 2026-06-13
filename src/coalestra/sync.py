from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine, Iterable, Mapping
from concurrent.futures import Future
from typing import Any, TypeVar

from coalestra.core.models import ResourceKey, Snapshot
from coalestra.orchestration.builder import SnapshotBuilder
from coalestra.orchestration.session import SnapshotSession

T = TypeVar("T")


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

    def __init__(self, builder: SnapshotBuilder) -> None:
        self.builder = builder
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._closed = False
        self._thread = threading.Thread(
            target=self._run_loop,
            name="coalestra-sync-loop",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()

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

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2.0)

    def __enter__(self) -> SyncSnapshotBuilder:
        self._ensure_open()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def _submit(self, coroutine: Coroutine[Any, Any, T]) -> T:
        self._ensure_open()
        future: Future[T] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result()

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
        if self._closed:
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
