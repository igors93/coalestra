from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterable, Mapping
from concurrent.futures import Future
from typing import Any

from coalestra.core.models import ResourceKey, Snapshot
from coalestra.orchestration.builder import SnapshotBuilder


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
        if self._closed:
            raise RuntimeError("SyncSnapshotBuilder is closed")

        coroutine = self.builder.build(
            keys,
            strict=strict,
            deadline_seconds=deadline_seconds,
            metadata=metadata,
            snapshot_id=snapshot_id,
        )
        future: Future[Snapshot] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=2.0)

    def __enter__(self) -> SyncSnapshotBuilder:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

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
