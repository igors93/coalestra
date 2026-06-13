from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Generic, TypeVar

K = TypeVar("K")
T = TypeVar("T")


class SingleFlight(Generic[K, T]):
    """Coalesce concurrent work for the same key into one shared future."""

    def __init__(self) -> None:
        self._tasks: dict[K, asyncio.Future[T]] = {}
        self._lock = asyncio.Lock()

    async def run(self, key: K, factory: Callable[[], Awaitable[T]]) -> tuple[T, bool]:
        async with self._lock:
            task = self._tasks.get(key)
            joined_existing = task is not None
            if task is None:
                task = asyncio.ensure_future(factory())
                self._tasks[key] = task
                task.add_done_callback(partial(self._schedule_cleanup, key))

        return await asyncio.shield(task), joined_existing

    async def in_flight(self) -> int:
        async with self._lock:
            return len(self._tasks)

    def _schedule_cleanup(self, key: K, task: asyncio.Future[T]) -> None:
        try:
            asyncio.get_running_loop().create_task(self._discard_if_same(key, task))
        except RuntimeError:
            # The loop is shutting down; process teardown will release the registry.
            return

    async def _discard_if_same(self, key: K, task: asyncio.Future[T]) -> None:
        async with self._lock:
            if self._tasks.get(key) is task:
                self._tasks.pop(key, None)
