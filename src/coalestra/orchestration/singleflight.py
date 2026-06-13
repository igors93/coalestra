from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection, Mapping
from typing import Generic, TypeVar

K = TypeVar("K")
T = TypeVar("T")


class SingleFlight(Generic[K, T]):
    """Coalesce concurrent work for identical keys into shared futures.

    ``run_many`` reserves every key independently, so partially overlapping batch requests still
    share work at resource granularity. The first caller owns unresolved keys and may resolve them
    together with one batch operation; later callers join the existing per-key futures.
    """

    def __init__(self) -> None:
        self._tasks: dict[K, asyncio.Future[T]] = {}
        self._producer_tasks: set[asyncio.Task[None]] = set()
        self._lock = asyncio.Lock()

    async def run(self, key: K, factory: Callable[[], Awaitable[T]]) -> tuple[T, bool]:
        async def produce(_keys: tuple[K, ...]) -> Mapping[K, T]:
            return {key: await factory()}

        results = await self.run_many((key,), produce)
        return results[key]

    async def run_many(
        self,
        keys: Collection[K],
        factory: Callable[[tuple[K, ...]], Awaitable[Mapping[K, T]]],
    ) -> dict[K, tuple[T, bool]]:
        unique_keys = tuple(dict.fromkeys(keys))
        if not unique_keys:
            return {}

        futures: dict[K, tuple[asyncio.Future[T], bool]] = {}
        owned: list[K] = []
        loop = asyncio.get_running_loop()

        async with self._lock:
            for key in unique_keys:
                future = self._tasks.get(key)
                joined_existing = future is not None
                if future is None:
                    future = loop.create_future()
                    future.add_done_callback(self._consume_unobserved_exception)
                    self._tasks[key] = future
                    owned.append(key)
                futures[key] = (future, joined_existing)

        if owned:
            producer = asyncio.create_task(self._produce(tuple(owned), factory))
            self._producer_tasks.add(producer)
            producer.add_done_callback(self._producer_tasks.discard)

        async def wait_one(key: K, future: asyncio.Future[T], joined: bool) -> tuple[K, T, bool]:
            return key, await asyncio.shield(future), joined

        completed = await asyncio.gather(
            *(wait_one(key, future, joined) for key, (future, joined) in futures.items())
        )
        return {key: (value, joined) for key, value, joined in completed}

    async def in_flight(self) -> int:
        async with self._lock:
            return len(self._tasks)

    async def _produce(
        self,
        owned: tuple[K, ...],
        factory: Callable[[tuple[K, ...]], Awaitable[Mapping[K, T]]],
    ) -> None:
        try:
            produced = await factory(owned)
            missing = [key for key in owned if key not in produced]
            if missing:
                rendered = ", ".join(str(key) for key in missing)
                raise RuntimeError(f"single-flight batch factory omitted owned keys: {rendered}")
            for key in owned:
                await self._complete(key, value=produced[key])
        except BaseException as error:
            for key in owned:
                await self._complete(key, error=error)

    async def _complete(
        self,
        key: K,
        *,
        value: T | None = None,
        error: BaseException | None = None,
    ) -> None:
        async with self._lock:
            future = self._tasks.pop(key, None)

        if future is None or future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(value)  # type: ignore[arg-type]

    @staticmethod
    def _consume_unobserved_exception(future: asyncio.Future[T]) -> None:
        if future.cancelled():
            return
        try:
            future.exception()
        except BaseException:
            return
