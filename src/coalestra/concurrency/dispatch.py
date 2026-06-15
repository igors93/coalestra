from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection
from typing import TypeVar

DispatchItem = TypeVar("DispatchItem")
DispatchResult = TypeVar("DispatchResult")


async def run_bounded(
    items: Collection[DispatchItem],
    operation: Callable[[DispatchItem], Awaitable[DispatchResult]],
    *,
    max_tasks: int,
) -> tuple[DispatchResult, ...]:
    """Run ordered asynchronous work through a fixed worker set.

    The helper creates at most ``max_tasks`` worker tasks regardless of the
    number of input items. Results preserve the original input order. If one
    operation fails or the caller is cancelled, every worker is cancelled and
    awaited before the exception is propagated.
    """

    if max_tasks < 1:
        raise ValueError("max_tasks must be at least 1")

    ordered = tuple(items)
    if not ordered:
        return ()

    worker_count = min(max_tasks, len(ordered))
    results: dict[int, DispatchResult] = {}
    next_index = 0

    async def worker() -> None:
        nonlocal next_index

        while next_index < len(ordered):
            index = next_index
            next_index += 1
            results[index] = await operation(ordered[index])

    workers = tuple(asyncio.create_task(worker()) for _ in range(worker_count))
    try:
        await asyncio.gather(*workers)
    except BaseException:
        for worker_task in workers:
            worker_task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise

    return tuple(results[index] for index in range(len(ordered)))
