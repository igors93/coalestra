from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from coalestra.core.errors import SnapshotDeadlineExceededError

T = TypeVar("T")


def remaining_deadline_seconds(
    deadline_monotonic: float | None,
    *,
    monotonic: Callable[[], float],
    operation: str,
) -> float | None:
    """Return remaining deadline time or raise when the budget is exhausted."""

    if deadline_monotonic is None:
        return None

    remaining = deadline_monotonic - monotonic()
    if remaining <= 0:
        raise SnapshotDeadlineExceededError(f"snapshot deadline exceeded before {operation}")
    return remaining


async def await_with_deadline(
    operation: Callable[[], Awaitable[T]],
    *,
    deadline_monotonic: float | None,
    monotonic: Callable[[], float],
    operation_name: str,
) -> T:
    """Await one cancellable operation within an absolute snapshot deadline."""

    timeout_seconds = remaining_deadline_seconds(
        deadline_monotonic,
        monotonic=monotonic,
        operation=operation_name,
    )
    if timeout_seconds is None:
        return await operation()

    task = asyncio.ensure_future(operation())
    try:
        completed, _pending = await asyncio.wait((task,), timeout=timeout_seconds)
    except BaseException:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise

    if task in completed:
        result = await task
        remaining_deadline_seconds(
            deadline_monotonic,
            monotonic=monotonic,
            operation=operation_name,
        )
        return result

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    raise SnapshotDeadlineExceededError(f"snapshot deadline exceeded while {operation_name}")
