from __future__ import annotations

import asyncio
import inspect
from collections.abc import Iterable


async def close_components(components: Iterable[object]) -> None:
    """Close each distinct component once without blocking the event loop."""

    seen: set[int] = set()
    for component in components:
        identity = id(component)
        if identity in seen:
            continue
        seen.add(identity)
        await close_component(component)


async def close_component(component: object) -> None:
    """Close one synchronous or asynchronous component when supported."""

    async_close = getattr(component, "aclose", None)
    if callable(async_close):
        if inspect.iscoroutinefunction(async_close):
            await async_close()
        else:
            result = await asyncio.to_thread(async_close)
            if inspect.isawaitable(result):
                await result
        return

    close = getattr(component, "close", None)
    if callable(close):
        if inspect.iscoroutinefunction(close):
            await close()
        else:
            result = await asyncio.to_thread(close)
            if inspect.isawaitable(result):
                await result
