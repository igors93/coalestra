from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from coalestra.core.models import FetchContext, ResourceKey, SourcePayload

SupportsFunction = Callable[[ResourceKey], bool]
FetcherResult = SourcePayload[Any] | Any
FetcherFunction = Callable[[ResourceKey, FetchContext], FetcherResult | Awaitable[FetcherResult]]


class CallableSource:
    """Adapts synchronous or asynchronous callables to the SnapshotSource protocol."""

    def __init__(
        self,
        *,
        name: str,
        priority: int,
        supports: SupportsFunction,
        fetcher: FetcherFunction,
        timeout_seconds: float | None = None,
    ) -> None:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("source name cannot be empty")
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.name = normalized_name
        self.priority = int(priority)
        self.timeout_seconds = timeout_seconds
        self._supports = supports
        self._fetcher = fetcher

    def supports(self, key: ResourceKey) -> bool:
        return bool(self._supports(key))

    async def fetch(self, key: ResourceKey, context: FetchContext) -> SourcePayload[Any]:
        if inspect.iscoroutinefunction(self._fetcher):
            result = await self._fetcher(key, context)
        else:
            result = await asyncio.to_thread(self._fetcher, key, context)
            if inspect.isawaitable(result):
                result = await result

        if isinstance(result, SourcePayload):
            return result
        return SourcePayload(value=result)
