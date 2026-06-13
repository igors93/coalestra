from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from coalestra.core.models import FetchContext, ResourceKey, SourcePayload
from coalestra.resilience.policy import SourceResiliencePolicy

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
        max_concurrency: int | None = None,
        resilience_policy: SourceResiliencePolicy | None = None,
        cache_supports: bool = True,
        run_sync_in_thread: bool = True,
    ) -> None:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("source name cannot be empty")
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1 or None")
        self.name = normalized_name
        self.priority = int(priority)
        self.timeout_seconds = timeout_seconds
        self.max_concurrency = None if max_concurrency is None else int(max_concurrency)
        self.resilience_policy = resilience_policy
        self.cache_supports = bool(cache_supports)
        self.run_sync_in_thread = bool(run_sync_in_thread)
        self._supports = supports
        self._fetcher = fetcher

    def supports(self, key: ResourceKey) -> bool:
        return bool(self._supports(key))

    async def fetch(self, key: ResourceKey, context: FetchContext) -> SourcePayload[Any]:
        if inspect.iscoroutinefunction(self._fetcher):
            result = await self._fetcher(key, context)
        elif self.run_sync_in_thread:
            result = await asyncio.to_thread(self._fetcher, key, context)
        else:
            result = self._fetcher(key, context)
        if inspect.isawaitable(result):
            result = await result

        if isinstance(result, SourcePayload):
            return result
        return SourcePayload(value=result)
