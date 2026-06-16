from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from coalestra.core.models import FetchContext, ResourceKey, SourcePayload
from coalestra.resilience.policy import SourceResiliencePolicy

SupportsFunction = Callable[[ResourceKey], bool]
FetcherResult = SourcePayload[Any] | Any
FetcherFunction = Callable[
    [ResourceKey, FetchContext],
    FetcherResult | Awaitable[FetcherResult],
]


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
        queue_timeout_seconds: float | None = None,
        max_concurrency: int | None = None,
        resilience_policy: SourceResiliencePolicy | None = None,
        cache_supports: bool = True,
        run_sync_in_thread: bool = True,
        blocking_io: bool = False,
        transport_timeout_seconds: float | None = None,
    ) -> None:
        normalized_name = name.strip()

        if not normalized_name:
            raise ValueError("source name cannot be empty")

        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        if queue_timeout_seconds is not None and queue_timeout_seconds <= 0:
            raise ValueError("queue_timeout_seconds must be positive")

        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1 or None")

        if not isinstance(run_sync_in_thread, bool):
            raise TypeError("run_sync_in_thread must be a boolean")

        if not isinstance(blocking_io, bool):
            raise TypeError("blocking_io must be a boolean")

        if transport_timeout_seconds is not None:
            if isinstance(transport_timeout_seconds, bool) or not isinstance(
                transport_timeout_seconds, (int, float)
            ):
                raise TypeError("transport_timeout_seconds must be a number or None")
            if transport_timeout_seconds <= 0:
                raise ValueError("transport_timeout_seconds must be positive")

        if not blocking_io and transport_timeout_seconds is not None:
            raise ValueError("transport_timeout_seconds requires blocking_io=True")

        self.name = normalized_name
        self.priority = int(priority)
        self.timeout_seconds = timeout_seconds
        self.queue_timeout_seconds = (
            timeout_seconds if queue_timeout_seconds is None else queue_timeout_seconds
        )
        self.max_concurrency = None if max_concurrency is None else int(max_concurrency)
        self.resilience_policy = resilience_policy
        self.cache_supports = bool(cache_supports)
        self.run_sync_in_thread = run_sync_in_thread
        self.blocking_io = blocking_io
        self.blocking_io_offloaded = run_sync_in_thread and not inspect.iscoroutinefunction(fetcher)
        self.transport_timeout_seconds = (
            None if transport_timeout_seconds is None else float(transport_timeout_seconds)
        )
        self._supports = supports
        self._fetcher = fetcher

    def supports(self, key: ResourceKey) -> bool:
        return bool(self._supports(key))

    async def fetch(
        self,
        key: ResourceKey,
        context: FetchContext,
    ) -> SourcePayload[Any]:
        if inspect.iscoroutinefunction(self._fetcher):
            result = await self._fetcher(key, context)
        elif self.run_sync_in_thread:
            result = await asyncio.to_thread(
                self._fetcher,
                key,
                context,
            )
        else:
            result = self._fetcher(key, context)

        if inspect.isawaitable(result):
            result = await result

        if isinstance(result, SourcePayload):
            return result

        return SourcePayload(value=result)
