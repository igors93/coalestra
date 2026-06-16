from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Collection
from typing import Any

from coalestra.core.models import (
    FetchContext,
    ResourceKey,
    Snapshot,
    SourcePayload,
)
from coalestra.resilience.policy import SourceResiliencePolicy

DerivedSupportsFunction = Callable[[ResourceKey], bool]
DependenciesFunction = Callable[
    [ResourceKey],
    Collection[ResourceKey],
]
DeriverResult = SourcePayload[Any] | Any
DeriverFunction = Callable[
    [ResourceKey, Snapshot, FetchContext],
    DeriverResult | Awaitable[DeriverResult],
]


class CallableDerivedSource:
    """Adapts a dependency declaration and derivation callable to DerivedSource."""

    def __init__(
        self,
        *,
        name: str,
        priority: int,
        supports: DerivedSupportsFunction,
        dependencies: DependenciesFunction,
        deriver: DeriverFunction,
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
        self.blocking_io_offloaded = run_sync_in_thread and not inspect.iscoroutinefunction(deriver)
        self.transport_timeout_seconds = (
            None if transport_timeout_seconds is None else float(transport_timeout_seconds)
        )
        self._supports = supports
        self._dependencies = dependencies
        self._deriver = deriver

    def supports(self, key: ResourceKey) -> bool:
        return bool(self._supports(key))

    def dependencies(
        self,
        key: ResourceKey,
    ) -> Collection[ResourceKey]:
        return tuple(dict.fromkeys(self._dependencies(key)))

    async def derive(
        self,
        key: ResourceKey,
        dependencies: Snapshot,
        context: FetchContext,
    ) -> SourcePayload[Any]:
        if inspect.iscoroutinefunction(self._deriver):
            result = await self._deriver(
                key,
                dependencies,
                context,
            )
        elif self.run_sync_in_thread:
            result = await asyncio.to_thread(
                self._deriver,
                key,
                dependencies,
                context,
            )
        else:
            result = self._deriver(
                key,
                dependencies,
                context,
            )

        if inspect.isawaitable(result):
            result = await result

        if isinstance(result, SourcePayload):
            return result

        return SourcePayload(value=result)
