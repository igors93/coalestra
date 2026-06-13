from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Collection, Mapping
from typing import Any

from coalestra.core.errors import SourceProtocolError
from coalestra.core.models import FetchContext, ResourceKey, SourcePayload
from coalestra.resilience.policy import SourceResiliencePolicy

BatchSupportsFunction = Callable[[ResourceKey], bool]
BatchFetcherResult = Mapping[ResourceKey, SourcePayload[Any] | Any]
BatchFetcherFunction = Callable[
    [Collection[ResourceKey], FetchContext],
    BatchFetcherResult | Awaitable[BatchFetcherResult],
]


class CallableBatchSource:
    """Adapts synchronous or asynchronous batch callables to BatchSnapshotSource."""

    def __init__(
        self,
        *,
        name: str,
        priority: int,
        supports: BatchSupportsFunction,
        fetcher: BatchFetcherFunction,
        timeout_seconds: float | None = None,
        max_concurrency: int | None = None,
        resilience_policy: SourceResiliencePolicy | None = None,
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
        self._supports = supports
        self._fetcher = fetcher

    def supports(self, key: ResourceKey) -> bool:
        return bool(self._supports(key))

    async def fetch_many(
        self,
        keys: Collection[ResourceKey],
        context: FetchContext,
    ) -> Mapping[ResourceKey, SourcePayload[Any]]:
        requested = tuple(dict.fromkeys(keys))
        if not requested:
            return {}

        if inspect.iscoroutinefunction(self._fetcher):
            result = await self._fetcher(requested, context)
        else:
            result = await asyncio.to_thread(self._fetcher, requested, context)
            if inspect.isawaitable(result):
                result = await result

        if not isinstance(result, Mapping):
            raise SourceProtocolError(
                f"batch source {self.name} must return a mapping, got {type(result).__name__}"
            )

        requested_set = set(requested)
        unexpected = [key for key in result if key not in requested_set]
        if unexpected:
            rendered = ", ".join(str(key) for key in unexpected)
            raise SourceProtocolError(
                f"batch source {self.name} returned unrequested resources: {rendered}"
            )

        return {
            key: value if isinstance(value, SourcePayload) else SourcePayload(value=value)
            for key, value in result.items()
        }
