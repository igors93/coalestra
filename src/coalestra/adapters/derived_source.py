from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Collection
from typing import Any

from coalestra.core.models import FetchContext, ResourceKey, Snapshot, SourcePayload

DerivedSupportsFunction = Callable[[ResourceKey], bool]
DependenciesFunction = Callable[[ResourceKey], Collection[ResourceKey]]
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
        self._dependencies = dependencies
        self._deriver = deriver

    def supports(self, key: ResourceKey) -> bool:
        return bool(self._supports(key))

    def dependencies(self, key: ResourceKey) -> Collection[ResourceKey]:
        return tuple(dict.fromkeys(self._dependencies(key)))

    async def derive(
        self,
        key: ResourceKey,
        dependencies: Snapshot,
        context: FetchContext,
    ) -> SourcePayload[Any]:
        if inspect.iscoroutinefunction(self._deriver):
            result = await self._deriver(key, dependencies, context)
        else:
            result = await asyncio.to_thread(self._deriver, key, dependencies, context)
            if inspect.isawaitable(result):
                result = await result

        if isinstance(result, SourcePayload):
            return result
        return SourcePayload(value=result)
