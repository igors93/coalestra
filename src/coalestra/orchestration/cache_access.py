from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from dataclasses import replace
from typing import Any

from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.models import CacheLookup, FreshnessPolicy, ResourceKey, SnapshotValue
from coalestra.core.protocols import AsyncCache, BatchAsyncCache, Clock


class CacheAccess:
    """Coordinate cache reads, writes, and snapshot-safe cached copies."""

    def __init__(self, *, cache: AsyncCache, clock: Clock) -> None:
        self.cache = cache
        self.clock = clock

    async def get_many(
        self,
        keys: Collection[ResourceKey],
        *,
        now: float,
        policies: Mapping[ResourceKey, FreshnessPolicy],
        diagnostics: DiagnosticsCollector,
    ) -> Mapping[ResourceKey, CacheLookup]:
        unique = tuple(dict.fromkeys(keys))
        if not unique:
            return {}
        if isinstance(self.cache, BatchAsyncCache):
            diagnostics.cache_batch_reads += 1
            return await self.cache.get_many(unique, now=now, policies=policies)
        completed = await asyncio.gather(
            *(self.cache.get(key, now=now, policy=policies[key]) for key in unique)
        )
        return dict(zip(unique, completed, strict=True))

    async def set_many(
        self,
        values: Collection[SnapshotValue[Any]],
        *,
        diagnostics: DiagnosticsCollector,
    ) -> None:
        unique = tuple({value.key: value for value in values}.values())
        if not unique:
            return
        if isinstance(self.cache, BatchAsyncCache):
            diagnostics.cache_batch_writes += 1
            await self.cache.set_many(unique)
            return
        await asyncio.gather(*(self.cache.set(value) for value in unique))

    def cached_copy(
        self,
        value: SnapshotValue[Any],
        *,
        stale: bool,
        extra_metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotValue[Any]:
        now = self.clock.now()
        return replace(
            value,
            fetched_at=now,
            age_seconds=max(0.0, now - value.observed_at),
            stale=stale,
            from_cache=True,
            latency_ms=0.0,
            metadata={**value.metadata, **dict(extra_metadata or {})},
        )
