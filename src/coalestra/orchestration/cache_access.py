from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from dataclasses import replace
from typing import Any

from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.isolation import PayloadIsolator
from coalestra.core.models import CacheLookup, FreshnessPolicy, ResourceKey, SnapshotValue
from coalestra.core.protocols import AsyncCache, BatchAsyncCache, Clock


class CacheAccess:
    """Coordinate cache reads, writes, and isolated cached copies."""

    def __init__(
        self,
        *,
        cache: AsyncCache,
        clock: Clock,
        payload_isolator: PayloadIsolator,
    ) -> None:
        self.cache = cache
        self.clock = clock
        self.payload_isolator = payload_isolator

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
            lookups = await self.cache.get_many(unique, now=now, policies=policies)
        else:
            completed = await asyncio.gather(
                *(self.cache.get(key, now=now, policy=policies[key]) for key in unique)
            )
            lookups = dict(zip(unique, completed, strict=True))

        return {key: self._isolated_lookup(key, lookups[key]) for key in unique}

    async def set_many(
        self,
        values: Collection[SnapshotValue[Any]],
        *,
        diagnostics: DiagnosticsCollector,
    ) -> None:
        unique = tuple({value.key: value for value in values}.values())
        if not unique:
            return
        isolated = tuple(
            self.payload_isolator.clone_snapshot_value(
                value,
                context=f"cache write for {value.key}",
            )
            for value in unique
        )
        if isinstance(self.cache, BatchAsyncCache):
            diagnostics.cache_batch_writes += 1
            await self.cache.set_many(isolated)
            return
        await asyncio.gather(*(self.cache.set(value) for value in isolated))

    def cached_copy(
        self,
        value: SnapshotValue[Any],
        *,
        stale: bool,
        extra_metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotValue[Any]:
        now = self.clock.now()
        metadata = {**value.metadata, **dict(extra_metadata or {})}
        return self.payload_isolator.clone_snapshot_value(
            value,
            context=f"cached snapshot value for {value.key}",
            fetched_at=now,
            age_seconds=max(0.0, now - value.observed_at),
            stale=stale,
            from_cache=True,
            latency_ms=0.0,
            metadata=metadata,
        )

    def _isolated_lookup(
        self,
        key: ResourceKey,
        lookup: CacheLookup,
    ) -> CacheLookup:
        if lookup.value is None:
            return lookup
        return replace(
            lookup,
            value=self.payload_isolator.clone_snapshot_value(
                lookup.value,
                context=f"cache read for {key}",
            ),
        )
