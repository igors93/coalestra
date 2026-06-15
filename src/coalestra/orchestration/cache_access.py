from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import Any

from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.isolation import PayloadIsolator
from coalestra.core.models import (
    CacheLookup,
    CacheWriteResult,
    FreshnessPolicy,
    ResourceKey,
    SnapshotValue,
)
from coalestra.core.protocols import (
    AsyncCache,
    AtomicAsyncCache,
    BatchAsyncCache,
    BatchAtomicAsyncCache,
    Clock,
)


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
        self._all_values_policy = FreshnessPolicy(
            ttl_seconds=float("inf"),
            max_stale_seconds=float("inf"),
        )

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

        isolated = {key: self._isolated_lookup(key, lookups[key]) for key in unique}
        if bool(getattr(self.cache, "validates_dependency_versions", False)):
            return isolated
        return await self._filter_invalid_dependencies(isolated, now=now)

    async def set_many(
        self,
        values: Collection[SnapshotValue[Any]],
        *,
        diagnostics: DiagnosticsCollector,
    ) -> Mapping[ResourceKey, CacheWriteResult] | None:
        unique = tuple({value.key: value for value in values}.values())
        if not unique:
            return MappingProxyType({})
        isolated = tuple(
            self.payload_isolator.clone_snapshot_value(
                value,
                context=f"cache write for {value.key}",
            )
            for value in unique
        )
        if isinstance(self.cache, BatchAtomicAsyncCache):
            diagnostics.cache_batch_writes += 1
            return await self.cache.set_many_if_newer(isolated)
        if isinstance(self.cache, AtomicAsyncCache):
            completed = await asyncio.gather(
                *(self.cache.set_if_newer(value) for value in isolated)
            )
            return MappingProxyType({result.value.key: result for result in completed})
        if isinstance(self.cache, BatchAsyncCache):
            diagnostics.cache_batch_writes += 1
            await self.cache.set_many(isolated)
            return None
        await asyncio.gather(*(self.cache.set(value) for value in isolated))
        return None

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

    async def _filter_invalid_dependencies(
        self,
        lookups: Mapping[ResourceKey, CacheLookup],
        *,
        now: float,
    ) -> Mapping[ResourceKey, CacheLookup]:
        cached_values: dict[ResourceKey, SnapshotValue[Any] | None] = {}
        invalid: set[ResourceKey] = set()
        filtered = dict(lookups)

        for key, lookup in lookups.items():
            value = lookup.value
            if value is None or not value.dependency_versions:
                continue
            if not await self._dependencies_current(
                value,
                now=now,
                cached_values=cached_values,
                visiting=frozenset(),
                invalid=invalid,
            ):
                invalid.add(key)
                filtered[key] = CacheLookup(
                    value=None,
                    fresh=False,
                    usable_stale=False,
                    age_seconds=lookup.age_seconds,
                )

        if invalid:
            await self._invalidate_many(invalid)
        return filtered

    async def _dependencies_current(
        self,
        value: SnapshotValue[Any],
        *,
        now: float,
        cached_values: dict[ResourceKey, SnapshotValue[Any] | None],
        visiting: frozenset[ResourceKey],
        invalid: set[ResourceKey],
    ) -> bool:
        if not value.dependency_versions:
            return True
        if value.key in visiting:
            invalid.add(value.key)
            return False

        path = visiting | {value.key}
        for dependency_key, expected_version in value.dependency_versions.items():
            dependency = await self._read_unfiltered(
                dependency_key,
                now=now,
                cached_values=cached_values,
            )
            if dependency is None or dependency.version != expected_version:
                return False
            if not await self._dependencies_current(
                dependency,
                now=now,
                cached_values=cached_values,
                visiting=path,
                invalid=invalid,
            ):
                invalid.add(dependency.key)
                return False
        return True

    async def _read_unfiltered(
        self,
        key: ResourceKey,
        *,
        now: float,
        cached_values: dict[ResourceKey, SnapshotValue[Any] | None],
    ) -> SnapshotValue[Any] | None:
        if key in cached_values:
            return cached_values[key]
        lookup = await self.cache.get(
            key,
            now=now,
            policy=self._all_values_policy,
        )
        cached_values[key] = lookup.value
        return lookup.value

    async def _invalidate_many(self, keys: Collection[ResourceKey]) -> None:
        unique = tuple(dict.fromkeys(keys))
        if isinstance(self.cache, BatchAsyncCache):
            await self.cache.invalidate_many(unique)
            return
        await asyncio.gather(*(self.cache.invalidate(key) for key in unique))

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
