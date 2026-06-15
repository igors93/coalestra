from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from coalestra.core.keys import ResourceKey
from coalestra.core.models import (
    CacheLookup,
    CacheWriteResult,
    CacheWriteStatus,
    FreshnessPolicy,
    SnapshotValue,
)
from coalestra.core.protocols import FreshnessPolicyProvider


@dataclass(frozen=True)
class CacheStats:
    """Operational counters for :class:`AsyncMemoryCache`."""

    size: int
    max_entries: int | None
    hits: int
    misses: int
    fresh_hits: int
    stale_hits: int
    sets: int
    invalidations: int
    evictions: int
    expirations: int


class AsyncMemoryCache:
    """Concurrency-safe in-memory LRU cache with atomic monotonic writes."""

    def __init__(self, *, max_entries: int | None = 10_000) -> None:
        if max_entries is not None and max_entries < 1:
            raise ValueError("max_entries must be at least 1 or None")
        self.max_entries = max_entries
        self._entries: OrderedDict[ResourceKey, SnapshotValue[Any]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0
        self._fresh_hits = 0
        self._stale_hits = 0
        self._sets = 0
        self._invalidations = 0
        self._evictions = 0
        self._expirations = 0

    async def get(
        self,
        key: ResourceKey,
        *,
        now: float,
        policy: FreshnessPolicy,
    ) -> CacheLookup:
        lookups = await self.get_many((key,), now=now, policies={key: policy})
        return lookups[key]

    async def get_many(
        self,
        keys: Collection[ResourceKey],
        *,
        now: float,
        policies: Mapping[ResourceKey, FreshnessPolicy],
    ) -> Mapping[ResourceKey, CacheLookup]:
        unique = tuple(dict.fromkeys(keys))
        missing_policies = [key for key in unique if key not in policies]
        if missing_policies:
            rendered = ", ".join(str(key) for key in missing_policies)
            raise KeyError(f"missing freshness policies for: {rendered}")

        results: dict[ResourceKey, CacheLookup] = {}
        async with self._lock:
            for key in unique:
                value = self._entries.get(key)
                if value is None:
                    self._misses += 1
                    results[key] = CacheLookup(
                        value=None,
                        fresh=False,
                        usable_stale=False,
                        age_seconds=None,
                    )
                    continue

                age = max(0.0, now - value.observed_at)
                policy = policies[key]
                if age > policy.max_stale_seconds:
                    self._entries.pop(key, None)
                    self._misses += 1
                    self._expirations += 1
                    results[key] = CacheLookup(
                        value=None,
                        fresh=False,
                        usable_stale=False,
                        age_seconds=age,
                    )
                    continue

                self._entries.move_to_end(key)
                fresh = age <= policy.ttl_seconds
                self._hits += 1
                if fresh:
                    self._fresh_hits += 1
                else:
                    self._stale_hits += 1
                results[key] = CacheLookup(
                    value=value,
                    fresh=fresh,
                    usable_stale=True,
                    age_seconds=age,
                )
        return results

    async def set(self, value: SnapshotValue[Any]) -> None:
        await self.set_if_newer(value)

    async def set_many(self, values: Collection[SnapshotValue[Any]]) -> None:
        await self.set_many_if_newer(values)

    async def set_if_newer(
        self,
        value: SnapshotValue[Any],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> CacheWriteResult:
        results = await self.set_many_if_newer(
            (value,),
            force=force,
            replace_equal=replace_equal,
        )
        return results[value.key]

    async def set_many_if_newer(
        self,
        values: Collection[SnapshotValue[Any]],
        *,
        force: bool = False,
        replace_equal: bool = False,
    ) -> Mapping[ResourceKey, CacheWriteResult]:
        candidates = self._select_candidates(values)
        if not candidates:
            return MappingProxyType({})

        results: dict[ResourceKey, CacheWriteResult] = {}
        async with self._lock:
            for value in candidates.values():
                previous = self._entries.get(value.key)
                status = self._write_status(
                    previous,
                    value,
                    force=force,
                    replace_equal=replace_equal,
                )

                if status is CacheWriteStatus.STORED:
                    self._entries[value.key] = value
                    self._entries.move_to_end(value.key)
                    self._sets += 1
                    current = value
                else:
                    assert previous is not None
                    current = previous

                results[value.key] = CacheWriteResult(
                    status=status,
                    value=current,
                    previous=previous,
                )

            self._enforce_limit_locked()

        return MappingProxyType(results)

    @staticmethod
    def _select_candidates(
        values: Collection[SnapshotValue[Any]],
    ) -> dict[ResourceKey, SnapshotValue[Any]]:
        selected: dict[ResourceKey, SnapshotValue[Any]] = {}
        for value in values:
            current = selected.get(value.key)
            if current is None or value.observed_at >= current.observed_at:
                selected[value.key] = value
        return selected

    @staticmethod
    def _write_status(
        previous: SnapshotValue[Any] | None,
        value: SnapshotValue[Any],
        *,
        force: bool,
        replace_equal: bool,
    ) -> CacheWriteStatus:
        if force or previous is None:
            return CacheWriteStatus.STORED
        if value.observed_at < previous.observed_at:
            return CacheWriteStatus.IGNORED_OLDER
        if value.observed_at == previous.observed_at and not replace_equal:
            return CacheWriteStatus.IGNORED_DUPLICATE
        return CacheWriteStatus.STORED

    async def invalidate(self, key: ResourceKey) -> None:
        await self.invalidate_many((key,))

    async def invalidate_many(self, keys: Collection[ResourceKey]) -> None:
        unique = tuple(dict.fromkeys(keys))
        async with self._lock:
            for key in unique:
                if self._entries.pop(key, None) is not None:
                    self._invalidations += 1

    async def invalidate_matching(
        self,
        predicate: Callable[[ResourceKey], bool],
    ) -> tuple[ResourceKey, ...]:
        """Invalidate every key accepted by ``predicate`` and return removed keys."""

        async with self._lock:
            removed = tuple(key for key in self._entries if predicate(key))
            for key in removed:
                self._entries.pop(key, None)
            self._invalidations += len(removed)
            return removed

    async def invalidate_namespace(
        self,
        namespace: str,
        *,
        name: str | None = None,
        subject: str | None = None,
    ) -> tuple[ResourceKey, ...]:
        """Invalidate a namespace, optionally narrowed by exact name and subject."""

        return await self.invalidate_matching(
            lambda key: (
                key.namespace == namespace
                and (name is None or key.name == name)
                and (subject is None or key.subject == subject)
            )
        )

    async def prune(
        self,
        *,
        now: float,
        policy_resolver: FreshnessPolicyProvider,
    ) -> tuple[ResourceKey, ...]:
        """Remove entries older than their resource-specific maximum stale window."""

        async with self._lock:
            expired = tuple(
                key
                for key, value in self._entries.items()
                if max(0.0, now - value.observed_at)
                > policy_resolver.resolve(key).max_stale_seconds
            )
            for key in expired:
                self._entries.pop(key, None)
            self._expirations += len(expired)
            return expired

    async def clear(self) -> None:
        async with self._lock:
            removed = len(self._entries)
            self._entries.clear()
            self._invalidations += removed

    async def size(self) -> int:
        async with self._lock:
            return len(self._entries)

    async def stats(self) -> CacheStats:
        async with self._lock:
            return CacheStats(
                size=len(self._entries),
                max_entries=self.max_entries,
                hits=self._hits,
                misses=self._misses,
                fresh_hits=self._fresh_hits,
                stale_hits=self._stale_hits,
                sets=self._sets,
                invalidations=self._invalidations,
                evictions=self._evictions,
                expirations=self._expirations,
            )

    def _enforce_limit_locked(self) -> None:
        if self.max_entries is None:
            return
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)
            self._evictions += 1
