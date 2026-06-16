from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from coalestra.core.isolation import PayloadCopier, PayloadIsolator
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


@dataclass(frozen=True)
class _CapturedWriteResult:
    status: CacheWriteStatus
    value: SnapshotValue[Any]
    previous: SnapshotValue[Any] | None


class AsyncMemoryCache:
    """Concurrency-safe LRU cache with authority-aware isolated writes."""

    validates_dependency_versions = True
    validates_source_authority = True

    def __init__(
        self,
        *,
        max_entries: int | None = 10_000,
        payload_copier: PayloadCopier | None = None,
    ) -> None:
        if max_entries is not None and max_entries < 1:
            raise ValueError("max_entries must be at least 1 or None")
        self.max_entries = max_entries
        self._payload_isolator = PayloadIsolator(payload_copier)
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

        captured: dict[ResourceKey, CacheLookup] = {}
        async with self._lock:
            for key in unique:
                value = self._entries.get(key)
                if value is None:
                    self._misses += 1
                    captured[key] = CacheLookup(
                        value=None,
                        fresh=False,
                        usable_stale=False,
                        age_seconds=None,
                    )
                    continue

                age = max(0.0, now - value.observed_at)
                policy = policies[key]
                if not self._dependencies_current_locked(value, visiting=frozenset()):
                    self._entries.pop(key, None)
                    self._misses += 1
                    self._invalidations += 1
                    captured[key] = CacheLookup(
                        value=None,
                        fresh=False,
                        usable_stale=False,
                        age_seconds=age,
                    )
                    continue
                if age > policy.max_stale_seconds:
                    self._entries.pop(key, None)
                    self._misses += 1
                    self._expirations += 1
                    captured[key] = CacheLookup(
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
                captured[key] = CacheLookup(
                    value=value,
                    fresh=fresh,
                    usable_stale=True,
                    age_seconds=age,
                )

        return {
            key: CacheLookup(
                value=(
                    None
                    if lookup.value is None
                    else self._clone_for_caller(lookup.value, operation="cache read")
                ),
                fresh=lookup.fresh,
                usable_stale=lookup.usable_stale,
                age_seconds=lookup.age_seconds,
            )
            for key, lookup in captured.items()
        }

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

        prepared: dict[ResourceKey, SnapshotValue[Any]] = {}
        captured: dict[ResourceKey, _CapturedWriteResult]

        # Copy only candidates that currently qualify for storage, then recheck the
        # whole batch under the lock before committing any entry.
        while True:
            missing: list[SnapshotValue[Any]] = []
            async with self._lock:
                statuses: dict[ResourceKey, CacheWriteStatus] = {}
                for candidate in candidates.values():
                    status = self._write_status(
                        self._entries.get(candidate.key),
                        candidate,
                        force=force,
                        replace_equal=replace_equal,
                    )
                    statuses[candidate.key] = status
                    if status is CacheWriteStatus.STORED and candidate.key not in prepared:
                        missing.append(candidate)

                if not missing:
                    captured = {}
                    for candidate in candidates.values():
                        previous = self._entries.get(candidate.key)
                        status = statuses[candidate.key]
                        if status is CacheWriteStatus.STORED:
                            stored = prepared[candidate.key]
                            self._entries[candidate.key] = stored
                            self._entries.move_to_end(candidate.key)
                            self._sets += 1
                            current = stored
                        else:
                            assert previous is not None
                            current = previous

                        captured[candidate.key] = _CapturedWriteResult(
                            status=status,
                            value=current,
                            previous=previous,
                        )

                    self._enforce_limit_locked()
                    break

            for candidate in missing:
                prepared[candidate.key] = self._payload_isolator.clone_snapshot_value(
                    candidate,
                    context=f"cache storage for {candidate.key}",
                )

        results = {
            key: CacheWriteResult(
                status=result.status,
                value=self._clone_for_caller(result.value, operation="cache write result"),
                previous=(
                    None
                    if result.previous is None
                    else self._clone_for_caller(
                        result.previous,
                        operation="cache previous value",
                    )
                ),
            )
            for key, result in captured.items()
        }
        return MappingProxyType(results)

    @staticmethod
    def _select_candidates(
        values: Collection[SnapshotValue[Any]],
    ) -> dict[ResourceKey, SnapshotValue[Any]]:
        selected: dict[ResourceKey, SnapshotValue[Any]] = {}
        for value in values:
            current = selected.get(value.key)
            if current is None or AsyncMemoryCache._candidate_precedes(current, value):
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
        if value.authority_rank < previous.authority_rank:
            return CacheWriteStatus.IGNORED_LOWER_AUTHORITY
        if value.authority_rank > previous.authority_rank:
            return CacheWriteStatus.STORED
        if value.observed_at < previous.observed_at:
            return CacheWriteStatus.IGNORED_OLDER
        if value.observed_at == previous.observed_at and not replace_equal:
            return CacheWriteStatus.IGNORED_DUPLICATE
        return CacheWriteStatus.STORED

    @staticmethod
    def _candidate_precedes(
        current: SnapshotValue[Any],
        candidate: SnapshotValue[Any],
    ) -> bool:
        if candidate.authority_rank != current.authority_rank:
            return candidate.authority_rank > current.authority_rank
        return candidate.observed_at >= current.observed_at

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

    def _clone_for_caller(
        self,
        value: SnapshotValue[Any],
        *,
        operation: str,
    ) -> SnapshotValue[Any]:
        return self._payload_isolator.clone_snapshot_value(
            value,
            context=f"{operation} for {value.key}",
        )

    def _dependencies_current_locked(
        self,
        value: SnapshotValue[Any],
        *,
        visiting: frozenset[ResourceKey],
    ) -> bool:
        if not value.dependency_versions:
            return True
        if value.key in visiting:
            return False

        path = visiting | {value.key}
        for dependency_key, expected_version in value.dependency_versions.items():
            dependency = self._entries.get(dependency_key)
            if dependency is None or dependency.version != expected_version:
                return False
            if not self._dependencies_current_locked(dependency, visiting=path):
                return False
        return True

    def _enforce_limit_locked(self) -> None:
        if self.max_entries is None:
            return
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)
            self._evictions += 1
