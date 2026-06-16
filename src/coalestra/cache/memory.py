from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass
from functools import partial
from types import MappingProxyType
from typing import Any, TypeVar

from coalestra.concurrency.dispatch import run_bounded
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

_CopyItem = TypeVar("_CopyItem")
_CopyResult = TypeVar("_CopyResult")


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


class _AsyncPayloadCopyRunner:
    """Run synchronous payload copies without blocking the event loop when configured."""

    def __init__(self, *, run_in_thread: bool, max_concurrency: int) -> None:
        self.run_in_thread = run_in_thread
        self.max_concurrency = max_concurrency
        self._limiter = asyncio.Semaphore(max_concurrency)

    async def run(self, operation: Callable[[], _CopyResult]) -> _CopyResult:
        if not self.run_in_thread:
            return operation()

        await self._limiter.acquire()
        try:
            worker = asyncio.create_task(asyncio.to_thread(operation))
        except BaseException:
            self._limiter.release()
            raise

        released = False

        def release_slot(_task: asyncio.Task[_CopyResult]) -> None:
            nonlocal released
            if released:
                return
            released = True
            if not _task.cancelled():
                _task.exception()
            self._limiter.release()

        # A thread cannot be cancelled after it starts. Keep its capacity slot reserved
        # until the underlying copy really finishes, even if the caller is cancelled.
        worker.add_done_callback(release_slot)
        try:
            return await asyncio.shield(worker)
        finally:
            if worker.done():
                release_slot(worker)


class AsyncMemoryCache:
    """Concurrency-safe LRU cache with authority-aware isolated writes."""

    validates_dependency_versions = True
    validates_source_authority = True

    def __init__(
        self,
        *,
        max_entries: int | None = 10_000,
        payload_copier: PayloadCopier | None = None,
        run_payload_copies_in_thread: bool | None = None,
        max_copy_concurrency: int = 4,
    ) -> None:
        if max_entries is not None and max_entries < 1:
            raise ValueError("max_entries must be at least 1 or None")
        if run_payload_copies_in_thread is not None and not isinstance(
            run_payload_copies_in_thread, bool
        ):
            raise TypeError("run_payload_copies_in_thread must be a boolean or None")
        if isinstance(max_copy_concurrency, bool) or not isinstance(max_copy_concurrency, int):
            raise TypeError("max_copy_concurrency must be an integer")
        if max_copy_concurrency < 1:
            raise ValueError("max_copy_concurrency must be at least 1")
        resolved_copy_offload = (
            payload_copier is None
            if run_payload_copies_in_thread is None
            else run_payload_copies_in_thread
        )
        self.max_entries = max_entries
        self.run_payload_copies_in_thread = resolved_copy_offload
        self.max_copy_concurrency = max_copy_concurrency
        self._payload_isolator = PayloadIsolator(payload_copier)
        self._copy_runner = _AsyncPayloadCopyRunner(
            run_in_thread=resolved_copy_offload,
            max_concurrency=max_copy_concurrency,
        )
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

        async def clone_lookup(
            item: tuple[ResourceKey, CacheLookup],
        ) -> tuple[ResourceKey, CacheLookup]:
            key, lookup = item
            copied_value = (
                None
                if lookup.value is None
                else await self._clone_for_caller(lookup.value, operation="cache read")
            )
            return (
                key,
                CacheLookup(
                    value=copied_value,
                    fresh=lookup.fresh,
                    usable_stale=lookup.usable_stale,
                    age_seconds=lookup.age_seconds,
                ),
            )

        copied = await self._run_copy_batch(tuple(captured.items()), clone_lookup)
        return dict(copied)

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

            async def prepare_candidate(
                candidate: SnapshotValue[Any],
            ) -> tuple[ResourceKey, SnapshotValue[Any]]:
                stored = await self._clone_snapshot_value(
                    candidate,
                    context=f"cache storage for {candidate.key}",
                )
                return candidate.key, stored

            prepared.update(await self._run_copy_batch(tuple(missing), prepare_candidate))

        async def clone_result(
            item: tuple[ResourceKey, _CapturedWriteResult],
        ) -> tuple[ResourceKey, CacheWriteResult]:
            key, result = item
            copied_value = await self._clone_for_caller(
                result.value,
                operation="cache write result",
            )
            copied_previous = (
                None
                if result.previous is None
                else await self._clone_for_caller(
                    result.previous,
                    operation="cache previous value",
                )
            )
            return (
                key,
                CacheWriteResult(
                    status=result.status,
                    value=copied_value,
                    previous=copied_previous,
                ),
            )

        copied_results = await self._run_copy_batch(tuple(captured.items()), clone_result)
        return MappingProxyType(dict(copied_results))

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

    async def _run_copy_batch(
        self,
        items: Collection[_CopyItem],
        operation: Callable[[_CopyItem], Awaitable[_CopyResult]],
    ) -> tuple[_CopyResult, ...]:
        if not items:
            return ()
        if self.run_payload_copies_in_thread:
            return await run_bounded(
                items,
                operation,
                max_tasks=self.max_copy_concurrency,
            )
        return tuple([await operation(item) for item in items])

    async def _clone_snapshot_value(
        self,
        value: SnapshotValue[Any],
        *,
        context: str,
    ) -> SnapshotValue[Any]:
        return await self._copy_runner.run(
            partial(
                self._payload_isolator.clone_snapshot_value,
                value,
                context=context,
            )
        )

    async def _clone_for_caller(
        self,
        value: SnapshotValue[Any],
        *,
        operation: str,
    ) -> SnapshotValue[Any]:
        return await self._clone_snapshot_value(
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
