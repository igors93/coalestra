from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import replace
from typing import Any

from coalestra.concurrency.dispatch import run_bounded
from coalestra.core.deadline import await_with_deadline
from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.health import OperationalHealthTracker
from coalestra.core.isolation import AsyncPayloadIsolator, PayloadIsolator
from coalestra.core.models import (
    CacheLookup,
    CacheWriteResult,
    FetchContext,
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
        async_payload_isolator: AsyncPayloadIsolator,
        max_pending_tasks: int,
        health_tracker: OperationalHealthTracker | None = None,
    ) -> None:
        self.cache = cache
        self.clock = clock
        if max_pending_tasks < 1:
            raise ValueError("max_pending_tasks must be at least 1")
        self.payload_isolator = payload_isolator
        self.async_payload_isolator = async_payload_isolator
        self.max_pending_tasks = int(max_pending_tasks)
        self.health_tracker = health_tracker
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
        context: FetchContext,
    ) -> Mapping[ResourceKey, CacheLookup]:
        unique = tuple(dict.fromkeys(keys))
        if not unique:
            return {}
        if isinstance(self.cache, BatchAsyncCache):
            diagnostics.cache_batch_reads += 1
            batch_cache = self.cache
            lookups = await await_with_deadline(
                lambda: batch_cache.get_many(unique, now=now, policies=policies),
                deadline_monotonic=context.deadline_monotonic,
                monotonic=self.clock.monotonic,
                operation_name="reading cache values",
            )
        else:

            async def read_one(key: ResourceKey) -> CacheLookup:
                return await self.cache.get(key, now=now, policy=policies[key])

            completed = await await_with_deadline(
                lambda: run_bounded(
                    unique,
                    read_one,
                    max_tasks=self.max_pending_tasks,
                    health_tracker=self.health_tracker,
                ),
                deadline_monotonic=context.deadline_monotonic,
                monotonic=self.clock.monotonic,
                operation_name="reading cache values",
            )
            lookups = dict(zip(unique, completed, strict=True))

        isolated_items = await self.async_payload_isolator.map(
            tuple((key, lookups[key]) for key in unique),
            lambda item: (item[0], self._isolated_lookup(item[0], item[1])),
            deadline_monotonic=context.deadline_monotonic,
            monotonic=self.clock.monotonic,
            deadline_context="isolating cache read results",
        )
        isolated = dict(isolated_items)
        if bool(getattr(self.cache, "validates_dependency_versions", False)):
            return isolated
        return await self._filter_invalid_dependencies(isolated, now=now, context=context)

    async def set_many(
        self,
        values: Collection[SnapshotValue[Any]],
        *,
        diagnostics: DiagnosticsCollector,
        context: FetchContext,
    ) -> Mapping[ResourceKey, CacheWriteResult] | None:
        unique = tuple({value.key: value for value in values}.values())
        if not unique:
            return None
        isolated = await self.async_payload_isolator.map(
            unique,
            lambda value: self.payload_isolator.clone_snapshot_value(
                value, context=f"cache write for {value.key}"
            ),
            deadline_monotonic=context.deadline_monotonic,
            monotonic=self.clock.monotonic,
            deadline_context="isolating cache write values",
        )
        if isinstance(self.cache, BatchAtomicAsyncCache):
            diagnostics.cache_batch_writes += 1
            atomic_batch_cache = self.cache
            return await await_with_deadline(
                lambda: atomic_batch_cache.set_many_if_newer(isolated),
                deadline_monotonic=context.deadline_monotonic,
                monotonic=self.clock.monotonic,
                operation_name="writing cache values",
            )
        if isinstance(self.cache, BatchAsyncCache):
            diagnostics.cache_batch_writes += 1
            batch_cache = self.cache
            await await_with_deadline(
                lambda: batch_cache.set_many(isolated),
                deadline_monotonic=context.deadline_monotonic,
                monotonic=self.clock.monotonic,
                operation_name="writing cache values",
            )
            return None
        if isinstance(self.cache, AtomicAsyncCache):
            cache = self.cache

            async def write_atomic(value: SnapshotValue[Any]) -> CacheWriteResult:
                return await cache.set_if_newer(value)

            write_results = await await_with_deadline(
                lambda: run_bounded(
                    isolated,
                    write_atomic,
                    max_tasks=self.max_pending_tasks,
                    health_tracker=self.health_tracker,
                ),
                deadline_monotonic=context.deadline_monotonic,
                monotonic=self.clock.monotonic,
                operation_name="writing cache values",
            )
            return {v.key: r for v, r in zip(isolated, write_results, strict=True)}

        async def write_one(value: SnapshotValue[Any]) -> None:
            await self.cache.set(value)

        await await_with_deadline(
            lambda: run_bounded(
                isolated,
                write_one,
                max_tasks=self.max_pending_tasks,
                health_tracker=self.health_tracker,
            ),
            deadline_monotonic=context.deadline_monotonic,
            monotonic=self.clock.monotonic,
            operation_name="writing cache values",
        )
        return None

    async def cached_copy(
        self,
        value: SnapshotValue[Any],
        *,
        stale: bool,
        extra_metadata: Mapping[str, Any] | None = None,
        context: FetchContext,
    ) -> SnapshotValue[Any]:
        now = self.clock.now()
        metadata = {**value.metadata, **dict(extra_metadata or {})}
        return await self.async_payload_isolator.clone_snapshot_value(
            value,
            context=f"cached snapshot value for {value.key}",
            fetched_at=now,
            age_seconds=max(0.0, now - value.observed_at),
            stale=stale,
            from_cache=True,
            latency_ms=0.0,
            metadata=metadata,
            deadline_monotonic=context.deadline_monotonic,
            monotonic=self.clock.monotonic,
        )

    async def _filter_invalid_dependencies(
        self,
        lookups: Mapping[ResourceKey, CacheLookup],
        *,
        now: float,
        context: FetchContext,
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
                context=context,
            ):
                invalid.add(key)
                filtered[key] = CacheLookup(
                    value=None,
                    fresh=False,
                    usable_stale=False,
                    age_seconds=lookup.age_seconds,
                )

        if invalid:
            await self._invalidate_many(invalid, context=context)
        return filtered

    async def _dependencies_current(
        self,
        value: SnapshotValue[Any],
        *,
        now: float,
        cached_values: dict[ResourceKey, SnapshotValue[Any] | None],
        visiting: frozenset[ResourceKey],
        invalid: set[ResourceKey],
        context: FetchContext,
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
                context=context,
            )
            if dependency is None or dependency.version != expected_version:
                return False
            if not await self._dependencies_current(
                dependency,
                now=now,
                cached_values=cached_values,
                visiting=path,
                invalid=invalid,
                context=context,
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
        context: FetchContext,
    ) -> SnapshotValue[Any] | None:
        if key in cached_values:
            return cached_values[key]
        lookup = await await_with_deadline(
            lambda: self.cache.get(
                key,
                now=now,
                policy=self._all_values_policy,
            ),
            deadline_monotonic=context.deadline_monotonic,
            monotonic=self.clock.monotonic,
            operation_name=f"reading cache dependency {key}",
        )
        cached_values[key] = lookup.value
        return lookup.value

    async def _invalidate_many(
        self,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
    ) -> None:
        unique = tuple(dict.fromkeys(keys))
        if isinstance(self.cache, BatchAsyncCache):
            batch_cache = self.cache
            await await_with_deadline(
                lambda: batch_cache.invalidate_many(unique),
                deadline_monotonic=context.deadline_monotonic,
                monotonic=self.clock.monotonic,
                operation_name="invalidating cache dependencies",
            )
            return

        async def invalidate_one(key: ResourceKey) -> None:
            await self.cache.invalidate(key)

        await await_with_deadline(
            lambda: run_bounded(
                unique,
                invalidate_one,
                max_tasks=self.max_pending_tasks,
                health_tracker=self.health_tracker,
            ),
            deadline_monotonic=context.deadline_monotonic,
            monotonic=self.clock.monotonic,
            operation_name="invalidating cache dependencies",
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
