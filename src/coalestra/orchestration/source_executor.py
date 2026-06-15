from __future__ import annotations

import asyncio
from collections.abc import Collection
from functools import partial
from typing import Any, Protocol, cast

from coalestra.core.errors import (
    CircuitOpenError,
    DependencyCycleError,
    DependencyResolutionError,
    SnapshotDeadlineExceededError,
    SourceProtocolError,
    SourceQueueTimeoutError,
    SourceUnavailableError,
)
from coalestra.core.models import FetchContext, ResourceKey, Snapshot, SnapshotValue
from coalestra.core.protocols import BatchSnapshotSource, DerivedSource, SnapshotSource, Source
from coalestra.orchestration.runtime import ResolutionRuntime, SourceAttempt
from coalestra.orchestration.source_calls import SourceCalls
from coalestra.orchestration.source_catalog import SourceCatalog
from coalestra.resilience.retry import run_with_retry


class ResolveMany(Protocol):
    async def __call__(
        self,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...] = (),
        local_owned: frozenset[ResourceKey] = frozenset(),
    ) -> tuple[dict[ResourceKey, SnapshotValue[Any]], dict[ResourceKey, Exception]]: ...


class SourceExecutor:
    """Dispatch single, batch, and derived source attempts."""

    def __init__(
        self,
        *,
        source_catalog: SourceCatalog,
        source_calls: SourceCalls,
        resolve_many: ResolveMany,
        max_concurrency: int,
    ) -> None:
        self.source_catalog = source_catalog
        self.calls = source_calls
        self.resolve_many = resolve_many
        self.max_concurrency = max_concurrency

    async def attempt(
        self,
        source: Source,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...],
        local_owned: frozenset[ResourceKey],
    ) -> dict[ResourceKey, SourceAttempt]:
        kind = self.source_catalog.kind(source)
        if kind == "derived":
            return await self._attempt_derived_source(
                cast(DerivedSource, source),
                keys,
                context=context,
                runtime=runtime,
                ancestry=ancestry,
                local_owned=local_owned,
            )

        if kind == "batch":
            batch_source = cast(BatchSnapshotSource, source)
            requested = tuple(dict.fromkeys(keys))
            max_batch_size = getattr(batch_source, "max_batch_size", None)
            if max_batch_size is None or len(requested) <= int(max_batch_size):
                runtime.diagnostics.batch_chunks += 1
                return await self._attempt_batch_source(
                    batch_source,
                    requested,
                    context=context,
                    runtime=runtime,
                )

            chunks = tuple(
                requested[index : index + int(max_batch_size)]
                for index in range(0, len(requested), int(max_batch_size))
            )
            runtime.diagnostics.batch_chunks += len(chunks)
            source_limit = self.calls.capacity.limit_for(batch_source.name)
            wave_size = max(
                1,
                min(
                    self.max_concurrency,
                    source_limit or self.max_concurrency,
                ),
            )
            merged: dict[ResourceKey, SourceAttempt] = {}
            for start in range(0, len(chunks), wave_size):
                wave = chunks[start : start + wave_size]
                completed = await asyncio.gather(
                    *(
                        self._attempt_batch_source(
                            batch_source,
                            chunk,
                            context=context,
                            runtime=runtime,
                        )
                        for chunk in wave
                    )
                )
                for result in completed:
                    merged.update(result)
            return merged

        return await self._attempt_single_source(
            cast(SnapshotSource, source),
            keys,
            context=context,
            runtime=runtime,
        )

    async def _attempt_single_source(
        self,
        source: SnapshotSource,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> dict[ResourceKey, SourceAttempt]:
        resilience = self.source_catalog.resilience_for(source)

        async def fetch_one(key: ResourceKey) -> tuple[ResourceKey, SourceAttempt]:
            started = self.calls.clock.monotonic()
            try:
                await self.calls.circuit_breaker.before_call(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                payload, attempts = await run_with_retry(
                    partial(
                        self.calls.fetch_once,
                        source,
                        key,
                        context,
                        runtime,
                    ),
                    policy=resilience.retry,
                    retryable=self.calls.is_retryable,
                    deadline_monotonic=context.deadline_monotonic,
                    monotonic=self.calls.clock.monotonic,
                )
                runtime.diagnostics.retries += max(0, attempts - 1)
                await self.calls.record_payload_circuit_outcome(
                    source,
                    key,
                    payload,
                    resilience,
                )
                return key, SourceAttempt(
                    payload=payload,
                    attempts=attempts,
                    latency_ms=self.calls.elapsed_ms(started),
                )
            except asyncio.CancelledError:
                await self.calls.circuit_breaker.record_abandoned(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                raise
            except (
                SnapshotDeadlineExceededError,
                SourceQueueTimeoutError,
            ) as error:
                await self.calls.circuit_breaker.record_skipped(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                attempts = self.calls.attempt_count(
                    error,
                    resilience.retry,
                )
                return key, SourceAttempt(
                    error=error,
                    attempts=attempts,
                    latency_ms=self.calls.elapsed_ms(started),
                )
            except CircuitOpenError as error:
                self.calls.record_circuit_open(source, key, error)
                return key, SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self.calls.elapsed_ms(started),
                )
            except Exception as error:
                await self.calls.circuit_breaker.record_failure(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                if isinstance(error, SourceProtocolError) and "in the future" in str(error):
                    runtime.diagnostics.future_timestamp_rejections += 1

                attempts = self.calls.attempt_count(
                    error,
                    resilience.retry,
                )
                runtime.diagnostics.retries += max(0, attempts - 1)
                return key, SourceAttempt(
                    error=error,
                    attempts=attempts,
                    latency_ms=self.calls.elapsed_ms(started),
                )

        completed = await asyncio.gather(*(fetch_one(key) for key in keys))
        for _key, attempt in completed:
            runtime.diagnostics.record_source_latency(
                source.name,
                attempt.latency_ms,
            )
        return dict(completed)

    async def _attempt_batch_source(
        self,
        source: BatchSnapshotSource,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> dict[ResourceKey, SourceAttempt]:
        requested = tuple(dict.fromkeys(keys))
        resilience = self.source_catalog.resilience_for(source)
        started = self.calls.clock.monotonic()
        results: dict[ResourceKey, SourceAttempt] = {}

        groups = self.source_catalog.circuit_groups(
            source,
            requested,
            resilience,
        )
        active_groups: list[tuple[ResourceKey, tuple[ResourceKey, ...]]] = []
        allowed: set[ResourceKey] = set()

        for representative, grouped_keys in groups:
            try:
                await self.calls.circuit_breaker.before_call(
                    source.name,
                    key=representative,
                    policy=resilience.circuit,
                )
                active_groups.append((representative, grouped_keys))
                allowed.update(grouped_keys)
            except CircuitOpenError as error:
                self.calls.record_circuit_open(
                    source,
                    representative,
                    error,
                )
                for key in grouped_keys:
                    results[key] = SourceAttempt(
                        error=error,
                        attempts=0,
                        latency_ms=self.calls.elapsed_ms(started),
                    )

        active_keys = tuple(key for key in requested if key in allowed)
        if not active_keys:
            return results

        try:
            payloads, attempts = await run_with_retry(
                partial(
                    self.calls.fetch_many_once,
                    source,
                    active_keys,
                    context,
                    runtime,
                ),
                policy=resilience.retry,
                retryable=self.calls.is_retryable,
                deadline_monotonic=context.deadline_monotonic,
                monotonic=self.calls.clock.monotonic,
            )
            runtime.diagnostics.retries += max(0, attempts - 1)
            latency_ms = self.calls.elapsed_ms(started)
            runtime.diagnostics.record_source_latency(
                source.name,
                latency_ms,
            )

            invalid_payloads: dict[
                ResourceKey,
                SourceProtocolError,
            ] = {}
            for key, payload in payloads.items():
                try:
                    self.calls.validate_payload_timestamp(
                        key,
                        payload,
                    )
                except SourceProtocolError as error:
                    invalid_payloads[key] = error
                    runtime.diagnostics.future_timestamp_rejections += 1

            returned = set(payloads).difference(invalid_payloads)
            for representative, grouped_keys in active_groups:
                has_fresh_value = any(
                    key in returned
                    and self.calls.payload_is_fresh(
                        key,
                        payloads[key],
                    )
                    for key in grouped_keys
                )
                if has_fresh_value:
                    await self.calls.circuit_breaker.record_success(
                        source.name,
                        key=representative,
                        policy=resilience.circuit,
                    )
                else:
                    await self.calls.circuit_breaker.record_failure(
                        source.name,
                        key=representative,
                        policy=resilience.circuit,
                    )

            self.calls.metrics.increment(
                "source_batch_call_total",
                status="success",
                source=source.name,
            )
            self.calls.metrics.observe(
                "source_batch_size",
                float(len(active_keys)),
                source=source.name,
            )

            for key in active_keys:
                if key in invalid_payloads:
                    results[key] = SourceAttempt(
                        error=invalid_payloads[key],
                        attempts=attempts,
                        latency_ms=latency_ms,
                    )
                elif key in payloads:
                    results[key] = SourceAttempt(
                        payload=payloads[key],
                        attempts=attempts,
                        latency_ms=latency_ms,
                    )
                else:
                    results[key] = SourceAttempt(
                        error=SourceUnavailableError(
                            f"batch source {source.name} omitted resource {key}"
                        ),
                        attempts=attempts,
                        latency_ms=latency_ms,
                    )
            return results
        except asyncio.CancelledError:
            for representative, _grouped_keys in active_groups:
                await self.calls.circuit_breaker.record_abandoned(
                    source.name,
                    key=representative,
                    policy=resilience.circuit,
                )
            raise
        except (
            SnapshotDeadlineExceededError,
            SourceQueueTimeoutError,
        ) as error:
            for representative, _grouped_keys in active_groups:
                await self.calls.circuit_breaker.record_skipped(
                    source.name,
                    key=representative,
                    policy=resilience.circuit,
                )

            latency_ms = self.calls.elapsed_ms(started)
            runtime.diagnostics.record_source_latency(
                source.name,
                latency_ms,
            )
            attempts = self.calls.attempt_count(
                error,
                resilience.retry,
            )
            runtime.diagnostics.retries += max(0, attempts - 1)
            status = (
                "deadline_exceeded"
                if isinstance(
                    error,
                    SnapshotDeadlineExceededError,
                )
                else "queue_timeout"
            )
            self.calls.metrics.increment(
                "source_batch_call_total",
                status=status,
                source=source.name,
            )

            for key in active_keys:
                results[key] = SourceAttempt(
                    error=error,
                    attempts=attempts,
                    latency_ms=latency_ms,
                )
            return results
        except Exception as error:
            for representative, _grouped_keys in active_groups:
                await self.calls.circuit_breaker.record_failure(
                    source.name,
                    key=representative,
                    policy=resilience.circuit,
                )

            latency_ms = self.calls.elapsed_ms(started)
            runtime.diagnostics.record_source_latency(
                source.name,
                latency_ms,
            )
            self.calls.metrics.increment(
                "source_batch_call_total",
                status="failure",
                source=source.name,
            )
            attempts = self.calls.attempt_count(
                error,
                resilience.retry,
            )
            runtime.diagnostics.retries += max(0, attempts - 1)

            for key in active_keys:
                results[key] = SourceAttempt(
                    error=error,
                    attempts=attempts,
                    latency_ms=latency_ms,
                )
            return results

    async def _attempt_derived_source(
        self,
        source: DerivedSource,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...],
        local_owned: frozenset[ResourceKey],
    ) -> dict[ResourceKey, SourceAttempt]:
        resilience = self.source_catalog.resilience_for(source)

        async def derive_one(
            key: ResourceKey,
        ) -> tuple[ResourceKey, SourceAttempt]:
            started = self.calls.clock.monotonic()
            path = (*ancestry, key)
            try:
                dependencies = tuple(dict.fromkeys(source.dependencies(key)))
                for dependency in dependencies:
                    if not isinstance(dependency, ResourceKey):
                        raise SourceProtocolError(
                            f"derived source {source.name} returned a non-ResourceKey dependency"
                        )
                    if dependency in path:
                        cycle_start = path.index(dependency)
                        raise DependencyCycleError((*path[cycle_start:], dependency))

                dependency_values, dependency_errors = await self.resolve_many(
                    dependencies,
                    context=context,
                    runtime=runtime,
                    ancestry=path,
                    local_owned=local_owned,
                )
                if dependency_errors:
                    raise DependencyResolutionError(
                        key=key,
                        source=source.name,
                        errors=dependency_errors,
                    )

                dependency_snapshot = Snapshot(
                    snapshot_id=context.snapshot_id,
                    created_at=context.requested_at,
                    resources=dependency_values,
                    errors={},
                )
                await self.calls.circuit_breaker.before_call(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                payload, attempts = await run_with_retry(
                    partial(
                        self.calls.derive_once,
                        source,
                        key,
                        dependency_snapshot,
                        context,
                        runtime,
                    ),
                    policy=resilience.retry,
                    retryable=self.calls.is_retryable,
                    deadline_monotonic=context.deadline_monotonic,
                    monotonic=self.calls.clock.monotonic,
                )
                runtime.diagnostics.retries += max(
                    0,
                    attempts - 1,
                )
                await self.calls.record_payload_circuit_outcome(
                    source,
                    key,
                    payload,
                    resilience,
                )
                return key, SourceAttempt(
                    payload=payload,
                    attempts=attempts,
                    latency_ms=self.calls.elapsed_ms(started),
                )
            except asyncio.CancelledError:
                await self.calls.circuit_breaker.record_abandoned(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                raise
            except (
                SnapshotDeadlineExceededError,
                SourceQueueTimeoutError,
            ) as error:
                await self.calls.circuit_breaker.record_skipped(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                attempts = self.calls.attempt_count(
                    error,
                    resilience.retry,
                )
                return key, SourceAttempt(
                    error=error,
                    attempts=attempts,
                    latency_ms=self.calls.elapsed_ms(started),
                )
            except CircuitOpenError as error:
                self.calls.record_circuit_open(
                    source,
                    key,
                    error,
                )
                return key, SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self.calls.elapsed_ms(started),
                )
            except (
                DependencyCycleError,
                DependencyResolutionError,
            ) as error:
                return key, SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self.calls.elapsed_ms(started),
                )
            except SourceProtocolError as error:
                await self.calls.circuit_breaker.record_failure(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                runtime.diagnostics.future_timestamp_rejections += int(
                    "in the future" in str(error)
                )
                return key, SourceAttempt(
                    error=error,
                    attempts=1,
                    latency_ms=self.calls.elapsed_ms(started),
                )
            except Exception as error:
                await self.calls.circuit_breaker.record_failure(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                attempts = self.calls.attempt_count(
                    error,
                    resilience.retry,
                )
                runtime.diagnostics.retries += max(
                    0,
                    attempts - 1,
                )
                return key, SourceAttempt(
                    error=error,
                    attempts=attempts,
                    latency_ms=self.calls.elapsed_ms(started),
                )

        completed = await asyncio.gather(*(derive_one(key) for key in keys))
        for _key, attempt in completed:
            runtime.diagnostics.record_source_latency(
                source.name,
                attempt.latency_ms,
            )
        return dict(completed)
