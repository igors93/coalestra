from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import replace
from functools import partial
from typing import Any

from coalestra.cache.memory import AsyncMemoryCache
from coalestra.core.clock import SystemClock
from coalestra.core.errors import (
    CircuitOpenError,
    ResourceResolutionError,
    SnapshotBuildError,
    SourceFailure,
    SourceTimeoutError,
    SourceUnavailableError,
)
from coalestra.core.models import (
    FetchContext,
    FreshnessPolicy,
    ResourceKey,
    Snapshot,
    SnapshotValue,
    SourcePayload,
)
from coalestra.core.protocols import AsyncCache, Clock, EventSink, MetricsSink, SnapshotSource
from coalestra.observability.events import NullEventSink
from coalestra.observability.metrics import NullMetrics
from coalestra.orchestration.policy import PolicyResolver
from coalestra.orchestration.singleflight import SingleFlight
from coalestra.resilience.circuit_breaker import CircuitBreaker
from coalestra.resilience.retry import RetryPolicy, run_with_retry


class SnapshotBuilder:
    """Builds consistent snapshots from prioritized, resilient, coalesced sources."""

    def __init__(
        self,
        sources: Iterable[SnapshotSource],
        *,
        default_policy: FreshnessPolicy | None = None,
        policy_resolver: PolicyResolver | None = None,
        cache: AsyncCache | None = None,
        single_flight: SingleFlight[ResourceKey, SnapshotValue[Any]] | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        clock: Clock | None = None,
        metrics: MetricsSink | None = None,
        events: EventSink | None = None,
        max_concurrency: int = 8,
    ) -> None:
        source_list = list(sources)
        if not source_list:
            raise ValueError("at least one source is required")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")

        names = [source.name for source in source_list]
        if len(names) != len(set(names)):
            raise ValueError("source names must be unique")

        self.sources = tuple(sorted(source_list, key=lambda item: item.priority, reverse=True))
        self.clock = clock or SystemClock()
        default = default_policy or FreshnessPolicy(
            ttl_seconds=1.0,
            max_stale_seconds=10.0,
        )
        self.policy_resolver = policy_resolver or PolicyResolver(default)
        self.cache = cache or AsyncMemoryCache()
        self.single_flight = single_flight or SingleFlight()
        self.retry_policy = retry_policy or RetryPolicy()
        self.circuit_breaker = circuit_breaker or CircuitBreaker(clock=self.clock)
        self.metrics = metrics or NullMetrics()
        self.events = events or NullEventSink()
        self.max_concurrency = max_concurrency

    async def build(
        self,
        keys: Iterable[ResourceKey],
        *,
        strict: bool = True,
        deadline_seconds: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> Snapshot:
        unique_keys = tuple(dict.fromkeys(keys))
        if not unique_keys:
            raise ValueError("at least one resource key is required")
        if deadline_seconds is not None and deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")

        created_at = self.clock.now()
        resolved_snapshot_id = snapshot_id or uuid.uuid4().hex
        context = FetchContext(
            requested_at=created_at,
            deadline_at=(created_at + deadline_seconds if deadline_seconds is not None else None),
            deadline_monotonic=(
                self.clock.monotonic() + deadline_seconds if deadline_seconds is not None else None
            ),
            metadata=metadata or {},
        )
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def resolve(key: ResourceKey) -> SnapshotValue[Any]:
            async with semaphore:
                return await self._resolve_key(key, context)

        results = await asyncio.gather(
            *(resolve(key) for key in unique_keys),
            return_exceptions=True,
        )

        values: dict[ResourceKey, SnapshotValue[Any]] = {}
        errors: dict[ResourceKey, Exception] = {}
        for key, result in zip(unique_keys, results, strict=True):
            if isinstance(result, BaseException):
                if isinstance(result, Exception):
                    errors[key] = result
                    continue
                raise result
            values[key] = result

        self.metrics.increment("snapshot_build_total", status="error" if errors else "success")
        self.events.emit(
            "snapshot_built",
            snapshot_id=resolved_snapshot_id,
            resources=len(unique_keys),
            resolved=len(values),
            failed=len(errors),
            strict=strict,
        )

        if errors and strict:
            raise SnapshotBuildError(errors)

        return Snapshot(
            snapshot_id=resolved_snapshot_id,
            created_at=created_at,
            resources=values,
            errors=errors,
        )

    async def _resolve_key(
        self,
        key: ResourceKey,
        context: FetchContext,
    ) -> SnapshotValue[Any]:
        policy = self.policy_resolver.resolve(key)
        lookup = await self.cache.get(key, now=self.clock.now(), policy=policy)

        if lookup.fresh and lookup.value is not None:
            self.metrics.increment("cache_access_total", status="fresh", resource=str(key))
            self.events.emit("cache_hit", resource=str(key), freshness="fresh")
            return self._cached_copy(lookup.value, stale=False)

        self.metrics.increment("cache_access_total", status="miss", resource=str(key))
        stale_candidate = lookup.value if lookup.usable_stale else None

        try:
            value, joined_existing = await self.single_flight.run(
                key,
                lambda: self._fetch_from_sources(key, context, policy),
            )
            if joined_existing:
                self.metrics.increment("singleflight_join_total", resource=str(key))
                return replace(
                    value,
                    metadata={**value.metadata, "coalesced_request": True},
                )
            return value
        except Exception as error:
            if stale_candidate is not None and policy.allow_stale_on_error and lookup.usable_stale:
                self.metrics.increment(
                    "cache_access_total",
                    status="stale_fallback",
                    resource=str(key),
                )
                self.events.emit(
                    "stale_fallback_used",
                    resource=str(key),
                    error_type=type(error).__name__,
                )
                return self._cached_copy(
                    stale_candidate,
                    stale=True,
                    extra_metadata={
                        "fallback_error_type": type(error).__name__,
                        "fallback_error": str(error),
                    },
                )
            raise

    async def _fetch_from_sources(
        self,
        key: ResourceKey,
        context: FetchContext,
        policy: FreshnessPolicy,
    ) -> SnapshotValue[Any]:
        compatible = [source for source in self.sources if source.supports(key)]
        failures: list[SourceFailure] = []
        best_stale: SnapshotValue[Any] | None = None

        for source in compatible:
            started = self.clock.monotonic()
            try:
                await self.circuit_breaker.before_call(source.name)
                payload, attempts = await run_with_retry(
                    partial(self._fetch_once, source, key, context),
                    policy=self.retry_policy,
                    retryable=self._is_retryable,
                )
                fetched_at = self.clock.now()
                observed_at = payload.observed_at if payload.observed_at is not None else fetched_at
                age_seconds = max(0.0, fetched_at - observed_at)
                latency_ms = max(0.0, (self.clock.monotonic() - started) * 1000)
                value = SnapshotValue(
                    key=key,
                    value=payload.value,
                    source=source.name,
                    observed_at=observed_at,
                    fetched_at=fetched_at,
                    age_seconds=age_seconds,
                    stale=age_seconds > policy.ttl_seconds,
                    from_cache=False,
                    latency_ms=latency_ms,
                    attempts=attempts,
                    metadata=payload.metadata,
                )

                if value.stale:
                    await self.circuit_breaker.record_failure(source.name)
                    if age_seconds <= policy.max_stale_seconds and (
                        best_stale is None or value.observed_at > best_stale.observed_at
                    ):
                        best_stale = value
                    failures.append(
                        SourceFailure(
                            source=source.name,
                            error_type="StaleSourceData",
                            message=(
                                f"age {age_seconds:.3f}s exceeds ttl {policy.ttl_seconds:.3f}s"
                            ),
                            attempts=attempts,
                        )
                    )
                    continue

                await self.circuit_breaker.record_success(source.name)
                await self.cache.set(value)
                self.metrics.increment("source_fetch_total", status="success", source=source.name)
                self.metrics.observe("source_fetch_latency_ms", latency_ms, source=source.name)
                self.events.emit(
                    "resource_resolved",
                    resource=str(key),
                    source=source.name,
                    latency_ms=latency_ms,
                    attempts=attempts,
                )
                return value
            except CircuitOpenError as error:
                failures.append(
                    SourceFailure(
                        source=source.name,
                        error_type=type(error).__name__,
                        message=str(error),
                        attempts=0,
                    )
                )
                self.metrics.increment(
                    "source_fetch_total", status="circuit_open", source=source.name
                )
            except Exception as error:
                await self.circuit_breaker.record_failure(source.name)
                attempts = self.retry_policy.max_attempts if self._is_retryable(error) else 1
                failures.append(
                    SourceFailure(
                        source=source.name,
                        error_type=type(error).__name__,
                        message=str(error),
                        attempts=attempts,
                    )
                )
                self.metrics.increment("source_fetch_total", status="failure", source=source.name)
                self.events.emit(
                    "source_failed",
                    resource=str(key),
                    source=source.name,
                    error_type=type(error).__name__,
                    error=str(error),
                )

        if best_stale is not None and policy.allow_stale_on_error:
            await self.cache.set(best_stale)
            self.metrics.increment("source_fetch_total", status="stale", source=best_stale.source)
            return best_stale

        raise ResourceResolutionError(key, tuple(failures))

    async def _fetch_once(
        self,
        source: SnapshotSource,
        key: ResourceKey,
        context: FetchContext,
    ) -> SourcePayload[Any]:
        timeout = self._effective_timeout(source, context)
        try:
            if timeout is None:
                return await source.fetch(key, context)
            return await asyncio.wait_for(source.fetch(key, context), timeout=timeout)
        except TimeoutError as error:
            raise SourceTimeoutError(
                f"source {source.name} timed out while resolving {key}"
            ) from error

    def _effective_timeout(
        self,
        source: SnapshotSource,
        context: FetchContext,
    ) -> float | None:
        timeout = source.timeout_seconds
        if context.deadline_monotonic is None:
            return timeout

        remaining = context.deadline_monotonic - self.clock.monotonic()
        if remaining <= 0:
            raise SourceTimeoutError(
                f"snapshot deadline exceeded before resolving source {source.name}"
            )
        return remaining if timeout is None else min(timeout, remaining)

    @staticmethod
    def _is_retryable(error: Exception) -> bool:
        return isinstance(
            error,
            (
                SourceUnavailableError,
                TimeoutError,
                ConnectionError,
                OSError,
            ),
        ) and not isinstance(error, CircuitOpenError)

    def _cached_copy(
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
