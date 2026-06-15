from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar

from coalestra.concurrency.capacity import CapacityController
from coalestra.core.errors import (
    CircuitOpenError,
    SourceFailure,
    SourceProtocolError,
    SourceTimeoutError,
    SourceUnavailableError,
)
from coalestra.core.models import FetchContext, ResourceKey, Snapshot, SnapshotValue, SourcePayload
from coalestra.core.protocols import (
    BatchSnapshotSource,
    Clock,
    DerivedSource,
    EventSink,
    MetricsSink,
    SnapshotSource,
    SourceBase,
)
from coalestra.core.quality import ObservationPolicy
from coalestra.orchestration.policy import PolicyResolver
from coalestra.orchestration.runtime import ResolutionRuntime
from coalestra.resilience.circuit_breaker import CircuitBreaker
from coalestra.resilience.policy import SourceResiliencePolicy
from coalestra.resilience.retry import RetryPolicy, attempts_for

T = TypeVar("T")


class SourceCalls:
    """Run low-level source calls and apply timeout, capacity, and payload rules."""

    def __init__(
        self,
        *,
        clock: Clock,
        capacity: CapacityController,
        circuit_breaker: CircuitBreaker,
        policy_resolver: PolicyResolver,
        metrics: MetricsSink,
        events: EventSink,
        observation_policy: ObservationPolicy,
    ) -> None:
        self.clock = clock
        self.capacity = capacity
        self.circuit_breaker = circuit_breaker
        self.policy_resolver = policy_resolver
        self.metrics = metrics
        self.events = events
        self.observation_policy = observation_policy

    async def fetch_once(
        self,
        source: SnapshotSource,
        key: ResourceKey,
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> SourcePayload[Any]:
        async def invoke() -> SourcePayload[Any]:
            wait_started = self.clock.monotonic()
            async with self.capacity.slot(source.name):
                runtime.diagnostics.record_source_call(source.name, kind="single")
                self.metrics.observe(
                    "source_capacity_wait_ms",
                    self.elapsed_ms(wait_started),
                    source=source.name,
                )
                return self.coerce_payload(await source.fetch(key, context))

        return await self.with_timeout(source, context, invoke, resource=key)

    async def fetch_many_once(
        self,
        source: BatchSnapshotSource,
        keys: tuple[ResourceKey, ...],
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> Mapping[ResourceKey, SourcePayload[Any]]:
        async def invoke() -> Mapping[ResourceKey, SourcePayload[Any]]:
            wait_started = self.clock.monotonic()
            async with self.capacity.slot(source.name):
                runtime.diagnostics.record_source_call(source.name, kind="batch")
                self.metrics.observe(
                    "source_capacity_wait_ms",
                    self.elapsed_ms(wait_started),
                    source=source.name,
                )
                result = await source.fetch_many(keys, context)
                if not isinstance(result, Mapping):
                    raise SourceProtocolError(
                        f"batch source {source.name} must return a mapping, "
                        f"got {type(result).__name__}"
                    )
                requested = set(keys)
                unexpected = [key for key in result if key not in requested]
                if unexpected:
                    rendered = ", ".join(str(key) for key in unexpected)
                    raise SourceProtocolError(
                        f"batch source {source.name} returned unrequested resources: {rendered}"
                    )
                return {key: self.coerce_payload(value) for key, value in result.items()}

        return await self.with_timeout(source, context, invoke, resource=None)

    async def derive_once(
        self,
        source: DerivedSource,
        key: ResourceKey,
        dependencies: Snapshot,
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> SourcePayload[Any]:
        async def invoke() -> SourcePayload[Any]:
            wait_started = self.clock.monotonic()
            async with self.capacity.slot(source.name):
                runtime.diagnostics.record_source_call(source.name, kind="derived")
                self.metrics.observe(
                    "source_capacity_wait_ms",
                    self.elapsed_ms(wait_started),
                    source=source.name,
                )
                return self.coerce_payload(await source.derive(key, dependencies, context))

        return await self.with_timeout(source, context, invoke, resource=key)

    async def with_timeout(
        self,
        source: SourceBase,
        context: FetchContext,
        operation: Callable[[], Awaitable[T]],
        *,
        resource: ResourceKey | None,
    ) -> T:
        timeout = self.effective_timeout(source, context)
        try:
            if timeout is None:
                return await operation()
            return await asyncio.wait_for(operation(), timeout=timeout)
        except (TimeoutError, asyncio.TimeoutError) as error:
            target = f" while resolving {resource}" if resource is not None else ""

            raise SourceTimeoutError(
                f"source {source.name} timed out after {timeout:.3f}s{target}"
            ) from error

    def effective_timeout(
        self,
        source: SourceBase,
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

    def snapshot_value(
        self,
        *,
        key: ResourceKey,
        source: SourceBase,
        payload: SourcePayload[Any],
        attempts: int,
        latency_ms: float,
    ) -> SnapshotValue[Any]:
        fetched_at = self.clock.now()
        future_seconds = self.validate_payload_timestamp(key, payload)
        observed_at = payload.observed_at if payload.observed_at is not None else fetched_at
        age_seconds = max(0.0, fetched_at - observed_at)
        policy = self.policy_resolver.resolve(key)
        metadata = dict(payload.metadata)
        if future_seconds > 0:
            metadata["clock_skew_seconds"] = future_seconds
        return SnapshotValue(
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
            metadata=metadata,
        )

    @staticmethod
    def coerce_payload(value: SourcePayload[Any] | Any) -> SourcePayload[Any]:
        return value if isinstance(value, SourcePayload) else SourcePayload(value=value)

    @staticmethod
    def is_retryable(error: Exception) -> bool:
        return isinstance(
            error,
            (
                SourceUnavailableError,
                TimeoutError,
                ConnectionError,
                OSError,
            ),
        ) and not isinstance(error, CircuitOpenError)

    def validate_payload_timestamp(
        self,
        key: ResourceKey,
        payload: SourcePayload[Any],
    ) -> float:
        observed_at = payload.observed_at
        if observed_at is None:
            return 0.0
        future_seconds = float(observed_at) - self.clock.now()
        if (
            future_seconds > self.observation_policy.future_tolerance_seconds
            and self.observation_policy.reject_future_observations
        ):
            raise SourceProtocolError(
                f"source observation for {key} is {future_seconds:.6f}s in the future"
            )
        return max(0.0, future_seconds)

    def payload_is_fresh(
        self,
        key: ResourceKey,
        payload: SourcePayload[Any],
    ) -> bool:
        self.validate_payload_timestamp(key, payload)
        observed_at = payload.observed_at
        if observed_at is None:
            return True
        age_seconds = max(0.0, self.clock.now() - observed_at)
        return age_seconds <= self.policy_resolver.resolve(key).ttl_seconds

    async def record_payload_circuit_outcome(
        self,
        source: SourceBase,
        key: ResourceKey,
        payload: SourcePayload[Any],
        resilience: SourceResiliencePolicy,
    ) -> None:
        recorder = (
            self.circuit_breaker.record_success
            if self.payload_is_fresh(key, payload)
            else self.circuit_breaker.record_failure
        )
        await recorder(
            source.name,
            key=key,
            policy=resilience.circuit,
        )

    def record_circuit_open(
        self,
        source: SourceBase,
        key: ResourceKey,
        error: CircuitOpenError,
    ) -> None:
        self.metrics.increment(
            "source_fetch_total",
            status="circuit_open",
            source=source.name,
        )
        self.events.emit(
            "source_circuit_open",
            source=source.name,
            resource=str(key),
            error=str(error),
        )

    def attempt_count(self, error: Exception, retry_policy: RetryPolicy) -> int:
        default = retry_policy.max_attempts if self.is_retryable(error) else 1
        return attempts_for(error, default=default)

    def elapsed_ms(self, started: float) -> float:
        return max(0.0, (self.clock.monotonic() - started) * 1000)

    @staticmethod
    def source_failure(
        source: SourceBase,
        error: Exception,
        *,
        attempts: int,
    ) -> SourceFailure:
        return SourceFailure(
            source=source.name,
            error_type=type(error).__name__,
            message=str(error),
            attempts=attempts,
        )
