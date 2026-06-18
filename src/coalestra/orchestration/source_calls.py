from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any, TypeVar

from coalestra.concurrency.capacity import CapacityController, CapacityLease
from coalestra.core.authority import AuthorityPolicyResolver
from coalestra.core.errors import (
    CircuitOpenError,
    SnapshotDeadlineExceededError,
    SourceFailure,
    SourceProtocolError,
    SourceQueueTimeoutError,
    SourceTimeoutError,
    SourceUnavailableError,
)
from coalestra.core.health import OperationalHealthTracker
from coalestra.core.isolation import AsyncPayloadIsolator, PayloadIsolator
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
from coalestra.core.source_timeout import (
    SourceTimeoutGuarantee,
    SourceTimeoutGuaranteeStatus,
)
from coalestra.orchestration.policy import PolicyResolver
from coalestra.orchestration.runtime import ResolutionRuntime
from coalestra.resilience.circuit_breaker import CircuitBreaker
from coalestra.resilience.policy import SourceResiliencePolicy
from coalestra.resilience.retry import RetryPolicy, attempts_for

T = TypeVar("T")


@dataclass(frozen=True)
class _TimeoutBudget:
    seconds: float | None
    limited_by_deadline: bool = False


class _OperationTimedOut(Exception):
    """Internal marker raised only when a Coalestra-owned timer expires."""


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
        payload_isolator: PayloadIsolator,
        async_payload_isolator: AsyncPayloadIsolator,
        authority_resolver: AuthorityPolicyResolver,
        source_timeout_guarantees: Mapping[str, SourceTimeoutGuarantee],
        health_tracker: OperationalHealthTracker,
        transport_timeout_grace_seconds: float,
        deadline_dispatch_grace_seconds: float = 0.0,
    ) -> None:
        self.clock = clock
        self.capacity = capacity
        self.circuit_breaker = circuit_breaker
        self.policy_resolver = policy_resolver
        self.metrics = metrics
        self.events = events
        self.observation_policy = observation_policy
        self.payload_isolator = payload_isolator
        self.async_payload_isolator = async_payload_isolator
        self.authority_resolver = authority_resolver
        self.source_timeout_guarantees = dict(source_timeout_guarantees)
        self.health_tracker = health_tracker
        self.transport_timeout_grace_seconds = float(transport_timeout_grace_seconds)
        self.deadline_dispatch_grace_seconds = float(deadline_dispatch_grace_seconds)

    async def fetch_once(
        self,
        source: SnapshotSource,
        key: ResourceKey,
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> SourcePayload[Any]:
        lease = await self.acquire_capacity(
            source,
            context,
            resource=key,
        )
        try:
            runtime.diagnostics.record_source_call(source.name, kind="single")
            result = await self.with_timeout(
                source,
                context,
                lambda: source.fetch(key, context),
                resource=key,
            )
        finally:
            lease.release()
        return await self.coerce_payload(
            result,
            copy_context=f"source {source.name} payload for {key}",
            fetch_context=context,
        )

    async def fetch_many_once(
        self,
        source: BatchSnapshotSource,
        keys: tuple[ResourceKey, ...],
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> Mapping[ResourceKey, SourcePayload[Any]]:
        lease = await self.acquire_capacity(
            source,
            context,
            resource=None,
        )
        try:
            runtime.diagnostics.record_source_call(source.name, kind="batch")
            result = await self.with_timeout(
                source,
                context,
                lambda: source.fetch_many(keys, context),
                resource=None,
            )
        finally:
            lease.release()

        if not isinstance(result, Mapping):
            raise SourceProtocolError(
                f"batch source {source.name} must return a mapping, got {type(result).__name__}"
            )

        requested = set(keys)
        unexpected = [key for key in result if key not in requested]
        if unexpected:
            rendered = ", ".join(str(key) for key in unexpected)
            raise SourceProtocolError(
                f"batch source {source.name} returned unrequested resources: {rendered}"
            )

        copied = await self.async_payload_isolator.map(
            tuple(result.items()),
            lambda item: (
                item[0],
                self._coerce_payload_sync(
                    item[1],
                    context=f"batch source {source.name} payload for {item[0]}",
                ),
            ),
            deadline_monotonic=context.deadline_monotonic,
            monotonic=self.clock.monotonic,
            deadline_context=f"isolating batch source {source.name} payloads",
        )
        return dict(copied)

    async def derive_once(
        self,
        source: DerivedSource,
        key: ResourceKey,
        dependencies: Snapshot,
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> SourcePayload[Any]:
        lease = await self.acquire_capacity(
            source,
            context,
            resource=key,
        )
        try:
            runtime.diagnostics.record_source_call(source.name, kind="derived")
            result = await self.with_timeout(
                source,
                context,
                lambda: source.derive(key, dependencies, context),
                resource=key,
            )
        finally:
            lease.release()
        return await self.coerce_payload(
            result,
            copy_context=f"derived source {source.name} payload for {key}",
            fetch_context=context,
        )

    async def acquire_capacity(
        self,
        source: SourceBase,
        context: FetchContext,
        *,
        resource: ResourceKey | None,
    ) -> CapacityLease:
        configured_queue_timeout = getattr(
            source,
            "queue_timeout_seconds",
            source.timeout_seconds,
        )
        budget = self._timeout_budget(
            configured_queue_timeout,
            context,
            source=source,
            phase="waiting for capacity",
            resource=resource,
        )
        wait_started = self.clock.monotonic()

        try:
            return await self._await_with_timeout(
                lambda: self.capacity.acquire(source.name),
                budget.seconds,
            )
        except _OperationTimedOut as error:
            target = self._target_suffix(resource)
            if budget.limited_by_deadline:
                self.metrics.increment(
                    "source_capacity_timeout_total",
                    source=source.name,
                    reason="snapshot_deadline",
                )
                raise SnapshotDeadlineExceededError(
                    f"snapshot deadline exceeded while waiting for capacity "
                    f"for source {source.name}{target}"
                ) from error

            self.metrics.increment(
                "source_capacity_timeout_total",
                source=source.name,
                reason="queue_timeout",
            )
            raise SourceQueueTimeoutError(
                f"source {source.name} waited more than {budget.seconds:.3f}s for capacity{target}"
            ) from error
        finally:
            self.metrics.observe(
                "source_capacity_wait_ms",
                self.elapsed_ms(wait_started),
                source=source.name,
            )

    async def with_timeout(
        self,
        source: SourceBase,
        context: FetchContext,
        operation: Callable[[], Awaitable[T]],
        *,
        resource: ResourceKey | None,
    ) -> T:
        budget = self._timeout_budget(
            source.timeout_seconds,
            context,
            source=source,
            phase="calling the source",
            resource=resource,
        )
        self._ensure_transport_timeout_fits_budget(
            source,
            budget=budget,
            resource=resource,
        )
        started_at = self.clock.monotonic()
        try:
            result = await self._await_with_timeout(
                operation,
                budget.seconds,
            )
        except _OperationTimedOut as error:
            self._record_transport_timeout_violation(
                source,
                elapsed_seconds=max(0.0, self.clock.monotonic() - started_at),
                resource=resource,
                outcome="coalestra_timeout",
                force=True,
            )
            target = self._target_suffix(resource)
            if budget.limited_by_deadline:
                raise SnapshotDeadlineExceededError(
                    f"snapshot deadline exceeded while calling source {source.name}{target}"
                ) from error

            raise SourceTimeoutError(
                f"source {source.name} timed out after {budget.seconds:.3f}s{target}"
            ) from error
        except BaseException:
            self._record_transport_timeout_violation(
                source,
                elapsed_seconds=max(0.0, self.clock.monotonic() - started_at),
                resource=resource,
                outcome="failed_late",
            )
            raise

        self._record_transport_timeout_violation(
            source,
            elapsed_seconds=max(0.0, self.clock.monotonic() - started_at),
            resource=resource,
            outcome="completed_late",
        )
        return result

    def _record_transport_timeout_violation(
        self,
        source: SourceBase,
        *,
        elapsed_seconds: float,
        resource: ResourceKey | None,
        outcome: str,
        force: bool = False,
    ) -> None:
        guarantee = self.source_timeout_guarantees.get(source.name)
        if (
            guarantee is None
            or guarantee.status is not SourceTimeoutGuaranteeStatus.PROTECTED
            or guarantee.transport_timeout_seconds is None
        ):
            return
        threshold = guarantee.transport_timeout_seconds + self.transport_timeout_grace_seconds
        if not force and elapsed_seconds <= threshold:
            return
        self.health_tracker.record_source_transport_timeout_violation(source.name)
        self.metrics.increment(
            "source_transport_timeout_violation_total",
            source=source.name,
            outcome=outcome,
        )
        self.events.emit(
            "source_transport_timeout_violated",
            source=source.name,
            resource=str(resource) if resource is not None else "",
            outcome=outcome,
            declared_transport_timeout_seconds=guarantee.transport_timeout_seconds,
            observed_elapsed_seconds=elapsed_seconds,
            grace_seconds=self.transport_timeout_grace_seconds,
        )

    def _ensure_transport_timeout_fits_budget(
        self,
        source: SourceBase,
        *,
        budget: _TimeoutBudget,
        resource: ResourceKey | None,
    ) -> None:
        guarantee = self.source_timeout_guarantees.get(source.name)
        if (
            guarantee is None
            or guarantee.status is not SourceTimeoutGuaranteeStatus.PROTECTED
            or guarantee.transport_timeout_seconds is None
            or budget.seconds is None
        ):
            return

        transport = guarantee.transport_timeout_seconds

        # Budget comfortably covers the declared transport timeout — nothing to do.
        if transport < budget.seconds:
            return

        # Budget is within the deadline dispatch grace window: allow the call but emit a
        # pressure event so operators can detect when configuration margins are tight.
        # Grace only applies when the budget is limited by the session deadline, not when
        # source.timeout_seconds itself is misconfigured to be smaller than transport_timeout.
        if (
            budget.limited_by_deadline
            and transport < budget.seconds + self.deadline_dispatch_grace_seconds
        ):
            self.events.emit(
                "source_dispatch_under_deadline_pressure",
                source=source.name,
                resource=str(resource) if resource is not None else "",
                transport_timeout_seconds=transport,
                available_budget_seconds=budget.seconds,
                deadline_dispatch_grace_seconds=self.deadline_dispatch_grace_seconds,
                gap_seconds=transport - budget.seconds,
            )
            return

        # Budget is too small to safely dispatch — reject before the call starts.
        target = self._target_suffix(resource)
        self.metrics.increment(
            "source_transport_budget_rejection_total",
            source=source.name,
            reason="snapshot_deadline" if budget.limited_by_deadline else "source_timeout",
        )
        self.events.emit(
            "source_transport_budget_rejected",
            source=source.name,
            resource=str(resource) if resource is not None else "",
            transport_timeout_seconds=transport,
            available_budget_seconds=budget.seconds,
        )
        gap = transport - budget.seconds
        detail = f"budget={budget.seconds:.3f}s, transport_timeout={transport:.3f}s, gap={gap:.3f}s"
        if budget.limited_by_deadline:
            raise SnapshotDeadlineExceededError(
                f"snapshot deadline budget is shorter than the declared transport timeout "
                f"for source {source.name}{target} ({detail})"
            )
        raise SourceTimeoutError(
            f"source timeout budget is not greater than the declared transport timeout "
            f"for source {source.name}{target} ({detail})"
        )

    def effective_timeout(
        self,
        source: SourceBase,
        context: FetchContext,
    ) -> float | None:
        """Return the effective source-call timeout for compatibility."""

        return self._timeout_budget(
            source.timeout_seconds,
            context,
            source=source,
            phase="resolving",
            resource=None,
        ).seconds

    def snapshot_value(
        self,
        *,
        key: ResourceKey,
        source: SourceBase,
        payload: SourcePayload[Any],
        attempts: int,
        latency_ms: float,
        dependency_versions: Mapping[ResourceKey, str] | None = None,
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
            authority_rank=self.authority_resolver.rank_for(key, source.name),
            dependency_versions=dependency_versions or {},
        )

    async def coerce_payload(
        self,
        value: SourcePayload[Any] | Any,
        *,
        copy_context: str,
        fetch_context: FetchContext,
    ) -> SourcePayload[Any]:
        return await self.async_payload_isolator.run(
            partial(self._coerce_payload_sync, value, context=copy_context),
            deadline_monotonic=fetch_context.deadline_monotonic,
            monotonic=self.clock.monotonic,
            deadline_context=f"isolating {copy_context}",
        )

    def _coerce_payload_sync(
        self,
        value: SourcePayload[Any] | Any,
        *,
        context: str,
    ) -> SourcePayload[Any]:
        payload = value if isinstance(value, SourcePayload) else SourcePayload(value=value)
        return SourcePayload(
            value=self.payload_isolator.copy(
                payload.value,
                context=context,
            ),
            observed_at=payload.observed_at,
            metadata=self.payload_isolator.copy_metadata(
                payload.metadata,
                context=f"{context} metadata",
            ),
        )

    @staticmethod
    def is_retryable(error: Exception) -> bool:
        if isinstance(
            error,
            (
                CircuitOpenError,
                SnapshotDeadlineExceededError,
                SourceQueueTimeoutError,
            ),
        ):
            return False

        return isinstance(
            error,
            (
                SourceUnavailableError,
                TimeoutError,
                ConnectionError,
                OSError,
            ),
        )

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

    def _timeout_budget(
        self,
        configured_timeout: float | None,
        context: FetchContext,
        *,
        source: SourceBase,
        phase: str,
        resource: ResourceKey | None,
    ) -> _TimeoutBudget:
        if context.deadline_monotonic is None:
            return _TimeoutBudget(configured_timeout)

        remaining = context.deadline_monotonic - self.clock.monotonic()
        target = self._target_suffix(resource)
        if remaining <= 0:
            raise SnapshotDeadlineExceededError(
                f"snapshot deadline exceeded before {phase} for source {source.name}{target}"
            )

        if configured_timeout is None or remaining <= configured_timeout:
            return _TimeoutBudget(
                seconds=remaining,
                limited_by_deadline=True,
            )

        return _TimeoutBudget(configured_timeout)

    @staticmethod
    async def _await_with_timeout(
        operation: Callable[[], Awaitable[T]],
        timeout_seconds: float | None,
    ) -> T:
        if timeout_seconds is None:
            return await operation()

        task = asyncio.ensure_future(operation())
        try:
            completed, _pending = await asyncio.wait(
                (task,),
                timeout=timeout_seconds,
            )
        except BaseException:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise

        if task in completed:
            return await task

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise _OperationTimedOut

    @staticmethod
    def _target_suffix(resource: ResourceKey | None) -> str:
        return f" while resolving {resource}" if resource is not None else ""
