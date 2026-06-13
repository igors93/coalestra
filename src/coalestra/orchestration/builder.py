from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Collection, Iterable, Mapping
from dataclasses import dataclass, field, replace
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from coalestra.cache.memory import AsyncMemoryCache
from coalestra.core.clock import SystemClock
from coalestra.core.errors import (
    CircuitOpenError,
    DependencyCycleError,
    DependencyResolutionError,
    ResourceResolutionError,
    SnapshotBuildError,
    SourceFailure,
    SourceProtocolError,
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
from coalestra.core.protocols import (
    AsyncCache,
    BatchSnapshotSource,
    Clock,
    DerivedSource,
    EventSink,
    MetricsSink,
    SnapshotSource,
    Source,
    SourceBase,
)
from coalestra.observability.events import NullEventSink
from coalestra.observability.metrics import NullMetrics
from coalestra.orchestration.policy import PolicyResolver
from coalestra.orchestration.singleflight import SingleFlight
from coalestra.resilience.circuit_breaker import CircuitBreaker
from coalestra.resilience.retry import RetryPolicy, run_with_retry

if TYPE_CHECKING:
    from coalestra.orchestration.session import SnapshotSession

SourceKind = Literal["single", "batch", "derived"]
T = TypeVar("T")


@dataclass(frozen=True)
class _ResolutionResult:
    value: SnapshotValue[Any] | None = None
    error: Exception | None = None

    def __post_init__(self) -> None:
        if (self.value is None) == (self.error is None):
            raise ValueError("resolution result must contain exactly one of value or error")


@dataclass(frozen=True)
class _SourceAttempt:
    payload: SourcePayload[Any] | None = None
    error: Exception | None = None
    attempts: int = 1
    latency_ms: float = 0.0

    def __post_init__(self) -> None:
        if (self.payload is None) == (self.error is None):
            raise ValueError("source attempt must contain exactly one of payload or error")


@dataclass
class _ResolutionRuntime:
    semaphore: asyncio.Semaphore
    memo: dict[ResourceKey, SnapshotValue[Any]] = field(default_factory=dict)


class SnapshotBuilder:
    """Build consistent snapshots from prioritized, resilient and coalesced sources.

    The builder accepts three compatible source contracts:

    * ``SnapshotSource`` resolves one resource per call.
    * ``BatchSnapshotSource`` resolves several resources with one call.
    * ``DerivedSource`` computes resources from other resources resolved by this builder.

    Sources of all three kinds participate in the same priority and fallback chain.
    """

    def __init__(
        self,
        sources: Iterable[Source],
        *,
        default_policy: FreshnessPolicy | None = None,
        policy_resolver: PolicyResolver | None = None,
        cache: AsyncCache | None = None,
        single_flight: SingleFlight[ResourceKey, _ResolutionResult] | None = None,
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
        for source in source_list:
            self._validate_source(source)

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
        """Resolve one immutable snapshot in a single stage.

        This method remains the compact API. Multi-stage consumers should use ``session()``.
        """

        session = self.session(
            deadline_seconds=deadline_seconds,
            metadata=metadata,
            snapshot_id=snapshot_id,
        )
        try:
            try:
                await session.resolve(keys, strict=strict)
            except SnapshotBuildError:
                self._record_snapshot_built(session.snapshot(), strict=strict)
                raise
            snapshot = session.snapshot()
            self._record_snapshot_built(snapshot, strict=strict)
            return snapshot
        finally:
            await session.close()

    def session(
        self,
        *,
        deadline_seconds: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> SnapshotSession:
        """Create a multi-stage acquisition session with one identity and deadline."""

        from coalestra.orchestration.session import SnapshotSession

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
            snapshot_id=resolved_snapshot_id,
        )
        runtime = _ResolutionRuntime(semaphore=asyncio.Semaphore(self.max_concurrency))
        return SnapshotSession(
            builder=self,
            context=context,
            runtime=runtime,
        )

    async def _resolve_many(
        self,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: _ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...] = (),
        local_owned: frozenset[ResourceKey] = frozenset(),
    ) -> tuple[dict[ResourceKey, SnapshotValue[Any]], dict[ResourceKey, Exception]]:
        unique_keys = tuple(dict.fromkeys(keys))
        values: dict[ResourceKey, SnapshotValue[Any]] = {}
        errors: dict[ResourceKey, Exception] = {}
        stale_candidates: dict[ResourceKey, SnapshotValue[Any]] = {}
        pending: list[ResourceKey] = []

        for key in unique_keys:
            memoized = runtime.memo.get(key)
            if memoized is not None:
                values[key] = memoized
                continue

            policy = self.policy_resolver.resolve(key)
            lookup = await self.cache.get(key, now=self.clock.now(), policy=policy)
            if lookup.fresh and lookup.value is not None:
                self.metrics.increment("cache_access_total", status="fresh", resource=str(key))
                self.events.emit("cache_hit", resource=str(key), freshness="fresh")
                cached = self._cached_copy(lookup.value, stale=False)
                runtime.memo[key] = cached
                values[key] = cached
                continue

            self.metrics.increment("cache_access_total", status="miss", resource=str(key))
            if lookup.usable_stale and lookup.value is not None:
                stale_candidates[key] = lookup.value
            pending.append(key)

        if not pending:
            return values, errors

        resolution_results: dict[ResourceKey, tuple[_ResolutionResult, bool]] = {}
        directly_owned = tuple(key for key in pending if key in local_owned)
        shared_pending = tuple(key for key in pending if key not in local_owned)

        if directly_owned:
            direct_results = await self._resolve_owned_keys(
                directly_owned,
                context=context,
                runtime=runtime,
                ancestry=ancestry,
                local_owned=local_owned,
            )
            resolution_results.update(
                {key: (result, False) for key, result in direct_results.items()}
            )

        if shared_pending:
            flight_results = await self.single_flight.run_many(
                shared_pending,
                lambda owned: self._resolve_owned_keys(
                    owned,
                    context=context,
                    runtime=runtime,
                    ancestry=ancestry,
                    local_owned=frozenset(owned),
                ),
            )
            resolution_results.update(flight_results)

        for key in pending:
            result, joined_existing = resolution_results[key]
            if result.value is not None:
                value = result.value
                if joined_existing:
                    self.metrics.increment("singleflight_join_total", resource=str(key))
                    value = replace(
                        value,
                        metadata={**value.metadata, "coalesced_request": True},
                    )
                runtime.memo[key] = value
                values[key] = value
                continue

            error = cast(Exception, result.error)
            policy = self.policy_resolver.resolve(key)
            stale_candidate = stale_candidates.get(key)
            if stale_candidate is not None and policy.allow_stale_on_error:
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
                value = self._cached_copy(
                    stale_candidate,
                    stale=True,
                    extra_metadata={
                        "fallback_error_type": type(error).__name__,
                        "fallback_error": str(error),
                    },
                )
                runtime.memo[key] = value
                values[key] = value
            else:
                errors[key] = error

        return values, errors

    async def _resolve_owned_keys(
        self,
        keys: tuple[ResourceKey, ...],
        *,
        context: FetchContext,
        runtime: _ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...],
        local_owned: frozenset[ResourceKey],
    ) -> Mapping[ResourceKey, _ResolutionResult]:
        unresolved = list(keys)
        failures: dict[ResourceKey, list[SourceFailure]] = {key: [] for key in keys}
        best_stale: dict[ResourceKey, SnapshotValue[Any]] = {}
        resolved: dict[ResourceKey, _ResolutionResult] = {}

        for source in self.sources:
            for key in tuple(unresolved):
                memoized = runtime.memo.get(key)
                if memoized is not None:
                    resolved[key] = _ResolutionResult(value=memoized)
            unresolved = [key for key in unresolved if key not in resolved]
            if not unresolved:
                break

            candidates: list[ResourceKey] = []
            for key in unresolved:
                try:
                    supported = source.supports(key)
                except Exception as error:
                    failures[key].append(self._source_failure(source, error, attempts=0))
                    continue
                if supported:
                    candidates.append(key)

            if not candidates:
                continue

            attempts = await self._attempt_source(
                source,
                candidates,
                context=context,
                runtime=runtime,
                ancestry=ancestry,
                local_owned=local_owned,
            )

            for key in candidates:
                attempt = attempts[key]
                if attempt.error is not None:
                    failures[key].append(
                        self._source_failure(source, attempt.error, attempts=attempt.attempts)
                    )
                    self.metrics.increment(
                        "source_fetch_total",
                        status="failure",
                        source=source.name,
                    )
                    self.events.emit(
                        "source_failed",
                        resource=str(key),
                        source=source.name,
                        error_type=type(attempt.error).__name__,
                        error=str(attempt.error),
                    )
                    continue

                payload = cast(SourcePayload[Any], attempt.payload)
                value = self._snapshot_value(
                    key=key,
                    source=source,
                    payload=payload,
                    attempts=attempt.attempts,
                    latency_ms=attempt.latency_ms,
                )
                policy = self.policy_resolver.resolve(key)
                if value.stale:
                    if value.age_seconds <= policy.max_stale_seconds and (
                        key not in best_stale or value.observed_at > best_stale[key].observed_at
                    ):
                        best_stale[key] = value
                    failures[key].append(
                        SourceFailure(
                            source=source.name,
                            error_type="StaleSourceData",
                            message=(
                                f"age {value.age_seconds:.3f}s exceeds "
                                f"ttl {policy.ttl_seconds:.3f}s"
                            ),
                            attempts=attempt.attempts,
                        )
                    )
                    continue

                await self.cache.set(value)
                runtime.memo[key] = value
                resolved[key] = _ResolutionResult(value=value)
                self.metrics.increment("source_fetch_total", status="success", source=source.name)
                self.metrics.observe(
                    "source_fetch_latency_ms",
                    attempt.latency_ms,
                    source=source.name,
                )
                self.events.emit(
                    "resource_resolved",
                    resource=str(key),
                    source=source.name,
                    latency_ms=attempt.latency_ms,
                    attempts=attempt.attempts,
                    source_kind=self._source_kind(source),
                )

            unresolved = [key for key in unresolved if key not in resolved]
            if not unresolved:
                break

        for key in unresolved:
            policy = self.policy_resolver.resolve(key)
            stale = best_stale.get(key)
            if stale is not None and policy.allow_stale_on_error:
                await self.cache.set(stale)
                runtime.memo[key] = stale
                self.metrics.increment("source_fetch_total", status="stale", source=stale.source)
                resolved[key] = _ResolutionResult(value=stale)
                continue
            resolved[key] = _ResolutionResult(
                error=ResourceResolutionError(key, tuple(failures[key]))
            )

        return resolved

    async def _attempt_source(
        self,
        source: Source,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: _ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...],
        local_owned: frozenset[ResourceKey],
    ) -> dict[ResourceKey, _SourceAttempt]:
        kind = self._source_kind(source)
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
            return await self._attempt_batch_source(
                cast(BatchSnapshotSource, source),
                keys,
                context=context,
                runtime=runtime,
            )
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
        runtime: _ResolutionRuntime,
    ) -> dict[ResourceKey, _SourceAttempt]:
        async def fetch_one(key: ResourceKey) -> tuple[ResourceKey, _SourceAttempt]:
            started = self.clock.monotonic()
            try:
                await self.circuit_breaker.before_call(source.name)
                payload, attempts = await run_with_retry(
                    partial(self._fetch_once, source, key, context, runtime),
                    policy=self.retry_policy,
                    retryable=self._is_retryable,
                )
                await self.circuit_breaker.record_success(source.name)
                return key, _SourceAttempt(
                    payload=payload,
                    attempts=attempts,
                    latency_ms=self._elapsed_ms(started),
                )
            except CircuitOpenError as error:
                return key, _SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self._elapsed_ms(started),
                )
            except Exception as error:
                await self.circuit_breaker.record_failure(source.name)
                return key, _SourceAttempt(
                    error=error,
                    attempts=self._attempt_count(error),
                    latency_ms=self._elapsed_ms(started),
                )

        completed = await asyncio.gather(*(fetch_one(key) for key in keys))
        return dict(completed)

    async def _attempt_batch_source(
        self,
        source: BatchSnapshotSource,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: _ResolutionRuntime,
    ) -> dict[ResourceKey, _SourceAttempt]:
        requested = tuple(dict.fromkeys(keys))
        started = self.clock.monotonic()
        try:
            await self.circuit_breaker.before_call(source.name)
            payloads, attempts = await run_with_retry(
                partial(self._fetch_many_once, source, requested, context, runtime),
                policy=self.retry_policy,
                retryable=self._is_retryable,
            )
            await self.circuit_breaker.record_success(source.name)
            latency_ms = self._elapsed_ms(started)
            self.metrics.increment(
                "source_batch_call_total",
                status="success",
                source=source.name,
            )
            self.metrics.observe(
                "source_batch_size",
                float(len(requested)),
                source=source.name,
            )
            return {
                key: (
                    _SourceAttempt(
                        payload=payloads[key],
                        attempts=attempts,
                        latency_ms=latency_ms,
                    )
                    if key in payloads
                    else _SourceAttempt(
                        error=SourceUnavailableError(
                            f"batch source {source.name} omitted resource {key}"
                        ),
                        attempts=attempts,
                        latency_ms=latency_ms,
                    )
                )
                for key in requested
            }
        except CircuitOpenError as error:
            latency_ms = self._elapsed_ms(started)
            return {
                key: _SourceAttempt(error=error, attempts=0, latency_ms=latency_ms)
                for key in requested
            }
        except Exception as error:
            await self.circuit_breaker.record_failure(source.name)
            latency_ms = self._elapsed_ms(started)
            self.metrics.increment(
                "source_batch_call_total",
                status="failure",
                source=source.name,
            )
            return {
                key: _SourceAttempt(
                    error=error,
                    attempts=self._attempt_count(error),
                    latency_ms=latency_ms,
                )
                for key in requested
            }

    async def _attempt_derived_source(
        self,
        source: DerivedSource,
        keys: Collection[ResourceKey],
        *,
        context: FetchContext,
        runtime: _ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...],
        local_owned: frozenset[ResourceKey],
    ) -> dict[ResourceKey, _SourceAttempt]:
        async def derive_one(key: ResourceKey) -> tuple[ResourceKey, _SourceAttempt]:
            started = self.clock.monotonic()
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

                dependency_values, dependency_errors = await self._resolve_many(
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
                await self.circuit_breaker.before_call(source.name)
                payload, attempts = await run_with_retry(
                    partial(
                        self._derive_once,
                        source,
                        key,
                        dependency_snapshot,
                        context,
                        runtime,
                    ),
                    policy=self.retry_policy,
                    retryable=self._is_retryable,
                )
                await self.circuit_breaker.record_success(source.name)
                return key, _SourceAttempt(
                    payload=payload,
                    attempts=attempts,
                    latency_ms=self._elapsed_ms(started),
                )
            except CircuitOpenError as error:
                return key, _SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self._elapsed_ms(started),
                )
            except (DependencyCycleError, DependencyResolutionError, SourceProtocolError) as error:
                return key, _SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self._elapsed_ms(started),
                )
            except Exception as error:
                await self.circuit_breaker.record_failure(source.name)
                return key, _SourceAttempt(
                    error=error,
                    attempts=self._attempt_count(error),
                    latency_ms=self._elapsed_ms(started),
                )

        completed = await asyncio.gather(*(derive_one(key) for key in keys))
        return dict(completed)

    async def _fetch_once(
        self,
        source: SnapshotSource,
        key: ResourceKey,
        context: FetchContext,
        runtime: _ResolutionRuntime,
    ) -> SourcePayload[Any]:
        async def invoke() -> SourcePayload[Any]:
            async with runtime.semaphore:
                return self._coerce_payload(await source.fetch(key, context))

        return await self._with_timeout(source, context, invoke(), resource=key)

    async def _fetch_many_once(
        self,
        source: BatchSnapshotSource,
        keys: tuple[ResourceKey, ...],
        context: FetchContext,
        runtime: _ResolutionRuntime,
    ) -> Mapping[ResourceKey, SourcePayload[Any]]:
        async def invoke() -> Mapping[ResourceKey, SourcePayload[Any]]:
            async with runtime.semaphore:
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
                return {key: self._coerce_payload(value) for key, value in result.items()}

        return await self._with_timeout(source, context, invoke(), resource=None)

    async def _derive_once(
        self,
        source: DerivedSource,
        key: ResourceKey,
        dependencies: Snapshot,
        context: FetchContext,
        runtime: _ResolutionRuntime,
    ) -> SourcePayload[Any]:
        async def invoke() -> SourcePayload[Any]:
            async with runtime.semaphore:
                return self._coerce_payload(await source.derive(key, dependencies, context))

        return await self._with_timeout(source, context, invoke(), resource=key)

    async def _with_timeout(
        self,
        source: SourceBase,
        context: FetchContext,
        operation: Awaitable[T],
        *,
        resource: ResourceKey | None,
    ) -> T:
        timeout = self._effective_timeout(source, context)
        try:
            if timeout is None:
                return await operation
            return await asyncio.wait_for(operation, timeout=timeout)
        except TimeoutError as error:
            target = f" while resolving {resource}" if resource is not None else ""
            raise SourceTimeoutError(f"source {source.name} timed out{target}") from error

    def _effective_timeout(
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

    def _snapshot_value(
        self,
        *,
        key: ResourceKey,
        source: SourceBase,
        payload: SourcePayload[Any],
        attempts: int,
        latency_ms: float,
    ) -> SnapshotValue[Any]:
        fetched_at = self.clock.now()
        observed_at = payload.observed_at if payload.observed_at is not None else fetched_at
        age_seconds = max(0.0, fetched_at - observed_at)
        policy = self.policy_resolver.resolve(key)
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
            metadata=payload.metadata,
        )

    @staticmethod
    def _coerce_payload(value: SourcePayload[Any] | Any) -> SourcePayload[Any]:
        return value if isinstance(value, SourcePayload) else SourcePayload(value=value)

    @staticmethod
    def _source_kind(source: Source) -> SourceKind:
        if callable(getattr(source, "dependencies", None)) and callable(
            getattr(source, "derive", None)
        ):
            return "derived"
        if callable(getattr(source, "fetch_many", None)):
            return "batch"
        return "single"

    @classmethod
    def _validate_source(cls, source: Source) -> None:
        if not str(getattr(source, "name", "")).strip():
            raise ValueError("source name cannot be empty")
        if not callable(getattr(source, "supports", None)):
            raise TypeError(f"source {source.name} must define supports()")
        kind = cls._source_kind(source)
        if kind == "single" and not callable(getattr(source, "fetch", None)):
            raise TypeError(
                f"source {source.name} must define fetch(), fetch_many(), "
                "or dependencies()+derive()"
            )

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

    def _attempt_count(self, error: Exception) -> int:
        return self.retry_policy.max_attempts if self._is_retryable(error) else 1

    def _elapsed_ms(self, started: float) -> float:
        return max(0.0, (self.clock.monotonic() - started) * 1000)

    @staticmethod
    def _source_failure(
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

    def _record_snapshot_built(self, snapshot: Snapshot, *, strict: bool) -> None:
        self.metrics.increment(
            "snapshot_build_total",
            status="error" if snapshot.errors else "success",
        )
        self.events.emit(
            "snapshot_built",
            snapshot_id=snapshot.snapshot_id,
            resources=len(snapshot.resources) + len(snapshot.errors),
            resolved=len(snapshot.resources),
            failed=len(snapshot.errors),
            strict=strict,
        )
