from __future__ import annotations

import asyncio
import inspect
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field, replace
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

from coalestra.cache.memory import AsyncMemoryCache
from coalestra.cache.publisher import ResourcePublisher
from coalestra.concurrency.capacity import CapacityController, CapacitySnapshot
from coalestra.core.clock import SystemClock
from coalestra.core.diagnostics import DiagnosticsCollector
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
from coalestra.core.health import BuilderHealth
from coalestra.core.models import (
    CacheLookup,
    FetchContext,
    FreshnessPolicy,
    RefreshMode,
    ResourceKey,
    Snapshot,
    SnapshotValue,
    SourcePayload,
)
from coalestra.core.protocols import (
    AsyncCache,
    BatchAsyncCache,
    BatchSnapshotSource,
    Clock,
    DerivedSource,
    EventSink,
    MetricsSink,
    SnapshotSource,
    Source,
    SourceBase,
)
from coalestra.core.quality import ObservationPolicy
from coalestra.core.request import SnapshotRequest
from coalestra.observability.events import NullEventSink
from coalestra.observability.metrics import NullMetrics
from coalestra.orchestration.policy import PolicyResolver
from coalestra.orchestration.singleflight import SingleFlight
from coalestra.resilience.circuit_breaker import CircuitBreaker, CircuitIdentity
from coalestra.resilience.policy import (
    ResiliencePolicyResolver,
    SourceResiliencePolicy,
)
from coalestra.resilience.retry import RetryPolicy, attempts_for, run_with_retry

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
    diagnostics: DiagnosticsCollector
    memo: dict[ResourceKey, SnapshotValue[Any]] = field(default_factory=dict)
    cache_stale_results: bool = True


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
        default_resilience: SourceResiliencePolicy | None = None,
        source_resilience: Mapping[str, SourceResiliencePolicy] | None = None,
        resilience_resolver: ResiliencePolicyResolver | None = None,
        clock: Clock | None = None,
        metrics: MetricsSink | None = None,
        events: EventSink | None = None,
        max_concurrency: int = 8,
        source_concurrency: Mapping[str, int] | None = None,
        observation_policy: ObservationPolicy | None = None,
        cache_source_support: bool = True,
        source_support_cache_max_entries: int | None = 100_000,
        manage_lifecycle: bool = False,
    ) -> None:
        source_list = list(sources)
        if not source_list:
            raise ValueError("at least one source is required")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if source_support_cache_max_entries is not None and source_support_cache_max_entries < 1:
            raise ValueError("source_support_cache_max_entries must be at least 1 or None")

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
        default_source_resilience = default_resilience or SourceResiliencePolicy(
            retry=self.retry_policy,
            circuit=self.circuit_breaker.default_policy,
        )
        self.resilience_resolver = resilience_resolver or ResiliencePolicyResolver(
            default_source_resilience,
            overrides=source_resilience,
        )
        self.metrics = metrics or NullMetrics()
        self.events = events or NullEventSink()
        self.observation_policy = observation_policy or ObservationPolicy()
        self.cache_source_support = bool(cache_source_support)
        self.source_support_cache_max_entries = source_support_cache_max_entries
        self.manage_lifecycle = bool(manage_lifecycle)
        self._source_kinds = {source.name: self._source_kind(source) for source in self.sources}
        self._source_support_cache: OrderedDict[tuple[str, ResourceKey], bool] = OrderedDict()
        self.max_concurrency = int(max_concurrency)
        self.capacity = CapacityController(
            global_limit=self.max_concurrency,
            source_limits=source_concurrency,
        )
        for source in self.sources:
            declared_limit = getattr(source, "max_concurrency", None)
            if self.capacity.limit_for(source.name) is None:
                self.capacity.register_source(source.name, declared_limit)
        self._background_refreshes: dict[ResourceKey, asyncio.Task[None]] = {}
        self._closed = False
        self.publisher = ResourcePublisher(
            cache=self.cache,
            clock=self.clock,
            policy_resolver=self.policy_resolver,
            metrics=self.metrics,
            events=self.events,
            observation_policy=self.observation_policy,
        )

    @property
    def closed(self) -> bool:
        return self._closed

    async def capacity_snapshot(self) -> dict[str, CapacitySnapshot]:
        """Return builder-wide and per-source capacity diagnostics."""

        return await self.capacity.snapshot()

    def clear_source_support_cache(self) -> None:
        """Forget memoized ``source.supports(key)`` results.

        Most source capability predicates are structural and stable. Dynamic integrations can
        disable support caching globally or call this method after reconfiguration.
        """

        self._source_support_cache.clear()

    async def health_snapshot(self) -> BuilderHealth:
        """Return an immutable integration health snapshot without performing source I/O."""

        cache_stats = None
        stats = getattr(self.cache, "stats", None)
        if callable(stats):
            cache_stats = await stats()
        return BuilderHealth(
            closed=self._closed,
            background_refreshes=len(self._background_refreshes),
            singleflight_in_flight=await self.single_flight.in_flight(),
            source_support_cache_entries=len(self._source_support_cache),
            capacity=await self.capacity.snapshot(),
            cache=cache_stats,
            circuits=await self.circuit_breaker.snapshot(),
        )

    async def wait_for_refreshes(self) -> None:
        """Wait until every currently scheduled background refresh finishes."""

        while self._background_refreshes:
            tasks = tuple(self._background_refreshes.values())
            await asyncio.gather(*tasks, return_exceptions=True)

    async def aclose(self, *, cancel_refreshes: bool = False) -> None:
        """Close the builder, settle refreshes, and optionally close owned components."""

        if self._closed:
            return
        self._closed = True
        tasks = tuple(self._background_refreshes.values())
        if cancel_refreshes:
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.manage_lifecycle:
            await self._close_managed_components()

    async def __aenter__(self) -> SnapshotBuilder:
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> None:
        await self.aclose()

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

        self._ensure_open()
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

    async def build_request(
        self,
        request: SnapshotRequest,
        *,
        deadline_seconds: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> Snapshot:
        """Resolve required and optional resources with integration-friendly failure semantics."""

        self._ensure_open()
        session = self.session(
            deadline_seconds=deadline_seconds,
            metadata=metadata,
            snapshot_id=snapshot_id,
        )
        try:
            snapshot = await session.resolve_request(request)
            self._record_snapshot_built(snapshot, strict=True)
            return snapshot
        except SnapshotBuildError as error:
            snapshot = session.snapshot()
            self._record_snapshot_built(snapshot, strict=True)
            if error.snapshot is None:
                error.snapshot = snapshot
            raise
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

        self._ensure_open()
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
        runtime = _ResolutionRuntime(
            diagnostics=DiagnosticsCollector(started_monotonic=self.clock.monotonic())
        )
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
        cache_keys: list[ResourceKey] = []

        for key in unique_keys:
            memoized = runtime.memo.get(key)
            if memoized is not None:
                values[key] = memoized
            else:
                cache_keys.append(key)

        policies = {key: self.policy_resolver.resolve(key) for key in cache_keys}
        lookups = await self._cache_get_many(
            cache_keys,
            now=self.clock.now(),
            policies=policies,
            diagnostics=runtime.diagnostics,
        )
        pending: list[ResourceKey] = []

        for key in cache_keys:
            lookup = lookups[key]
            policy = policies[key]
            age = lookup.age_seconds
            if lookup.fresh and lookup.value is not None:
                runtime.diagnostics.cache_hits += 1
                self.metrics.increment("cache_access_total", status="fresh", resource=str(key))
                self.events.emit("cache_hit", resource=str(key), freshness="fresh")
                refresh_scheduled = False
                if age is not None and policy.should_refresh_ahead(age):
                    refresh_scheduled = self._schedule_refresh(
                        key,
                        parent_context=context,
                        diagnostics=runtime.diagnostics,
                        reason="refresh_ahead",
                    )
                cached = self._cached_copy(
                    lookup.value,
                    stale=False,
                    extra_metadata={"refresh_scheduled": refresh_scheduled}
                    if refresh_scheduled
                    else None,
                )
                runtime.memo[key] = cached
                values[key] = cached
                continue

            if (
                lookup.value is not None
                and lookup.usable_stale
                and policy.refresh_mode is RefreshMode.STALE_WHILE_REVALIDATE
            ):
                runtime.diagnostics.cache_hits += 1
                runtime.diagnostics.stale_values += 1
                self.metrics.increment(
                    "cache_access_total",
                    status="stale_while_revalidate",
                    resource=str(key),
                )
                refresh_scheduled = self._schedule_refresh(
                    key,
                    parent_context=context,
                    diagnostics=runtime.diagnostics,
                    reason="stale_while_revalidate",
                )
                cached = self._cached_copy(
                    lookup.value,
                    stale=True,
                    extra_metadata={
                        "refresh_mode": RefreshMode.STALE_WHILE_REVALIDATE.value,
                        "refresh_scheduled": refresh_scheduled,
                    },
                )
                runtime.memo[key] = cached
                values[key] = cached
                continue

            runtime.diagnostics.cache_misses += 1
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
                    runtime.diagnostics.coalesced_requests += 1
                    self.metrics.increment("singleflight_join_total", resource=str(key))
                    value = replace(
                        value,
                        metadata={**value.metadata, "coalesced_request": True},
                    )
                runtime.memo[key] = value
                values[key] = value
                if value.stale:
                    runtime.diagnostics.stale_values += 1
                continue

            error = cast(Exception, result.error)
            policy = self.policy_resolver.resolve(key)
            stale_candidate = stale_candidates.get(key)
            if stale_candidate is not None and policy.allow_stale_on_error:
                runtime.diagnostics.stale_values += 1
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
                    supported = self._source_supports(source, key, runtime.diagnostics)
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

            fresh_values: list[SnapshotValue[Any]] = []
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
                try:
                    value = self._snapshot_value(
                        key=key,
                        source=source,
                        payload=payload,
                        attempts=attempt.attempts,
                        latency_ms=attempt.latency_ms,
                    )
                except SourceProtocolError as error:
                    runtime.diagnostics.future_timestamp_rejections += 1
                    failures[key].append(
                        self._source_failure(source, error, attempts=attempt.attempts)
                    )
                    self.metrics.increment(
                        "source_fetch_total",
                        status="invalid_timestamp",
                        source=source.name,
                    )
                    self.events.emit(
                        "source_payload_rejected",
                        resource=str(key),
                        source=source.name,
                        error=str(error),
                    )
                    continue
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

                fresh_values.append(value)
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
                    source_kind=self._source_kinds[source.name],
                )

            if fresh_values:
                await self._cache_set_many(fresh_values, diagnostics=runtime.diagnostics)

            unresolved = [key for key in unresolved if key not in resolved]
            if not unresolved:
                break

        stale_to_cache: list[SnapshotValue[Any]] = []
        for key in unresolved:
            policy = self.policy_resolver.resolve(key)
            stale = best_stale.get(key)
            if stale is not None and policy.allow_stale_on_error:
                stale_to_cache.append(stale)
                runtime.memo[key] = stale
                self.metrics.increment("source_fetch_total", status="stale", source=stale.source)
                resolved[key] = _ResolutionResult(value=stale)
                continue
            resolved[key] = _ResolutionResult(
                error=ResourceResolutionError(key, tuple(failures[key]))
            )

        if stale_to_cache and runtime.cache_stale_results:
            await self._cache_set_many(stale_to_cache, diagnostics=runtime.diagnostics)
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
        kind = self._source_kinds[source.name]
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
            source_limit = self.capacity.limit_for(batch_source.name)
            wave_size = max(1, min(self.max_concurrency, source_limit or self.max_concurrency))
            merged: dict[ResourceKey, _SourceAttempt] = {}
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
        runtime: _ResolutionRuntime,
    ) -> dict[ResourceKey, _SourceAttempt]:
        resilience = self._resilience_for(source)

        async def fetch_one(key: ResourceKey) -> tuple[ResourceKey, _SourceAttempt]:
            started = self.clock.monotonic()
            try:
                await self.circuit_breaker.before_call(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                payload, attempts = await run_with_retry(
                    partial(self._fetch_once, source, key, context, runtime),
                    policy=resilience.retry,
                    retryable=self._is_retryable,
                    deadline_monotonic=context.deadline_monotonic,
                    monotonic=self.clock.monotonic,
                )
                runtime.diagnostics.retries += max(0, attempts - 1)
                await self._record_payload_circuit_outcome(
                    source,
                    key,
                    payload,
                    resilience,
                )
                return key, _SourceAttempt(
                    payload=payload,
                    attempts=attempts,
                    latency_ms=self._elapsed_ms(started),
                )
            except asyncio.CancelledError:
                await self.circuit_breaker.record_abandoned(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                raise
            except CircuitOpenError as error:
                self._record_circuit_open(source, key, error)
                return key, _SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self._elapsed_ms(started),
                )
            except Exception as error:
                await self.circuit_breaker.record_failure(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                if isinstance(error, SourceProtocolError) and "in the future" in str(error):
                    runtime.diagnostics.future_timestamp_rejections += 1
                attempts = self._attempt_count(error, resilience.retry)
                runtime.diagnostics.retries += max(0, attempts - 1)
                return key, _SourceAttempt(
                    error=error,
                    attempts=attempts,
                    latency_ms=self._elapsed_ms(started),
                )

        completed = await asyncio.gather(*(fetch_one(key) for key in keys))
        for _key, attempt in completed:
            runtime.diagnostics.record_source_latency(source.name, attempt.latency_ms)
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
        resilience = self._resilience_for(source)
        started = self.clock.monotonic()
        results: dict[ResourceKey, _SourceAttempt] = {}

        groups = self._circuit_groups(source, requested, resilience)
        active_groups: list[tuple[ResourceKey, tuple[ResourceKey, ...]]] = []
        allowed: set[ResourceKey] = set()
        for representative, grouped_keys in groups:
            try:
                await self.circuit_breaker.before_call(
                    source.name,
                    key=representative,
                    policy=resilience.circuit,
                )
                active_groups.append((representative, grouped_keys))
                allowed.update(grouped_keys)
            except CircuitOpenError as error:
                self._record_circuit_open(source, representative, error)
                for key in grouped_keys:
                    results[key] = _SourceAttempt(
                        error=error,
                        attempts=0,
                        latency_ms=self._elapsed_ms(started),
                    )

        active_keys = tuple(key for key in requested if key in allowed)
        if not active_keys:
            return results

        try:
            payloads, attempts = await run_with_retry(
                partial(self._fetch_many_once, source, active_keys, context, runtime),
                policy=resilience.retry,
                retryable=self._is_retryable,
                deadline_monotonic=context.deadline_monotonic,
                monotonic=self.clock.monotonic,
            )
            runtime.diagnostics.retries += max(0, attempts - 1)
            latency_ms = self._elapsed_ms(started)
            runtime.diagnostics.record_source_latency(source.name, latency_ms)
            invalid_payloads: dict[ResourceKey, SourceProtocolError] = {}
            for key, payload in payloads.items():
                try:
                    self._validate_payload_timestamp(key, payload)
                except SourceProtocolError as error:
                    invalid_payloads[key] = error
                    runtime.diagnostics.future_timestamp_rejections += 1
            returned = set(payloads).difference(invalid_payloads)
            for representative, grouped_keys in active_groups:
                has_fresh_value = any(
                    key in returned and self._payload_is_fresh(key, payloads[key])
                    for key in grouped_keys
                )
                if has_fresh_value:
                    await self.circuit_breaker.record_success(
                        source.name,
                        key=representative,
                        policy=resilience.circuit,
                    )
                else:
                    await self.circuit_breaker.record_failure(
                        source.name,
                        key=representative,
                        policy=resilience.circuit,
                    )

            self.metrics.increment(
                "source_batch_call_total",
                status="success",
                source=source.name,
            )
            self.metrics.observe(
                "source_batch_size",
                float(len(active_keys)),
                source=source.name,
            )
            for key in active_keys:
                if key in invalid_payloads:
                    results[key] = _SourceAttempt(
                        error=invalid_payloads[key],
                        attempts=attempts,
                        latency_ms=latency_ms,
                    )
                elif key in payloads:
                    results[key] = _SourceAttempt(
                        payload=payloads[key],
                        attempts=attempts,
                        latency_ms=latency_ms,
                    )
                else:
                    results[key] = _SourceAttempt(
                        error=SourceUnavailableError(
                            f"batch source {source.name} omitted resource {key}"
                        ),
                        attempts=attempts,
                        latency_ms=latency_ms,
                    )
            return results
        except asyncio.CancelledError:
            for representative, _grouped_keys in active_groups:
                await self.circuit_breaker.record_abandoned(
                    source.name,
                    key=representative,
                    policy=resilience.circuit,
                )
            raise
        except Exception as error:
            for representative, _grouped_keys in active_groups:
                await self.circuit_breaker.record_failure(
                    source.name,
                    key=representative,
                    policy=resilience.circuit,
                )
            latency_ms = self._elapsed_ms(started)
            runtime.diagnostics.record_source_latency(source.name, latency_ms)
            self.metrics.increment(
                "source_batch_call_total",
                status="failure",
                source=source.name,
            )
            attempts = self._attempt_count(error, resilience.retry)
            runtime.diagnostics.retries += max(0, attempts - 1)
            for key in active_keys:
                results[key] = _SourceAttempt(
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
        runtime: _ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...],
        local_owned: frozenset[ResourceKey],
    ) -> dict[ResourceKey, _SourceAttempt]:
        resilience = self._resilience_for(source)

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
                await self.circuit_breaker.before_call(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                payload, attempts = await run_with_retry(
                    partial(
                        self._derive_once,
                        source,
                        key,
                        dependency_snapshot,
                        context,
                        runtime,
                    ),
                    policy=resilience.retry,
                    retryable=self._is_retryable,
                    deadline_monotonic=context.deadline_monotonic,
                    monotonic=self.clock.monotonic,
                )
                runtime.diagnostics.retries += max(0, attempts - 1)
                await self._record_payload_circuit_outcome(
                    source,
                    key,
                    payload,
                    resilience,
                )
                return key, _SourceAttempt(
                    payload=payload,
                    attempts=attempts,
                    latency_ms=self._elapsed_ms(started),
                )
            except asyncio.CancelledError:
                await self.circuit_breaker.record_abandoned(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                raise
            except CircuitOpenError as error:
                self._record_circuit_open(source, key, error)
                return key, _SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self._elapsed_ms(started),
                )
            except (DependencyCycleError, DependencyResolutionError) as error:
                return key, _SourceAttempt(
                    error=error,
                    attempts=0,
                    latency_ms=self._elapsed_ms(started),
                )
            except SourceProtocolError as error:
                await self.circuit_breaker.record_failure(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                runtime.diagnostics.future_timestamp_rejections += int(
                    "in the future" in str(error)
                )
                return key, _SourceAttempt(
                    error=error,
                    attempts=1,
                    latency_ms=self._elapsed_ms(started),
                )
            except Exception as error:
                await self.circuit_breaker.record_failure(
                    source.name,
                    key=key,
                    policy=resilience.circuit,
                )
                attempts = self._attempt_count(error, resilience.retry)
                runtime.diagnostics.retries += max(0, attempts - 1)
                return key, _SourceAttempt(
                    error=error,
                    attempts=attempts,
                    latency_ms=self._elapsed_ms(started),
                )

        completed = await asyncio.gather(*(derive_one(key) for key in keys))
        for _key, attempt in completed:
            runtime.diagnostics.record_source_latency(source.name, attempt.latency_ms)
        return dict(completed)

    async def _fetch_once(
        self,
        source: SnapshotSource,
        key: ResourceKey,
        context: FetchContext,
        runtime: _ResolutionRuntime,
    ) -> SourcePayload[Any]:
        async def invoke() -> SourcePayload[Any]:
            wait_started = self.clock.monotonic()
            async with self.capacity.slot(source.name):
                runtime.diagnostics.record_source_call(source.name, kind="single")
                self.metrics.observe(
                    "source_capacity_wait_ms",
                    self._elapsed_ms(wait_started),
                    source=source.name,
                )
                return self._coerce_payload(await source.fetch(key, context))

        return await self._with_timeout(source, context, invoke, resource=key)

    async def _fetch_many_once(
        self,
        source: BatchSnapshotSource,
        keys: tuple[ResourceKey, ...],
        context: FetchContext,
        runtime: _ResolutionRuntime,
    ) -> Mapping[ResourceKey, SourcePayload[Any]]:
        async def invoke() -> Mapping[ResourceKey, SourcePayload[Any]]:
            wait_started = self.clock.monotonic()
            async with self.capacity.slot(source.name):
                runtime.diagnostics.record_source_call(source.name, kind="batch")
                self.metrics.observe(
                    "source_capacity_wait_ms",
                    self._elapsed_ms(wait_started),
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
                return {key: self._coerce_payload(value) for key, value in result.items()}

        return await self._with_timeout(source, context, invoke, resource=None)

    async def _derive_once(
        self,
        source: DerivedSource,
        key: ResourceKey,
        dependencies: Snapshot,
        context: FetchContext,
        runtime: _ResolutionRuntime,
    ) -> SourcePayload[Any]:
        async def invoke() -> SourcePayload[Any]:
            wait_started = self.clock.monotonic()
            async with self.capacity.slot(source.name):
                runtime.diagnostics.record_source_call(source.name, kind="derived")
                self.metrics.observe(
                    "source_capacity_wait_ms",
                    self._elapsed_ms(wait_started),
                    source=source.name,
                )
                return self._coerce_payload(await source.derive(key, dependencies, context))

        return await self._with_timeout(source, context, invoke, resource=key)

    async def _with_timeout(
        self,
        source: SourceBase,
        context: FetchContext,
        operation: Callable[[], Awaitable[T]],
        *,
        resource: ResourceKey | None,
    ) -> T:
        timeout = self._effective_timeout(source, context)
        try:
            if timeout is None:
                return await operation()
            return await asyncio.wait_for(operation(), timeout=timeout)
        except (TimeoutError, asyncio.TimeoutError) as error:
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
        future_seconds = self._validate_payload_timestamp(key, payload)
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
    def _coerce_payload(value: SourcePayload[Any] | Any) -> SourcePayload[Any]:
        return value if isinstance(value, SourcePayload) else SourcePayload(value=value)

    def _source_supports(
        self,
        source: SourceBase,
        key: ResourceKey,
        diagnostics: DiagnosticsCollector,
    ) -> bool:
        cacheable = self.cache_source_support and bool(getattr(source, "cache_supports", True))
        cache_key = (source.name, key)
        if cacheable and cache_key in self._source_support_cache:
            diagnostics.support_cache_hits += 1
            self._source_support_cache.move_to_end(cache_key)
            return self._source_support_cache[cache_key]
        diagnostics.support_cache_misses += 1
        supported = bool(source.supports(key))
        if cacheable:
            self._source_support_cache[cache_key] = supported
            self._source_support_cache.move_to_end(cache_key)
            if self.source_support_cache_max_entries is not None:
                while len(self._source_support_cache) > self.source_support_cache_max_entries:
                    self._source_support_cache.popitem(last=False)
        return supported

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
        max_concurrency = getattr(source, "max_concurrency", None)
        if max_concurrency is not None and int(max_concurrency) < 1:
            raise ValueError(f"source {source.name} max_concurrency must be at least 1")
        max_batch_size = getattr(source, "max_batch_size", None)
        if max_batch_size is not None and int(max_batch_size) < 1:
            raise ValueError(f"source {source.name} max_batch_size must be at least 1")
        declared_resilience = getattr(source, "resilience_policy", None)
        if declared_resilience is not None and not isinstance(
            declared_resilience, SourceResiliencePolicy
        ):
            raise TypeError(
                f"source {source.name} resilience_policy must be SourceResiliencePolicy or None"
            )
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

    def _resilience_for(self, source: SourceBase) -> SourceResiliencePolicy:
        declared = getattr(source, "resilience_policy", None)
        if declared is not None and not isinstance(declared, SourceResiliencePolicy):
            raise TypeError(
                f"source {source.name} resilience_policy must be SourceResiliencePolicy or None"
            )
        return self.resilience_resolver.resolve(source.name, declared=declared)

    def _circuit_groups(
        self,
        source: SourceBase,
        keys: Collection[ResourceKey],
        resilience: SourceResiliencePolicy,
    ) -> list[tuple[ResourceKey, tuple[ResourceKey, ...]]]:
        grouped: dict[CircuitIdentity, list[ResourceKey]] = {}
        for key in keys:
            identity = self.circuit_breaker.identity_for(
                source.name,
                key=key,
                scope=resilience.circuit.scope,
            )
            grouped.setdefault(identity, []).append(key)
        return [(items[0], tuple(items)) for items in grouped.values()]

    def _validate_payload_timestamp(
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

    def _payload_is_fresh(
        self,
        key: ResourceKey,
        payload: SourcePayload[Any],
    ) -> bool:
        self._validate_payload_timestamp(key, payload)
        observed_at = payload.observed_at
        if observed_at is None:
            return True
        age_seconds = max(0.0, self.clock.now() - observed_at)
        return age_seconds <= self.policy_resolver.resolve(key).ttl_seconds

    async def _record_payload_circuit_outcome(
        self,
        source: SourceBase,
        key: ResourceKey,
        payload: SourcePayload[Any],
        resilience: SourceResiliencePolicy,
    ) -> None:
        recorder = (
            self.circuit_breaker.record_success
            if self._payload_is_fresh(key, payload)
            else self.circuit_breaker.record_failure
        )
        await recorder(
            source.name,
            key=key,
            policy=resilience.circuit,
        )

    def _record_circuit_open(
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

    def _attempt_count(self, error: Exception, retry_policy: RetryPolicy) -> int:
        default = retry_policy.max_attempts if self._is_retryable(error) else 1
        return attempts_for(error, default=default)

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

    async def _cache_get_many(
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
            return await self.cache.get_many(unique, now=now, policies=policies)
        completed = await asyncio.gather(
            *(self.cache.get(key, now=now, policy=policies[key]) for key in unique)
        )
        return dict(zip(unique, completed, strict=True))

    async def _cache_set_many(
        self,
        values: Collection[SnapshotValue[Any]],
        *,
        diagnostics: DiagnosticsCollector,
    ) -> None:
        unique = tuple({value.key: value for value in values}.values())
        if not unique:
            return
        if isinstance(self.cache, BatchAsyncCache):
            diagnostics.cache_batch_writes += 1
            await self.cache.set_many(unique)
            return
        await asyncio.gather(*(self.cache.set(value) for value in unique))

    def _schedule_refresh(
        self,
        key: ResourceKey,
        *,
        parent_context: FetchContext,
        diagnostics: DiagnosticsCollector,
        reason: str,
    ) -> bool:
        if self._closed:
            return False
        existing = self._background_refreshes.get(key)
        if existing is not None and not existing.done():
            return True
        diagnostics.refresh_scheduled += 1
        task = asyncio.create_task(
            self._refresh_resource(
                key,
                parent_context=parent_context,
                diagnostics=diagnostics,
                reason=reason,
            ),
            name=f"coalestra-refresh:{key}",
        )
        self._background_refreshes[key] = task

        def cleanup(completed: asyncio.Task[None], resource: ResourceKey = key) -> None:
            self._finish_refresh(resource, completed)

        task.add_done_callback(cleanup)
        self.metrics.increment("resource_refresh_total", status="scheduled", resource=str(key))
        self.events.emit(
            "resource_refresh_scheduled",
            resource=str(key),
            reason=reason,
            snapshot_id=parent_context.snapshot_id,
        )
        return True

    async def _refresh_resource(
        self,
        key: ResourceKey,
        *,
        parent_context: FetchContext,
        diagnostics: DiagnosticsCollector,
        reason: str,
    ) -> None:
        now = self.clock.now()
        context = FetchContext(
            requested_at=now,
            metadata={
                **parent_context.metadata,
                "background_refresh": True,
                "refresh_reason": reason,
                "parent_snapshot_id": parent_context.snapshot_id,
            },
            snapshot_id=f"{parent_context.snapshot_id}:refresh:{uuid.uuid4().hex[:8]}",
        )
        runtime = _ResolutionRuntime(
            diagnostics=diagnostics,
            cache_stale_results=False,
        )
        try:
            flight_results = await self.single_flight.run_many(
                (key,),
                lambda owned: self._resolve_owned_keys(
                    owned,
                    context=context,
                    runtime=runtime,
                    ancestry=(),
                    local_owned=frozenset(owned),
                ),
            )
            result, _joined = flight_results[key]
            if result.value is None or result.value.stale:
                error = result.error or SourceUnavailableError(
                    f"background refresh for {key} did not produce a fresh value"
                )
                raise error
            diagnostics.refresh_completed += 1
            self.metrics.increment("resource_refresh_total", status="success", resource=str(key))
            self.events.emit(
                "resource_refresh_completed",
                resource=str(key),
                source=result.value.source,
                reason=reason,
            )
        except asyncio.CancelledError:
            diagnostics.refresh_failed += 1
            self.metrics.increment("resource_refresh_total", status="cancelled", resource=str(key))
            raise
        except Exception as error:
            diagnostics.refresh_failed += 1
            self.metrics.increment("resource_refresh_total", status="failure", resource=str(key))
            self.events.emit(
                "resource_refresh_failed",
                resource=str(key),
                reason=reason,
                error_type=type(error).__name__,
                error=str(error),
            )

    def _finish_refresh(self, key: ResourceKey, task: asyncio.Task[None]) -> None:
        if self._background_refreshes.get(key) is task:
            self._background_refreshes.pop(key, None)
        if not task.cancelled():
            task.exception()

    async def _close_managed_components(self) -> None:
        seen: set[int] = set()
        components = (*self.sources, self.cache, self.events, self.metrics)
        for component in components:
            identity = id(component)
            if identity in seen:
                continue
            seen.add(identity)
            await self._close_component(component)

    @staticmethod
    async def _close_component(component: object) -> None:
        async_close = getattr(component, "aclose", None)
        if callable(async_close):
            if inspect.iscoroutinefunction(async_close):
                await async_close()
            else:
                result = await asyncio.to_thread(async_close)
                if inspect.isawaitable(result):
                    await result
            return
        close = getattr(component, "close", None)
        if callable(close):
            if inspect.iscoroutinefunction(close):
                await close()
            else:
                result = await asyncio.to_thread(close)
                if inspect.isawaitable(result):
                    await result

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("SnapshotBuilder is closed")

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
            diagnostics=snapshot.diagnostics,
        )
