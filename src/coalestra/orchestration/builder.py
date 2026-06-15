from __future__ import annotations

import uuid
from collections.abc import Collection, Iterable, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

from coalestra.cache.memory import AsyncMemoryCache
from coalestra.cache.publisher import ResourcePublisher
from coalestra.concurrency.capacity import CapacityController, CapacitySnapshot
from coalestra.core.authority import AuthorityPolicyResolver, SourceAuthorityPolicy
from coalestra.core.clock import SystemClock
from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.errors import (
    ResourceResolutionError,
    SnapshotBuildError,
    SourceFailure,
    SourceProtocolError,
)
from coalestra.core.health import BuilderHealth, OperationalHealthTracker
from coalestra.core.isolation import PayloadCopier, PayloadIsolator
from coalestra.core.models import (
    CacheWriteStatus,
    FetchContext,
    FreshnessPolicy,
    RefreshMode,
    ResourceKey,
    Snapshot,
    SnapshotValue,
    SourcePayload,
)
from coalestra.core.protocols import AsyncCache, Clock, EventSink, MetricsSink, Source
from coalestra.core.quality import ObservationPolicy
from coalestra.core.request import SnapshotRequest
from coalestra.observability.events import NullEventSink
from coalestra.observability.labels import resource_metric_labels
from coalestra.observability.metrics import NullMetrics
from coalestra.orchestration.cache_access import CacheAccess
from coalestra.orchestration.lifecycle import close_components
from coalestra.orchestration.policy import PolicyResolver
from coalestra.orchestration.refresh import RefreshManager
from coalestra.orchestration.runtime import (
    ResolutionResult,
    ResolutionRuntime,
    SourceAttempt,
)
from coalestra.orchestration.singleflight import SingleFlight
from coalestra.orchestration.source_calls import SourceCalls
from coalestra.orchestration.source_catalog import SourceCatalog
from coalestra.orchestration.source_executor import SourceExecutor
from coalestra.resilience.circuit_breaker import CircuitBreaker
from coalestra.resilience.policy import ResiliencePolicyResolver, SourceResiliencePolicy
from coalestra.resilience.retry import RetryPolicy

if TYPE_CHECKING:
    from coalestra.orchestration.session import SnapshotSession

# Compatibility aliases for private types used by older internal integrations.
_ResolutionResult = ResolutionResult
_ResolutionRuntime = ResolutionRuntime
_SourceAttempt = SourceAttempt


class SnapshotBuilder:
    """Build consistent snapshots from prioritized, resilient and coalesced sources.

    Public orchestration remains on this facade. Source discovery, cache access, source execution,
    refresh scheduling, and component shutdown are delegated to focused internal collaborators.
    """

    def __init__(
        self,
        sources: Iterable[Source],
        *,
        default_policy: FreshnessPolicy | None = None,
        policy_resolver: PolicyResolver | None = None,
        authority_policy: SourceAuthorityPolicy | None = None,
        authority_resolver: AuthorityPolicyResolver | None = None,
        cache: AsyncCache | None = None,
        single_flight: SingleFlight[ResourceKey, ResolutionResult] | None = None,
        retry_policy: RetryPolicy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        default_resilience: SourceResiliencePolicy | None = None,
        source_resilience: Mapping[str, SourceResiliencePolicy] | None = None,
        resilience_resolver: ResiliencePolicyResolver | None = None,
        clock: Clock | None = None,
        metrics: MetricsSink | None = None,
        events: EventSink | None = None,
        max_concurrency: int = 8,
        max_pending_tasks: int | None = None,
        source_concurrency: Mapping[str, int] | None = None,
        observation_policy: ObservationPolicy | None = None,
        cache_source_support: bool = True,
        source_support_cache_max_entries: int | None = 100_000,
        manage_lifecycle: bool = False,
        payload_copier: PayloadCopier | None = None,
    ) -> None:
        if authority_policy is not None and authority_resolver is not None:
            raise ValueError("authority_policy and authority_resolver cannot be provided together")

        source_list = list(sources)
        if not source_list:
            raise ValueError("at least one source is required")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if max_pending_tasks is not None and max_pending_tasks < 1:
            raise ValueError("max_pending_tasks must be at least 1 or None")
        if source_support_cache_max_entries is not None and source_support_cache_max_entries < 1:
            raise ValueError("source_support_cache_max_entries must be at least 1 or None")

        self.clock = clock or SystemClock()
        default = default_policy or FreshnessPolicy(
            ttl_seconds=1.0,
            max_stale_seconds=10.0,
        )
        self.policy_resolver = policy_resolver or PolicyResolver(default)
        self.authority_resolver = authority_resolver or AuthorityPolicyResolver(authority_policy)
        self._payload_isolator = PayloadIsolator(payload_copier)
        self.cache = cache or AsyncMemoryCache(payload_copier=payload_copier)
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
        self.max_concurrency = int(max_concurrency)
        self.max_pending_tasks = (
            self.max_concurrency if max_pending_tasks is None else int(max_pending_tasks)
        )
        self._health_tracker = OperationalHealthTracker()

        self._source_catalog = SourceCatalog(
            source_list,
            resilience_resolver=self.resilience_resolver,
            circuit_breaker=self.circuit_breaker,
            cache_supports=self.cache_source_support,
            support_cache_max_entries=self.source_support_cache_max_entries,
        )
        self.sources = self._source_catalog.sources
        self._source_kinds = self._source_catalog.kinds
        self._source_support_cache = self._source_catalog.support_cache

        self.capacity = CapacityController(
            global_limit=self.max_concurrency,
            source_limits=source_concurrency,
        )
        for source in self.sources:
            declared_limit = getattr(source, "max_concurrency", None)
            if self.capacity.limit_for(source.name) is None:
                self.capacity.register_source(source.name, declared_limit)

        self._cache_access = CacheAccess(
            cache=self.cache,
            clock=self.clock,
            payload_isolator=self._payload_isolator,
            max_pending_tasks=self.max_pending_tasks,
            health_tracker=self._health_tracker,
        )
        self._source_calls = SourceCalls(
            clock=self.clock,
            capacity=self.capacity,
            circuit_breaker=self.circuit_breaker,
            policy_resolver=self.policy_resolver,
            metrics=self.metrics,
            events=self.events,
            observation_policy=self.observation_policy,
            payload_isolator=self._payload_isolator,
            authority_resolver=self.authority_resolver,
        )
        self._source_executor = SourceExecutor(
            source_catalog=self._source_catalog,
            source_calls=self._source_calls,
            resolve_many=self._resolve_many,
            max_concurrency=self.max_concurrency,
            max_pending_tasks=self.max_pending_tasks,
            health_tracker=self._health_tracker,
        )
        self._refresh_manager = RefreshManager(
            clock=self.clock,
            single_flight=self.single_flight,
            resolve_owned=self._resolve_owned_keys,
            metrics=self.metrics,
            events=self.events,
        )
        self._background_refreshes = self._refresh_manager.tasks
        self._closed = False
        self.publisher = ResourcePublisher(
            cache=self.cache,
            clock=self.clock,
            policy_resolver=self.policy_resolver,
            metrics=self.metrics,
            events=self.events,
            observation_policy=self.observation_policy,
            payload_isolator=self._payload_isolator,
            authority_resolver=self.authority_resolver,
            max_pending_tasks=self.max_pending_tasks,
            health_tracker=self._health_tracker,
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

        self._source_catalog.clear_support_cache()

    async def health_snapshot(self) -> BuilderHealth:
        """Return an immutable integration health snapshot without performing source I/O."""

        cache_stats = None
        stats = getattr(self.cache, "stats", None)
        if callable(stats):
            cache_stats = await stats()
        capacity = await self.capacity.snapshot()
        operational = self._health_tracker.snapshot()
        return BuilderHealth(
            closed=self._closed,
            background_refreshes=self._refresh_manager.count,
            singleflight_in_flight=await self.single_flight.in_flight(),
            source_support_cache_entries=self._source_catalog.support_cache_size,
            capacity=capacity,
            cache=cache_stats,
            circuits=await self.circuit_breaker.snapshot(),
            active_dispatch_workers=operational.active_dispatch_workers,
            waiting_for_capacity=sum(snapshot.waiting for snapshot in capacity.values()),
            queue_timeout_count=operational.queue_timeout_count,
            source_timeout_count=operational.source_timeout_count,
            deadline_exceeded_count=operational.deadline_exceeded_count,
            revalidation_attempt_count=operational.revalidation_attempt_count,
            revalidation_failure_count=operational.revalidation_failure_count,
        )

    async def wait_for_refreshes(self) -> None:
        """Wait until every currently scheduled background refresh finishes."""

        await self._refresh_manager.wait()

    async def aclose(self, *, cancel_refreshes: bool = False) -> None:
        """Close the builder, settle refreshes, and optionally close owned components."""

        if self._closed:
            return
        self._closed = True
        await self._refresh_manager.close(cancel=cancel_refreshes)
        if self.manage_lifecycle:
            await close_components((*self.sources, self.cache, self.events, self.metrics))

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
                self._record_snapshot_built(
                    session.snapshot(),
                    strict=strict,
                    failed=True,
                )
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
            self._record_snapshot_built(snapshot, strict=True, failed=False)
            return snapshot
        except SnapshotBuildError as error:
            snapshot = session.snapshot()
            self._record_snapshot_built(snapshot, strict=True, failed=True)
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
        runtime = ResolutionRuntime(
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
        runtime: ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...] = (),
        local_owned: frozenset[ResourceKey] = frozenset(),
    ) -> tuple[dict[ResourceKey, SnapshotValue[Any]], dict[ResourceKey, Exception]]:
        unique_keys = tuple(dict.fromkeys(keys))
        values: dict[ResourceKey, SnapshotValue[Any]] = {}
        errors: dict[ResourceKey, Exception] = {}
        stale_candidates: dict[ResourceKey, SnapshotValue[Any]] = {}
        cache_keys: list[ResourceKey] = []

        pending: list[ResourceKey] = []
        for key in unique_keys:
            if runtime.requires_refresh(key):
                runtime.diagnostics.cache_misses += 1
                self.metrics.increment(
                    "cache_access_total",
                    status="revalidation_bypass",
                    **resource_metric_labels(key),
                )
                self.events.emit(
                    "cache_bypassed",
                    resource=str(key),
                    reason="session_revalidation",
                )
                pending.append(key)
                continue

            memoized = runtime.memo.get(key)
            if memoized is not None:
                values[key] = memoized
            else:
                cache_keys.append(key)

        policies = {key: self.policy_resolver.resolve(key) for key in cache_keys}
        lookups = await self._cache_access.get_many(
            cache_keys,
            now=self.clock.now(),
            policies=policies,
            diagnostics=runtime.diagnostics,
        )
        for key in cache_keys:
            lookup = lookups[key]
            policy = policies[key]
            age = lookup.age_seconds
            if lookup.fresh and lookup.value is not None:
                runtime.diagnostics.cache_hits += 1
                self.metrics.increment(
                    "cache_access_total",
                    status="fresh",
                    **resource_metric_labels(key),
                )
                self.events.emit("cache_hit", resource=str(key), freshness="fresh")
                refresh_scheduled = False
                if age is not None and policy.should_refresh_ahead(age):
                    refresh_scheduled = self._refresh_manager.schedule(
                        key,
                        parent_context=context,
                        diagnostics=runtime.diagnostics,
                        reason="refresh_ahead",
                    )
                cached = self._cache_access.cached_copy(
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
                    **resource_metric_labels(key),
                )
                refresh_scheduled = self._refresh_manager.schedule(
                    key,
                    parent_context=context,
                    diagnostics=runtime.diagnostics,
                    reason="stale_while_revalidate",
                )
                cached = self._cache_access.cached_copy(
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
            self.metrics.increment(
                "cache_access_total",
                status="miss",
                **resource_metric_labels(key),
            )
            if lookup.usable_stale and lookup.value is not None:
                stale_candidates[key] = lookup.value
            pending.append(key)

        if not pending:
            return values, errors

        resolution_results: dict[ResourceKey, tuple[ResolutionResult, bool]] = {}
        directly_owned = tuple(
            key for key in pending if key in local_owned or runtime.requires_refresh(key)
        )
        directly_owned_set = frozenset(directly_owned)
        shared_pending = tuple(key for key in pending if key not in directly_owned_set)

        if directly_owned:
            direct_results = await self._resolve_owned_keys(
                directly_owned,
                context=context,
                runtime=runtime,
                ancestry=ancestry,
                local_owned=local_owned | directly_owned_set,
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
                    local_owned=local_owned | frozenset(owned),
                ),
            )
            resolution_results.update(flight_results)

        for key in pending:
            result, joined_existing = resolution_results[key]
            if result.value is not None:
                value = result.value
                if joined_existing:
                    runtime.diagnostics.coalesced_requests += 1
                    self.metrics.increment(
                        "singleflight_join_total",
                        **resource_metric_labels(key),
                    )
                    value = replace(
                        value,
                        metadata={**value.metadata, "coalesced_request": True},
                    )
                runtime.memo[key] = value
                runtime.mark_refreshed(key)
                values[key] = value
                if value.stale:
                    runtime.diagnostics.stale_values += 1
                continue

            error = cast(Exception, result.error)
            policy = self.policy_resolver.resolve(key)
            stale_candidate = stale_candidates.get(key)
            if (
                stale_candidate is not None
                and policy.allow_stale_on_error
                and not runtime.requires_refresh(key)
            ):
                runtime.diagnostics.stale_values += 1
                self.metrics.increment(
                    "cache_access_total",
                    status="stale_fallback",
                    **resource_metric_labels(key),
                )
                self.events.emit(
                    "stale_fallback_used",
                    resource=str(key),
                    error_type=type(error).__name__,
                )
                value = self._cache_access.cached_copy(
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
        runtime: ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...],
        local_owned: frozenset[ResourceKey],
    ) -> Mapping[ResourceKey, ResolutionResult]:
        unresolved = list(keys)
        failures: dict[ResourceKey, list[SourceFailure]] = {key: [] for key in keys}
        best_stale: dict[ResourceKey, SnapshotValue[Any]] = {}
        resolved: dict[ResourceKey, ResolutionResult] = {}

        for source in self.sources:
            for key in tuple(unresolved):
                if runtime.requires_refresh(key):
                    continue
                memoized = runtime.memo.get(key)
                if memoized is not None:
                    resolved[key] = ResolutionResult(value=memoized)
            unresolved = [key for key in unresolved if key not in resolved]
            if not unresolved:
                break

            candidates: list[ResourceKey] = []
            for key in unresolved:
                try:
                    supported = self._source_catalog.supports(source, key, runtime.diagnostics)
                except Exception as error:
                    failures[key].append(
                        self._source_calls.source_failure(source, error, attempts=0)
                    )
                    continue
                if supported:
                    candidates.append(key)

            if not candidates:
                continue

            attempts = await self._source_executor.attempt(
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
                        self._source_calls.source_failure(
                            source, attempt.error, attempts=attempt.attempts
                        )
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
                    value = self._source_calls.snapshot_value(
                        key=key,
                        source=source,
                        payload=payload,
                        attempts=attempt.attempts,
                        latency_ms=attempt.latency_ms,
                        dependency_versions=attempt.dependency_versions,
                    )
                except SourceProtocolError as error:
                    runtime.diagnostics.future_timestamp_rejections += 1
                    failures[key].append(
                        self._source_calls.source_failure(source, error, attempts=attempt.attempts)
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
                runtime.mark_refreshed(key)
                resolved[key] = ResolutionResult(value=value)
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
                    source_kind=self._source_catalog.kind(source),
                    authority_rank=value.authority_rank,
                )

            if fresh_values:
                write_results = await self._cache_access.set_many(
                    fresh_values, diagnostics=runtime.diagnostics
                )
                if write_results:
                    for written in fresh_values:
                        result = write_results.get(written.key)
                        if (
                            result is not None
                            and result.status is CacheWriteStatus.IGNORED_LOWER_AUTHORITY
                        ):
                            winning = self._cache_access.cached_copy(
                                result.value,
                                stale=False,
                                extra_metadata={"superseded_source": written.source},
                            )
                            runtime.memo[written.key] = winning
                            resolved[written.key] = ResolutionResult(value=winning)

            unresolved = [key for key in unresolved if key not in resolved]
            if not unresolved:
                break

        stale_to_cache: list[SnapshotValue[Any]] = []
        for key in unresolved:
            policy = self.policy_resolver.resolve(key)
            stale = best_stale.get(key)
            if (
                stale is not None
                and policy.allow_stale_on_error
                and not runtime.requires_refresh(key)
            ):
                stale_to_cache.append(stale)
                runtime.memo[key] = stale
                self.metrics.increment("source_fetch_total", status="stale", source=stale.source)
                resolved[key] = ResolutionResult(value=stale)
                continue
            resolved[key] = ResolutionResult(
                error=ResourceResolutionError(key, tuple(failures[key]))
            )

        if stale_to_cache and runtime.cache_stale_results:
            await self._cache_access.set_many(stale_to_cache, diagnostics=runtime.diagnostics)
        return resolved

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("SnapshotBuilder is closed")

    def _record_snapshot_built(
        self,
        snapshot: Snapshot,
        *,
        strict: bool,
        failed: bool | None = None,
    ) -> None:
        build_failed = bool(snapshot.errors) if failed is None else bool(failed)
        self.metrics.increment(
            "snapshot_build_total",
            status="error" if build_failed else "success",
        )
        self.events.emit(
            "snapshot_built",
            snapshot_id=snapshot.snapshot_id,
            resources=len(snapshot.resources) + len(snapshot.errors),
            resolved=len(snapshot.resources),
            failed=len(snapshot.errors),
            build_failed=build_failed,
            strict=strict,
            diagnostics=snapshot.diagnostics,
        )
