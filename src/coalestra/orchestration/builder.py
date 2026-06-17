from __future__ import annotations

import asyncio
import inspect
import math
import uuid
from collections.abc import Awaitable, Collection, Iterable, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, cast

from coalestra.cache.memory import AsyncMemoryCache
from coalestra.cache.publisher import ResourcePublisher
from coalestra.concurrency.capacity import CapacityController, CapacitySnapshot
from coalestra.core.authority import AuthorityPolicyResolver, SourceAuthorityPolicy
from coalestra.core.clock import SystemClock
from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.errors import (
    ObservabilityShutdownTimeoutError,
    PayloadCopyShutdownTimeoutError,
    ResourceResolutionError,
    SnapshotBuildError,
    SourceFailure,
    SourceProtocolError,
)
from coalestra.core.health import (
    BuilderHealth,
    OperationalHealthTracker,
    PayloadCopyHealth,
)
from coalestra.core.isolation import AsyncPayloadIsolator, PayloadCopier, PayloadIsolator
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
from coalestra.core.source_timeout import SourceTimeoutGuaranteeStatus
from coalestra.observability.buffered import (
    BufferedEventSink,
    BufferedMetricsSink,
    BufferedSinkStats,
    BufferOverflowPolicy,
)
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
        run_payload_copies_in_thread: bool | None = None,
        max_copy_concurrency: int = 4,
        cache_run_payload_copies_in_thread: bool | None = None,
        cache_max_copy_concurrency: int = 4,
        copy_shutdown_timeout_seconds: float = 5.0,
        buffer_observability: bool | None = None,
        observability_max_pending: int = 10_000,
        observability_overflow: BufferOverflowPolicy = BufferOverflowPolicy.DROP_OLDEST,
        observability_shutdown_timeout_seconds: float = 5.0,
        observability_drain_on_shutdown: bool = True,
        require_source_timeout_declarations: bool = True,
        allow_unsafe_blocking_sources: bool = False,
        source_transport_timeout_grace_seconds: float = 0.05,
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
        if run_payload_copies_in_thread is not None and not isinstance(
            run_payload_copies_in_thread, bool
        ):
            raise TypeError("run_payload_copies_in_thread must be a boolean or None")
        if isinstance(max_copy_concurrency, bool) or not isinstance(max_copy_concurrency, int):
            raise TypeError("max_copy_concurrency must be an integer")
        if max_copy_concurrency < 1:
            raise ValueError("max_copy_concurrency must be at least 1")
        if cache_run_payload_copies_in_thread is not None and not isinstance(
            cache_run_payload_copies_in_thread, bool
        ):
            raise TypeError("cache_run_payload_copies_in_thread must be a boolean or None")
        if isinstance(cache_max_copy_concurrency, bool) or not isinstance(
            cache_max_copy_concurrency, int
        ):
            raise TypeError("cache_max_copy_concurrency must be an integer")
        if cache_max_copy_concurrency < 1:
            raise ValueError("cache_max_copy_concurrency must be at least 1")
        if isinstance(copy_shutdown_timeout_seconds, bool) or not isinstance(
            copy_shutdown_timeout_seconds, (int, float)
        ):
            raise TypeError("copy_shutdown_timeout_seconds must be a number")
        if copy_shutdown_timeout_seconds <= 0:
            raise ValueError("copy_shutdown_timeout_seconds must be positive")
        if buffer_observability is not None and not isinstance(buffer_observability, bool):
            raise TypeError("buffer_observability must be a boolean or None")
        if isinstance(observability_max_pending, bool) or not isinstance(
            observability_max_pending, int
        ):
            raise TypeError("observability_max_pending must be an integer")
        if observability_max_pending < 1:
            raise ValueError("observability_max_pending must be at least 1")
        if not isinstance(observability_overflow, BufferOverflowPolicy):
            raise TypeError("observability_overflow must be a BufferOverflowPolicy")
        if isinstance(observability_shutdown_timeout_seconds, bool) or not isinstance(
            observability_shutdown_timeout_seconds, (int, float)
        ):
            raise TypeError("observability_shutdown_timeout_seconds must be a number")
        if observability_shutdown_timeout_seconds <= 0:
            raise ValueError("observability_shutdown_timeout_seconds must be positive")
        if not isinstance(observability_drain_on_shutdown, bool):
            raise TypeError("observability_drain_on_shutdown must be a boolean")
        if not isinstance(require_source_timeout_declarations, bool):
            raise TypeError("require_source_timeout_declarations must be a boolean")
        if not isinstance(allow_unsafe_blocking_sources, bool):
            raise TypeError("allow_unsafe_blocking_sources must be a boolean")
        if isinstance(source_transport_timeout_grace_seconds, bool) or not isinstance(
            source_transport_timeout_grace_seconds, (int, float)
        ):
            raise TypeError("source_transport_timeout_grace_seconds must be a number")
        if not math.isfinite(float(source_transport_timeout_grace_seconds)):
            raise ValueError("source_transport_timeout_grace_seconds must be finite")
        if source_transport_timeout_grace_seconds < 0:
            raise ValueError("source_transport_timeout_grace_seconds cannot be negative")
        if cache is not None and (
            cache_run_payload_copies_in_thread is not None or cache_max_copy_concurrency != 4
        ):
            raise ValueError(
                "cache payload copy settings apply only to the default AsyncMemoryCache"
            )

        self.clock = clock or SystemClock()
        default = default_policy or FreshnessPolicy(
            ttl_seconds=1.0,
            max_stale_seconds=10.0,
        )
        self.policy_resolver = policy_resolver or PolicyResolver(default)
        self.authority_resolver = authority_resolver or AuthorityPolicyResolver(authority_policy)
        self._payload_isolator = PayloadIsolator(payload_copier)
        self._async_payload_isolator = AsyncPayloadIsolator(
            self._payload_isolator,
            run_in_thread=run_payload_copies_in_thread,
            max_concurrency=max_copy_concurrency,
            component_name="builder",
        )
        self.run_payload_copies_in_thread = self._async_payload_isolator.run_in_thread
        self.max_copy_concurrency = self._async_payload_isolator.max_concurrency
        self.copy_shutdown_timeout_seconds = float(copy_shutdown_timeout_seconds)
        self._owns_cache = cache is None
        self.cache = cache or AsyncMemoryCache(
            payload_copier=payload_copier,
            run_payload_copies_in_thread=cache_run_payload_copies_in_thread,
            max_copy_concurrency=cache_max_copy_concurrency,
        )
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
        self._metrics_downstream = metrics or NullMetrics()
        self._events_downstream = events or NullEventSink()
        self.buffer_observability = buffer_observability
        self.observability_max_pending = int(observability_max_pending)
        self.observability_overflow = observability_overflow
        self.observability_shutdown_timeout_seconds = float(observability_shutdown_timeout_seconds)
        self.observability_drain_on_shutdown = observability_drain_on_shutdown
        self._owned_observability_buffers: dict[str, BufferedEventSink | BufferedMetricsSink] = {}
        self._observability_downstreams_closed = False
        self.observation_policy = observation_policy or ObservationPolicy()
        self.cache_source_support = bool(cache_source_support)
        self.source_support_cache_max_entries = source_support_cache_max_entries
        self.manage_lifecycle = bool(manage_lifecycle)
        self.require_source_timeout_declarations = require_source_timeout_declarations
        self.allow_unsafe_blocking_sources = allow_unsafe_blocking_sources
        self.source_transport_timeout_grace_seconds = float(source_transport_timeout_grace_seconds)
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
            require_timeout_declarations=self.require_source_timeout_declarations,
            allow_unsafe_blocking_sources=self.allow_unsafe_blocking_sources,
        )
        self.sources = self._source_catalog.sources
        self._source_kinds = self._source_catalog.kinds
        self._source_support_cache = self._source_catalog.support_cache

        if self.authority_resolver.has_rules and not bool(
            getattr(self.cache, "validates_source_authority", False)
        ):
            raise ValueError(
                "authority policies require a cache that declares validates_source_authority = True"
            )

        self.capacity = CapacityController(
            global_limit=self.max_concurrency,
            source_limits=source_concurrency,
        )
        for source in self.sources:
            declared_limit = getattr(source, "max_concurrency", None)
            if self.capacity.limit_for(source.name) is None:
                self.capacity.register_source(source.name, declared_limit)

        self.metrics = self._configure_metrics_sink(self._metrics_downstream)
        self.events = self._configure_event_sink(self._events_downstream)

        self._cache_access = CacheAccess(
            cache=self.cache,
            clock=self.clock,
            payload_isolator=self._payload_isolator,
            async_payload_isolator=self._async_payload_isolator,
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
            async_payload_isolator=self._async_payload_isolator,
            authority_resolver=self.authority_resolver,
            source_timeout_guarantees=self._source_catalog.timeout_guarantees,
            health_tracker=self._health_tracker,
            transport_timeout_grace_seconds=self.source_transport_timeout_grace_seconds,
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
        self._closing = False
        self._closed = False
        self.publisher = ResourcePublisher(
            cache=self.cache,
            clock=self.clock,
            policy_resolver=self.policy_resolver,
            metrics=self.metrics,
            events=self.events,
            observation_policy=self.observation_policy,
            payload_isolator=self._payload_isolator,
            _async_payload_isolator=self._async_payload_isolator,
            authority_resolver=self.authority_resolver,
            max_pending_tasks=self.max_pending_tasks,
            health_tracker=self._health_tracker,
        )

    def _configure_metrics_sink(self, sink: MetricsSink) -> MetricsSink:
        if not self._should_buffer_observability_sink(sink):
            return sink
        buffered = BufferedMetricsSink(
            sink,
            max_pending=self.observability_max_pending,
            overflow=self.observability_overflow,
        )
        self._owned_observability_buffers["metrics"] = buffered
        return buffered

    def _configure_event_sink(self, sink: EventSink) -> EventSink:
        if not self._should_buffer_observability_sink(sink):
            return sink
        buffered = BufferedEventSink(
            sink,
            max_pending=self.observability_max_pending,
            overflow=self.observability_overflow,
        )
        self._owned_observability_buffers["events"] = buffered
        return buffered

    def _should_buffer_observability_sink(self, sink: object) -> bool:
        if isinstance(sink, (BufferedEventSink, BufferedMetricsSink)):
            return False
        if self.buffer_observability is not None:
            return self.buffer_observability
        return not bool(getattr(sink, "coalestra_non_blocking", False))

    def _observability_buffer_stats(self) -> dict[str, BufferedSinkStats]:
        components: dict[str, BufferedSinkStats] = {}
        for name, sink in (("metrics", self.metrics), ("events", self.events)):
            if not bool(getattr(sink, "exposes_observability_buffer_health", False)):
                continue
            stats = getattr(sink, "stats", None)
            if not callable(stats):
                continue
            snapshot = stats()
            if not isinstance(snapshot, BufferedSinkStats):
                raise TypeError("buffered observability stats must return BufferedSinkStats")
            components[name] = snapshot
        return components

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

        copy_components: dict[str, PayloadCopyHealth] = {
            "builder": self._async_payload_isolator.health_snapshot(),
        }
        cache_isolator = getattr(self.cache, "_async_payload_isolator", None)
        copy_health_snapshot = getattr(self.cache, "copy_health_snapshot", None)
        exposes_copy_health = bool(getattr(self.cache, "exposes_payload_copy_health", False))
        if (
            exposes_copy_health
            and callable(copy_health_snapshot)
            and cache_isolator is not self._async_payload_isolator
        ):
            cache_copy_health = copy_health_snapshot()
            if inspect.isawaitable(cache_copy_health):
                cache_copy_health = await cache_copy_health
            if not isinstance(cache_copy_health, PayloadCopyHealth):
                raise TypeError("copy_health_snapshot must return PayloadCopyHealth")
            copy_components["cache"] = cache_copy_health

        observability_buffers = self._observability_buffer_stats()
        timeout_guarantees = self._source_catalog.timeout_guarantees
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
            source_timeout_guarantees=timeout_guarantees,
            blocking_source_count=sum(
                guarantee.blocking_io for guarantee in timeout_guarantees.values()
            ),
            protected_blocking_source_count=sum(
                guarantee.status is SourceTimeoutGuaranteeStatus.PROTECTED
                for guarantee in timeout_guarantees.values()
            ),
            unsafe_blocking_source_count=sum(
                guarantee.blocking_io and not guarantee.protected
                for guarantee in timeout_guarantees.values()
            ),
            undeclared_source_timeout_count=sum(
                not guarantee.declaration_present for guarantee in timeout_guarantees.values()
            ),
            source_transport_timeout_violation_count=(
                operational.source_transport_timeout_violation_count
            ),
            source_transport_timeout_violations=(operational.source_transport_timeout_violations),
            payload_copy_components=copy_components,
            active_payload_copies=sum(
                snapshot.active_copies for snapshot in copy_components.values()
            ),
            waiting_for_copy_capacity=sum(
                snapshot.waiting_for_capacity for snapshot in copy_components.values()
            ),
            payload_copy_started_count=sum(
                snapshot.started_count for snapshot in copy_components.values()
            ),
            payload_copy_completed_count=sum(
                snapshot.completed_count for snapshot in copy_components.values()
            ),
            payload_copy_failure_count=sum(
                snapshot.failure_count for snapshot in copy_components.values()
            ),
            payload_copy_timeout_count=sum(
                snapshot.timeout_count for snapshot in copy_components.values()
            ),
            payload_copy_capacity_timeout_count=sum(
                snapshot.capacity_timeout_count for snapshot in copy_components.values()
            ),
            payload_copy_shutdown_incomplete=any(
                snapshot.shutdown_incomplete for snapshot in copy_components.values()
            ),
            payload_copy_shutdown_timeout_count=sum(
                snapshot.shutdown_timeout_count for snapshot in copy_components.values()
            ),
            payload_copy_active_at_last_shutdown_timeout=sum(
                snapshot.active_at_last_shutdown_timeout for snapshot in copy_components.values()
            ),
            observability_buffers=observability_buffers,
            observability_pending=sum(
                snapshot.pending for snapshot in observability_buffers.values()
            ),
            observability_peak_pending=sum(
                snapshot.peak_pending for snapshot in observability_buffers.values()
            ),
            observability_dropped_count=sum(
                snapshot.dropped for snapshot in observability_buffers.values()
            ),
            observability_failure_count=sum(
                snapshot.failures for snapshot in observability_buffers.values()
            ),
            observability_shutdown_incomplete=any(
                snapshot.closed and snapshot.worker_alive
                for snapshot in observability_buffers.values()
            ),
            observability_shutdown_timeout_count=sum(
                snapshot.shutdown_timeout_count for snapshot in observability_buffers.values()
            ),
        )

    async def wait_for_refreshes(self) -> None:
        """Wait until every currently scheduled background refresh finishes."""

        await self._refresh_manager.wait()

    async def aclose(
        self,
        *,
        cancel_refreshes: bool = False,
        copy_shutdown_timeout_seconds: float | None = None,
        observability_shutdown_timeout_seconds: float | None = None,
    ) -> None:
        """Close the builder and drain owned background subsystems within their budgets."""

        if copy_shutdown_timeout_seconds is not None:
            if isinstance(copy_shutdown_timeout_seconds, bool) or not isinstance(
                copy_shutdown_timeout_seconds, (int, float)
            ):
                raise TypeError("copy_shutdown_timeout_seconds must be a number or None")
            if copy_shutdown_timeout_seconds <= 0:
                raise ValueError("copy_shutdown_timeout_seconds must be positive or None")
            resolved_copy_timeout = float(copy_shutdown_timeout_seconds)
        else:
            resolved_copy_timeout = self.copy_shutdown_timeout_seconds

        if observability_shutdown_timeout_seconds is not None:
            if isinstance(observability_shutdown_timeout_seconds, bool) or not isinstance(
                observability_shutdown_timeout_seconds, (int, float)
            ):
                raise TypeError("observability_shutdown_timeout_seconds must be a number or None")
            if observability_shutdown_timeout_seconds <= 0:
                raise ValueError("observability_shutdown_timeout_seconds must be positive or None")
            resolved_observability_timeout = float(observability_shutdown_timeout_seconds)
        else:
            resolved_observability_timeout = self.observability_shutdown_timeout_seconds

        if self._closed:
            return
        self._closing = True
        self._closed = True

        refresh_error: BaseException | None = None
        copy_error: BaseException | None = None
        lifecycle_error: BaseException | None = None
        observability_error: BaseException | None = None
        downstream_error: BaseException | None = None
        try:
            try:
                await self._refresh_manager.close(cancel=cancel_refreshes)
            except BaseException as error:
                refresh_error = error

            try:
                await self._close_payload_copy_components(
                    timeout_seconds=resolved_copy_timeout,
                )
            except BaseException as error:
                copy_error = error

            if self.manage_lifecycle:
                cache_lifecycle_handled = bool(
                    getattr(
                        self.cache,
                        "payload_copy_lifecycle_is_complete_close",
                        False,
                    )
                ) and (self._owns_cache or self.manage_lifecycle)
                components: tuple[object, ...]
                if cache_lifecycle_handled:
                    components = tuple(self.sources)
                else:
                    components = (*self.sources, self.cache)
                try:
                    await close_components(components)
                except BaseException as error:
                    lifecycle_error = error

            try:
                await self._close_owned_observability_buffers(
                    timeout_seconds=resolved_observability_timeout,
                    drain=self.observability_drain_on_shutdown,
                )
            except BaseException as error:
                observability_error = error

            if self.manage_lifecycle and observability_error is None:
                try:
                    await self._close_observability_downstreams()
                except BaseException as error:
                    downstream_error = error
        finally:
            self._closing = False

        if copy_error is not None:
            raise copy_error
        if refresh_error is not None:
            raise refresh_error
        if observability_error is not None:
            raise observability_error
        if lifecycle_error is not None:
            raise lifecycle_error
        if downstream_error is not None:
            raise downstream_error

    async def wait_for_payload_copy_shutdown(self) -> None:
        """Wait without a deadline for copy workers after a timed-out close attempt."""

        await self._close_payload_copy_components(timeout_seconds=None)

    async def wait_for_observability_shutdown(self) -> None:
        """Wait without a deadline for builder-owned observability buffers to stop."""

        await self._close_owned_observability_buffers(
            timeout_seconds=None,
            drain=self.observability_drain_on_shutdown,
        )
        if self.manage_lifecycle:
            await self._close_observability_downstreams()

    async def wait_for_background_shutdown(self) -> None:
        """Wait for late copy and observability workers after a timed-out close."""

        results = await asyncio.gather(
            self.wait_for_payload_copy_shutdown(),
            self.wait_for_observability_shutdown(),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def _close_owned_observability_buffers(
        self,
        *,
        timeout_seconds: float | None,
        drain: bool,
    ) -> None:
        if not self._owned_observability_buffers:
            return

        async def close_one(
            sink: BufferedEventSink | BufferedMetricsSink,
        ) -> bool:
            return await asyncio.to_thread(
                sink.close,
                timeout=timeout_seconds,
                drain=drain,
            )

        items = tuple(self._owned_observability_buffers.items())
        results = await asyncio.gather(
            *(close_one(sink) for _name, sink in items),
            return_exceptions=True,
        )
        incomplete: dict[str, int] = {}
        unexpected: BaseException | None = None
        for (name, sink), result in zip(items, results, strict=True):
            if isinstance(result, BaseException):
                if unexpected is None:
                    unexpected = result
                continue
            if not result:
                stats = sink.stats()
                incomplete[name] = stats.pending

        if incomplete:
            assert timeout_seconds is not None
            raise ObservabilityShutdownTimeoutError(
                timeout_seconds=timeout_seconds,
                pending_components=incomplete,
            )
        if unexpected is not None:
            raise unexpected

    async def _close_observability_downstreams(self) -> None:
        if self._observability_downstreams_closed:
            return
        await close_components((self._events_downstream, self._metrics_downstream))
        self._observability_downstreams_closed = True

    async def _close_payload_copy_components(
        self,
        *,
        timeout_seconds: float | None,
    ) -> None:
        closers: list[tuple[str, Awaitable[None]]] = [
            (
                "builder",
                self._async_payload_isolator.aclose(timeout_seconds=timeout_seconds),
            )
        ]
        manages_cache_copy_lifecycle = self._owns_cache or (
            self.manage_lifecycle
            and bool(getattr(self.cache, "exposes_payload_copy_lifecycle", False))
        )
        if manages_cache_copy_lifecycle:
            cache_close = getattr(self.cache, "aclose_payload_copies", None)
            if callable(cache_close):
                closers.append(("cache", cache_close(timeout_seconds=timeout_seconds)))

        results = await asyncio.gather(
            *(closer for _name, closer in closers),
            return_exceptions=True,
        )
        active_components: dict[str, int] = {}
        unexpected: BaseException | None = None
        for (name, _closer), result in zip(closers, results, strict=True):
            if isinstance(result, PayloadCopyShutdownTimeoutError):
                active_components[name] = result.active_copies
            elif isinstance(result, BaseException) and unexpected is None:
                unexpected = result

        if active_components:
            assert timeout_seconds is not None
            raise PayloadCopyShutdownTimeoutError(
                timeout_seconds=timeout_seconds,
                active_components=active_components,
            )
        if unexpected is not None:
            raise unexpected

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
                snapshot = await session.resolve(keys, strict=strict)
            except SnapshotBuildError as error:
                if error.snapshot is None:
                    self._record_snapshot_failed_without_delivery(
                        snapshot_id=session.snapshot_id,
                        failed_resources=len(error.errors),
                        strict=strict,
                    )
                else:
                    self._record_snapshot_built(
                        error.snapshot,
                        strict=strict,
                        failed=True,
                    )
                raise
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
            if error.snapshot is None:
                self._record_snapshot_failed_without_delivery(
                    snapshot_id=session.snapshot_id,
                    failed_resources=len(error.errors),
                    strict=True,
                )
            else:
                self._record_snapshot_built(error.snapshot, strict=True, failed=True)
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
            context=context,
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
                cached = await self._cache_access.cached_copy(
                    lookup.value,
                    stale=False,
                    extra_metadata={"refresh_scheduled": refresh_scheduled}
                    if refresh_scheduled
                    else None,
                    context=context,
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
                cached = await self._cache_access.cached_copy(
                    lookup.value,
                    stale=True,
                    extra_metadata={
                        "refresh_mode": RefreshMode.STALE_WHILE_REVALIDATE.value,
                        "refresh_scheduled": refresh_scheduled,
                    },
                    context=context,
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
                value = await self._cache_access.cached_copy(
                    stale_candidate,
                    stale=True,
                    extra_metadata={
                        "fallback_error_type": type(error).__name__,
                        "fallback_error": str(error),
                    },
                    context=context,
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
                if runtime.source_is_excluded(key, source.name):
                    self.metrics.increment(
                        "source_fetch_total",
                        status="consistency_excluded",
                        source=source.name,
                    )
                    self.events.emit(
                        "source_excluded",
                        resource=str(key),
                        source=source.name,
                        reason="consistency_fallback",
                    )
                    continue
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

            if fresh_values and runtime.cache_source_results:
                write_results = await self._cache_access.set_many(
                    fresh_values,
                    diagnostics=runtime.diagnostics,
                    context=context,
                )
                if write_results:
                    for written in fresh_values:
                        result = write_results.get(written.key)
                        if (
                            result is not None
                            and result.status is CacheWriteStatus.IGNORED_LOWER_AUTHORITY
                        ):
                            winning = await self._cache_access.cached_copy(
                                result.value,
                                stale=False,
                                extra_metadata={"superseded_source": written.source},
                                context=context,
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
            await self._cache_access.set_many(
                stale_to_cache,
                diagnostics=runtime.diagnostics,
                context=context,
            )
        return resolved

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("SnapshotBuilder is closed")

    def _record_snapshot_failed_without_delivery(
        self,
        *,
        snapshot_id: str,
        failed_resources: int,
        strict: bool,
    ) -> None:
        self.metrics.increment("snapshot_build_total", status="error")
        self.events.emit(
            "snapshot_built",
            snapshot_id=snapshot_id,
            resources=failed_resources,
            resolved=0,
            failed=failed_resources,
            build_failed=True,
            strict=strict,
            diagnostics=None,
        )

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
