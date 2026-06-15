from __future__ import annotations

import asyncio
import uuid
from collections.abc import Mapping
from typing import Protocol

from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.errors import SourceUnavailableError
from coalestra.core.models import FetchContext, ResourceKey
from coalestra.core.protocols import Clock, EventSink, MetricsSink
from coalestra.orchestration.runtime import ResolutionResult, ResolutionRuntime
from coalestra.orchestration.singleflight import SingleFlight


class ResolveOwned(Protocol):
    async def __call__(
        self,
        keys: tuple[ResourceKey, ...],
        *,
        context: FetchContext,
        runtime: ResolutionRuntime,
        ancestry: tuple[ResourceKey, ...],
        local_owned: frozenset[ResourceKey],
    ) -> Mapping[ResourceKey, ResolutionResult]: ...


class RefreshManager:
    """Schedule, deduplicate, observe, and settle background refresh work."""

    def __init__(
        self,
        *,
        clock: Clock,
        single_flight: SingleFlight[ResourceKey, ResolutionResult],
        resolve_owned: ResolveOwned,
        metrics: MetricsSink,
        events: EventSink,
    ) -> None:
        self.clock = clock
        self.single_flight = single_flight
        self.resolve_owned = resolve_owned
        self.metrics = metrics
        self.events = events
        self.tasks: dict[ResourceKey, asyncio.Task[None]] = {}
        self._closed = False

    @property
    def count(self) -> int:
        return len(self.tasks)

    async def wait(self) -> None:
        """Wait until every currently scheduled refresh finishes."""

        while self.tasks:
            tasks = tuple(self.tasks.values())
            await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self, *, cancel: bool = False) -> None:
        """Prevent new refreshes and settle every scheduled task."""

        self._closed = True
        tasks = tuple(self.tasks.values())
        if cancel:
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def schedule(
        self,
        key: ResourceKey,
        *,
        parent_context: FetchContext,
        diagnostics: DiagnosticsCollector,
        reason: str,
    ) -> bool:
        if self._closed:
            return False
        existing = self.tasks.get(key)
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
        self.tasks[key] = task

        def cleanup(completed: asyncio.Task[None], resource: ResourceKey = key) -> None:
            self._finish(resource, completed)

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
        runtime = ResolutionRuntime(
            diagnostics=diagnostics,
            cache_stale_results=False,
        )
        try:
            flight_results = await self.single_flight.run_many(
                (key,),
                lambda owned: self.resolve_owned(
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

    def _finish(self, key: ResourceKey, task: asyncio.Task[None]) -> None:
        if self.tasks.get(key) is task:
            self.tasks.pop(key, None)
        if not task.cancelled():
            task.exception()
