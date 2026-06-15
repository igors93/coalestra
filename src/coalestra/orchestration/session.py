from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from coalestra.core.errors import SessionClosedError, SnapshotBuildError
from coalestra.core.models import FetchContext, ResourceKey, Snapshot, SnapshotValue
from coalestra.core.request import SnapshotRequest
from coalestra.orchestration.runtime import ResolutionRuntime

if TYPE_CHECKING:
    from coalestra.orchestration.builder import SnapshotBuilder


class SnapshotSession:
    """Incrementally builds one logically consistent snapshot.

    A session keeps one snapshot identity, creation time, deadline, and acquisition memo across
    every ``resolve`` call. Capacity is owned by the long-lived builder and shared by all sessions.
    Values resolved in an earlier stage are pinned for the remainder of the session unless they are
    explicitly replaced through transactional selective revalidation.
    """

    def __init__(
        self,
        *,
        builder: SnapshotBuilder,
        context: FetchContext,
        runtime: ResolutionRuntime,
    ) -> None:
        self._builder = builder
        self._context = context
        self._runtime = runtime
        self._resources: dict[ResourceKey, SnapshotValue[object]] = {}
        self._errors: dict[ResourceKey, Exception] = {}
        self._closed = False
        self._lock = asyncio.Lock()

    @property
    def snapshot_id(self) -> str:
        return self._context.snapshot_id

    @property
    def created_at(self) -> float:
        return self._context.requested_at

    @property
    def context(self) -> FetchContext:
        return self._context

    @property
    def closed(self) -> bool:
        return self._closed

    async def resolve(
        self,
        keys: Iterable[ResourceKey],
        *,
        strict: bool = True,
        retry_errors: bool = False,
    ) -> Snapshot:
        """Resolve another stage and return the complete session snapshot.

        Existing successful values are never fetched again. Existing errors are retained unless
        ``retry_errors=True`` is supplied, in which case the requested failed keys are attempted
        again while preserving the same session identity and deadline.
        """

        async with self._lock:
            self._ensure_open()
            self._builder._ensure_open()
            requested = self._normalize_keys(keys)
            self._runtime.diagnostics.record_requested(requested)

            if retry_errors:
                for key in requested:
                    self._errors.pop(key, None)

            pending = [
                key for key in requested if key not in self._resources and key not in self._errors
            ]
            if pending:
                values, errors = await self._builder._resolve_many(
                    pending,
                    context=self._context,
                    runtime=self._runtime,
                )
                self._resources.update(values)
                self._errors.update(errors)

            requested_errors = {key: self._errors[key] for key in requested if key in self._errors}
            self._builder.metrics.increment(
                "snapshot_session_resolve_total",
                status="error" if requested_errors else "success",
            )
            self._builder.events.emit(
                "snapshot_session_resolved",
                snapshot_id=self.snapshot_id,
                requested=len(requested),
                resolved=sum(1 for key in requested if key in self._resources),
                failed=len(requested_errors),
                total_resources=len(self._resources),
                strict=strict,
                retry_errors=retry_errors,
            )

            snapshot = self.snapshot()
            if requested_errors and strict:
                raise SnapshotBuildError(requested_errors, snapshot=snapshot)
            return snapshot

    async def resolve_request(
        self,
        request: SnapshotRequest,
        *,
        retry_errors: bool = False,
    ) -> Snapshot:
        """Resolve a required/optional resource request in this session."""

        snapshot = await self.resolve(
            request.keys,
            strict=False,
            retry_errors=retry_errors,
        )
        required_errors = {
            key: snapshot.errors[key] for key in request.required if key in snapshot.errors
        }
        if required_errors:
            raise SnapshotBuildError(required_errors, snapshot=snapshot)
        return snapshot

    async def revalidate(
        self,
        keys: Iterable[ResourceKey],
        *,
        strict: bool = True,
        force_refresh: bool = False,
    ) -> Snapshot:
        """Refresh selected pinned resources without replacing unrelated session values.

        The operation always bypasses the session memo so it can observe newer published or cached
        revisions. ``force_refresh=True`` additionally bypasses the shared cache and requires fresh
        source resolution. Derived values already pinned in the session are refreshed transitively
        when they depend on a selected key. The session update is transactional: either every
        affected visible resource is committed together, or the previous pinned state is retained.

        When ``strict=False`` and revalidation fails, the returned snapshot contains the retained
        previous values plus transient errors for the failed refresh attempt. Those transient errors
        are not persisted in the session.
        """

        async with self._lock:
            self._ensure_open()
            self._builder._ensure_open()
            requested = self._normalize_keys(keys)
            unresolved = tuple(key for key in requested if key not in self._runtime.memo)
            if unresolved:
                rendered = ", ".join(str(key) for key in unresolved)
                raise ValueError(
                    f"revalidation requires resources already resolved in this session: {rendered}"
                )

            affected = self._affected_keys(requested)
            visible_dependents = tuple(
                key for key in self._resources if key in affected and key not in requested
            )
            targets = tuple(dict.fromkeys((*requested, *visible_dependents)))
            self._runtime.diagnostics.record_requested(requested)

            staging_runtime = ResolutionRuntime(
                diagnostics=self._runtime.diagnostics,
                memo={
                    key: value for key, value in self._runtime.memo.items() if key not in affected
                },
                cache_stale_results=not force_refresh,
                force_refresh_keys=set(affected) if force_refresh else set(),
            )
            values, errors = await self._builder._resolve_many(
                targets,
                context=self._context,
                runtime=staging_runtime,
            )

            if errors:
                self._record_revalidation(
                    requested=requested,
                    affected=affected,
                    errors=errors,
                    committed=False,
                    refreshed=0,
                    strict=strict,
                    force_refresh=force_refresh,
                )
                snapshot = self._snapshot_with_errors(errors)
                if strict:
                    raise SnapshotBuildError(errors, snapshot=snapshot)
                return snapshot

            self._runtime.memo.clear()
            self._runtime.memo.update(staging_runtime.memo)
            for key in targets:
                if key in self._resources:
                    self._resources[key] = values[key]
                self._errors.pop(key, None)

            self._record_revalidation(
                requested=requested,
                affected=affected,
                errors={},
                committed=True,
                refreshed=len(values),
                strict=strict,
                force_refresh=force_refresh,
            )
            return self.snapshot()

    def snapshot(self) -> Snapshot:
        """Return an immutable view of everything explicitly requested in this session."""

        return self._snapshot_with_errors({})

    async def close(self) -> None:
        async with self._lock:
            self._closed = True

    async def __aenter__(self) -> SnapshotSession:
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> None:
        await self.close()

    def _affected_keys(
        self,
        requested: tuple[ResourceKey, ...],
    ) -> frozenset[ResourceKey]:
        affected = set(requested)
        changed = True
        while changed:
            changed = False
            for key, value in self._runtime.memo.items():
                if key in affected:
                    continue
                if any(dependency in affected for dependency in value.dependency_versions):
                    affected.add(key)
                    changed = True
        return frozenset(affected)

    def _snapshot_with_errors(
        self,
        transient_errors: Mapping[ResourceKey, Exception],
    ) -> Snapshot:
        errors = {**self._errors, **transient_errors}
        diagnostics = self._runtime.diagnostics.snapshot(
            now_monotonic=self._builder.clock.monotonic(),
            resolved_resources=len(self._resources),
            failed_resources=len(errors),
            observed_at_values=tuple(value.observed_at for value in self._resources.values()),
        )
        resources = {
            key: self._builder._payload_isolator.clone_snapshot_value(
                value,
                context=f"snapshot delivery for {key}",
            )
            for key, value in self._resources.items()
        }
        return Snapshot(
            snapshot_id=self.snapshot_id,
            created_at=self.created_at,
            resources=resources,
            errors=errors,
            diagnostics=diagnostics,
        )

    def _record_revalidation(
        self,
        *,
        requested: tuple[ResourceKey, ...],
        affected: frozenset[ResourceKey],
        errors: Mapping[ResourceKey, Exception],
        committed: bool,
        refreshed: int,
        strict: bool,
        force_refresh: bool,
    ) -> None:
        status = "success" if committed else "error"
        self._builder.metrics.increment(
            "snapshot_session_revalidate_total",
            status=status,
        )
        self._builder.events.emit(
            "snapshot_session_revalidated",
            snapshot_id=self.snapshot_id,
            requested=len(requested),
            affected=len(affected),
            refreshed=refreshed,
            failed=len(errors),
            committed=committed,
            retained_previous=not committed,
            strict=strict,
            force_refresh=force_refresh,
        )

    @staticmethod
    def _normalize_keys(keys: Iterable[ResourceKey]) -> tuple[ResourceKey, ...]:
        requested = tuple(dict.fromkeys(keys))
        if not requested:
            raise ValueError("at least one resource key is required")
        return requested

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionClosedError("SnapshotSession is closed")
