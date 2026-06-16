from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from coalestra.core.acceptance import (
    SnapshotAcceptancePolicy,
    SnapshotAcceptanceViolation,
    find_snapshot_acceptance_violations,
)
from coalestra.core.consistency import (
    ObservationSkewViolation,
    SnapshotConsistencyPolicy,
    find_observation_skew_violation,
)
from coalestra.core.errors import (
    ResourceResolutionError,
    SessionClosedError,
    SnapshotAcceptanceError,
    SnapshotBuildError,
    SnapshotConsistencyError,
    SnapshotDeadlineExceededError,
)
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

            candidate_resources = dict(self._resources)
            candidate_errors = dict(self._errors)
            if retry_errors:
                for key in requested:
                    candidate_errors.pop(key, None)

            pending = [
                key
                for key in requested
                if key not in candidate_resources and key not in candidate_errors
            ]
            try:
                if pending:
                    values, errors = await self._builder._resolve_many(
                        pending,
                        context=self._context,
                        runtime=self._runtime,
                    )
                    candidate_resources.update(values)
                    candidate_errors.update(errors)

                requested_errors = {
                    key: candidate_errors[key] for key in requested if key in candidate_errors
                }
                snapshot = await self._snapshot_state_async(
                    candidate_resources,
                    candidate_errors,
                    enforce_deadline=not self._errors_include_deadline(requested_errors),
                )
            except SnapshotDeadlineExceededError as error:
                self._record_copy_deadline(phase="session_resolve")
                deadline_errors = dict.fromkeys(requested, error)
                self._record_resolve(
                    requested=requested,
                    requested_errors=deadline_errors,
                    total_resources=len(candidate_resources),
                    strict=strict,
                    retry_errors=retry_errors,
                )
                if strict:
                    raise SnapshotBuildError(deadline_errors) from error
                self._resources = candidate_resources
                self._errors = {**candidate_errors, **dict.fromkeys(pending, error)}
                return await self._snapshot_state_async(
                    self._resources,
                    self._errors,
                    enforce_deadline=False,
                )

            self._resources = candidate_resources
            self._errors = candidate_errors
            self._record_resolve(
                requested=requested,
                requested_errors=requested_errors,
                total_resources=len(candidate_resources),
                strict=strict,
                retry_errors=retry_errors,
            )
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

        consistency_policy = request.consistency_policy
        if consistency_policy is not None:
            consistency_keys = (
                request.keys if consistency_policy.include_optional_resources else request.required
            )
            violation = find_observation_skew_violation(
                snapshot.resources,
                consistency_keys,
                consistency_policy,
            )
            if violation is not None:
                raise self._consistency_error(violation, snapshot=snapshot)

        acceptance_policy = request.acceptance_policy
        if acceptance_policy is not None:
            violations = self._acceptance_violations(
                snapshot.resources,
                required=request.required,
                optional=request.optional,
                policy=acceptance_policy,
            )
            if violations:
                raise self._acceptance_error(violations, snapshot=snapshot)
            self._record_acceptance_success()
        return snapshot

    async def revalidate(
        self,
        keys: Iterable[ResourceKey],
        *,
        strict: bool = True,
        force_refresh: bool = False,
        consistency_policy: SnapshotConsistencyPolicy | None = None,
        acceptance_policy: SnapshotAcceptancePolicy | None = None,
    ) -> Snapshot:
        """Refresh selected pinned resources without replacing unrelated session values.

        The operation always bypasses the session memo so it can observe newer published or cached
        revisions. ``force_refresh=True`` additionally bypasses the shared cache and requires fresh
        source resolution. Derived values already pinned in the session are refreshed transitively
        when they depend on a selected key. The session update is transactional: either every
        affected visible resource and its detached delivery snapshot are committed together, or the
        previous pinned state is retained.

        When ``strict=False`` and resource resolution fails, the returned snapshot contains the
        retained previous values plus transient errors for the failed refresh attempt. Those
        transient errors are not persisted in the session. Declared consistency and acceptance
        policies are invariants: a policy violation always raises its dedicated error and leaves
        the previous session state unchanged.
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
            try:
                values, errors = await self._builder._resolve_many(
                    targets,
                    context=self._context,
                    runtime=staging_runtime,
                )
            except SnapshotDeadlineExceededError as error:
                self._record_copy_deadline(phase="session_revalidate")
                self._record_revalidation(
                    requested=requested,
                    affected=affected,
                    errors=dict.fromkeys(requested, error),
                    committed=False,
                    refreshed=0,
                    strict=strict,
                    force_refresh=force_refresh,
                )
                raise SnapshotBuildError(dict.fromkeys(requested, error)) from error

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
                try:
                    snapshot = await self._snapshot_state_async(
                        self._resources,
                        {**self._errors, **errors},
                        enforce_deadline=not self._errors_include_deadline(errors),
                    )
                except SnapshotDeadlineExceededError as error:
                    self._record_copy_deadline(phase="session_revalidate_delivery")
                    raise SnapshotBuildError(dict.fromkeys(requested, error)) from error
                if strict:
                    raise SnapshotBuildError(errors, snapshot=snapshot)
                return snapshot

            if consistency_policy is not None:
                violation = find_observation_skew_violation(
                    values,
                    requested,
                    consistency_policy,
                )
                if violation is not None:
                    self._record_revalidation(
                        requested=requested,
                        affected=affected,
                        errors={},
                        committed=False,
                        refreshed=0,
                        strict=strict,
                        force_refresh=force_refresh,
                        consistency_failed=True,
                    )
                    try:
                        snapshot = await self._snapshot_state_async(
                            self._resources,
                            self._errors,
                        )
                    except SnapshotDeadlineExceededError as error:
                        self._record_copy_deadline(phase="session_revalidate_delivery")
                        raise SnapshotBuildError(dict.fromkeys(requested, error)) from error
                    raise self._consistency_error(violation, snapshot=snapshot)

            candidate_resources = dict(self._resources)
            candidate_errors = dict(self._errors)
            for key in targets:
                if key in candidate_resources:
                    candidate_resources[key] = values[key]
                candidate_errors.pop(key, None)

            if acceptance_policy is not None:
                violations = self._acceptance_violations(
                    candidate_resources,
                    required=requested,
                    optional=(),
                    policy=acceptance_policy,
                )
                if violations:
                    self._record_revalidation(
                        requested=requested,
                        affected=affected,
                        errors={},
                        committed=False,
                        refreshed=0,
                        strict=strict,
                        force_refresh=force_refresh,
                        acceptance_failed=True,
                    )
                    try:
                        retained_snapshot = await self._snapshot_state_async(
                            self._resources,
                            self._errors,
                        )
                    except SnapshotDeadlineExceededError as error:
                        self._record_copy_deadline(phase="session_revalidate_delivery")
                        raise SnapshotBuildError(dict.fromkeys(requested, error)) from error
                    raise self._acceptance_error(violations, snapshot=retained_snapshot)

            try:
                snapshot = await self._snapshot_state_async(
                    candidate_resources,
                    candidate_errors,
                )
            except SnapshotDeadlineExceededError as error:
                self._record_copy_deadline(phase="session_revalidate_delivery")
                self._record_revalidation(
                    requested=requested,
                    affected=affected,
                    errors=dict.fromkeys(requested, error),
                    committed=False,
                    refreshed=0,
                    strict=strict,
                    force_refresh=force_refresh,
                )
                raise SnapshotBuildError(dict.fromkeys(requested, error)) from error

            self._runtime.memo.clear()
            self._runtime.memo.update(staging_runtime.memo)
            self._resources = candidate_resources
            self._errors = candidate_errors
            if acceptance_policy is not None:
                self._record_acceptance_success()
            self._record_revalidation(
                requested=requested,
                affected=affected,
                errors={},
                committed=True,
                refreshed=len(values),
                strict=strict,
                force_refresh=force_refresh,
            )
            return snapshot

    def snapshot(self) -> Snapshot:
        """Return an immutable view using synchronous payload isolation.

        Async consumers should prefer :meth:`snapshot_async` so large default copies can run
        outside the event loop. This synchronous method remains available for compatibility.
        """

        return self._snapshot_with_errors({})

    async def snapshot_async(self) -> Snapshot:
        """Return a detached view within the session's remaining deadline."""

        async with self._lock:
            try:
                return await self._snapshot_state_async(self._resources, self._errors)
            except SnapshotDeadlineExceededError:
                self._record_copy_deadline(phase="session_snapshot_delivery")
                raise

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

    async def _snapshot_with_errors_async(
        self,
        transient_errors: Mapping[ResourceKey, Exception],
    ) -> Snapshot:
        return await self._snapshot_state_async(
            self._resources,
            {**self._errors, **transient_errors},
        )

    async def _snapshot_state_async(
        self,
        resources: Mapping[ResourceKey, SnapshotValue[object]],
        errors: Mapping[ResourceKey, Exception],
        *,
        enforce_deadline: bool = True,
    ) -> Snapshot:
        diagnostics = self._runtime.diagnostics.snapshot(
            now_monotonic=self._builder.clock.monotonic(),
            resolved_resources=len(resources),
            failed_resources=len(errors),
            observed_at_values=tuple(value.observed_at for value in resources.values()),
        )
        copied = await self._builder._async_payload_isolator.map(
            tuple(resources.items()),
            lambda item: (
                item[0],
                self._builder._payload_isolator.clone_snapshot_value(
                    item[1],
                    context=f"snapshot delivery for {item[0]}",
                ),
            ),
            deadline_monotonic=(self._context.deadline_monotonic if enforce_deadline else None),
            monotonic=self._builder.clock.monotonic,
            deadline_context="delivering the snapshot",
        )
        return Snapshot(
            snapshot_id=self.snapshot_id,
            created_at=self.created_at,
            resources=dict(copied),
            errors=errors,
            diagnostics=diagnostics,
        )

    @staticmethod
    def _errors_include_deadline(
        errors: Mapping[ResourceKey, Exception],
    ) -> bool:
        for error in errors.values():
            if isinstance(error, SnapshotDeadlineExceededError):
                return True
            if isinstance(error, ResourceResolutionError) and any(
                failure.error_type == SnapshotDeadlineExceededError.__name__
                for failure in error.failures
            ):
                return True
        return False

    def _record_resolve(
        self,
        *,
        requested: tuple[ResourceKey, ...],
        requested_errors: Mapping[ResourceKey, Exception],
        total_resources: int,
        strict: bool,
        retry_errors: bool,
    ) -> None:
        self._builder.metrics.increment(
            "snapshot_session_resolve_total",
            status="error" if requested_errors else "success",
        )
        self._builder.events.emit(
            "snapshot_session_resolved",
            snapshot_id=self.snapshot_id,
            requested=len(requested),
            resolved=sum(1 for key in requested if key not in requested_errors),
            failed=len(requested_errors),
            total_resources=total_resources,
            strict=strict,
            retry_errors=retry_errors,
        )

    def _record_copy_deadline(self, *, phase: str) -> None:
        self._builder._health_tracker.record_deadline_exceeded()
        self._builder.metrics.increment(
            "payload_copy_deadline_total",
            phase=phase,
        )
        self._builder.events.emit(
            "payload_copy_deadline_exceeded",
            snapshot_id=self.snapshot_id,
            phase=phase,
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
        consistency_failed: bool = False,
        acceptance_failed: bool = False,
    ) -> None:
        status = "success" if committed else "error"
        self._builder._health_tracker.record_revalidation(failed=not committed)
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
            consistency_failed=consistency_failed,
            acceptance_failed=acceptance_failed,
        )

    def _acceptance_violations(
        self,
        resources: Mapping[ResourceKey, SnapshotValue[object]],
        *,
        required: Iterable[ResourceKey],
        optional: Iterable[ResourceKey],
        policy: SnapshotAcceptancePolicy,
    ) -> tuple[SnapshotAcceptanceViolation, ...]:
        return find_snapshot_acceptance_violations(
            resources,
            required_keys=required,
            optional_keys=optional,
            policy=policy,
            now=self._builder.clock.now(),
            freshness_policy_for=self._builder.policy_resolver.resolve,
        )

    def _record_acceptance_success(self) -> None:
        self._builder.metrics.increment(
            "snapshot_acceptance_total",
            status="success",
        )

    def _acceptance_error(
        self,
        violations: tuple[SnapshotAcceptanceViolation, ...],
        *,
        snapshot: Snapshot,
    ) -> SnapshotAcceptanceError:
        reasons = tuple(sorted({violation.reason.value for violation in violations}))
        self._builder.metrics.increment(
            "snapshot_acceptance_total",
            status="error",
        )
        for reason in reasons:
            self._builder.metrics.increment(
                "snapshot_acceptance_violation_total",
                reason=reason,
            )
        self._builder.events.emit(
            "snapshot_acceptance_failed",
            snapshot_id=self.snapshot_id,
            violations=len(violations),
            reasons=",".join(reasons),
        )
        return SnapshotAcceptanceError(violations=violations, snapshot=snapshot)

    def _consistency_error(
        self,
        violation: ObservationSkewViolation,
        *,
        snapshot: Snapshot,
    ) -> SnapshotConsistencyError:
        error = SnapshotConsistencyError(
            keys=violation.keys,
            oldest_key=violation.oldest_key,
            oldest_observed_at=violation.oldest_observed_at,
            newest_key=violation.newest_key,
            newest_observed_at=violation.newest_observed_at,
            observation_skew_seconds=violation.observation_skew_seconds,
            max_observation_skew_seconds=violation.max_observation_skew_seconds,
            snapshot=snapshot,
        )
        self._builder.metrics.increment(
            "snapshot_consistency_total",
            status="error",
            rule="observation_skew",
        )
        self._builder.events.emit(
            "snapshot_consistency_failed",
            snapshot_id=self.snapshot_id,
            resources=len(violation.keys),
            oldest_resource=str(violation.oldest_key),
            newest_resource=str(violation.newest_key),
            observation_skew_ms=violation.observation_skew_seconds * 1000.0,
            max_observation_skew_ms=violation.max_observation_skew_seconds * 1000.0,
        )
        return error

    @staticmethod
    def _normalize_keys(keys: Iterable[ResourceKey]) -> tuple[ResourceKey, ...]:
        requested = tuple(dict.fromkeys(keys))
        if not requested:
            raise ValueError("at least one resource key is required")
        return requested

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionClosedError("SnapshotSession is closed")
