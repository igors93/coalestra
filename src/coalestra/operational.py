from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, cast

from coalestra.core.acceptance import (
    SnapshotAcceptanceViolation,
    find_snapshot_acceptance_violations,
)
from coalestra.core.consistency import ObservationSkewViolation, find_observation_skew_violation
from coalestra.core.errors import (
    CoalestraError,
    SnapshotAcceptanceError,
    SnapshotBuildError,
    SnapshotConsistencyError,
)
from coalestra.core.models import ResourceKey, Snapshot
from coalestra.core.request import SnapshotRequest


class SnapshotResultState(str, Enum):
    """High-level outcome for a non-throwing operational request."""

    ACCEPTED = "accepted"
    DEGRADED = "degraded"
    REJECTED = "rejected"


@dataclass(frozen=True)
class RequestDegradationPolicy:
    """Controls how ``try_*_request`` classifies partial snapshots.

    The default is intentionally useful for high-fanout, read/evaluation workloads: a request that
    returns at least one resource is delivered as ``DEGRADED`` instead of raising, while a fully
    policy-compliant result is ``ACCEPTED``. Use ``fail_closed=True`` or one of the specific
    ``reject_on_*`` flags for entry/order-submission paths that must keep the historical fail-closed
    behavior.
    """

    minimum_resolved_resources: int = 1
    minimum_resolved_required_resources: int = 0
    fail_closed: bool = False
    reject_on_required_error: bool = False
    reject_on_acceptance_violation: bool = False
    reject_on_consistency_violation: bool = False
    accepted_actions: Iterable[str] = ("read", "evaluate", "act")
    degraded_actions: Iterable[str] = ("read", "evaluate")
    rejected_actions: Iterable[str] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("minimum_resolved_resources", "minimum_resolved_required_resources"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
            if value < 0:
                raise ValueError(f"{name} cannot be negative")
        for name in (
            "fail_closed",
            "reject_on_required_error",
            "reject_on_acceptance_violation",
            "reject_on_consistency_violation",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be a boolean")
        object.__setattr__(
            self,
            "accepted_actions",
            _normalize_actions(self.accepted_actions, name="accepted_actions"),
        )
        object.__setattr__(
            self,
            "degraded_actions",
            _normalize_actions(self.degraded_actions, name="degraded_actions"),
        )
        object.__setattr__(
            self,
            "rejected_actions",
            _normalize_actions(self.rejected_actions, name="rejected_actions"),
        )
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @classmethod
    def fail_closed_policy(
        cls,
        *,
        accepted_actions: Iterable[str] = ("read", "evaluate", "act"),
    ) -> RequestDegradationPolicy:
        """Return a policy matching the legacy strict behavior for critical execution paths."""

        return cls(
            fail_closed=True,
            reject_on_required_error=True,
            reject_on_acceptance_violation=True,
            reject_on_consistency_violation=True,
            accepted_actions=accepted_actions,
            degraded_actions=(),
        )

    @classmethod
    def read_only_degraded(
        cls,
        *,
        minimum_resolved_resources: int = 1,
        accepted_actions: Iterable[str] = ("read", "evaluate"),
        degraded_actions: Iterable[str] = ("read", "evaluate"),
    ) -> RequestDegradationPolicy:
        """Return a permissive policy for multi-symbol evaluation and monitoring."""

        return cls(
            minimum_resolved_resources=minimum_resolved_resources,
            accepted_actions=accepted_actions,
            degraded_actions=degraded_actions,
        )

    @classmethod
    def risk_reduction(
        cls,
        *,
        minimum_resolved_resources: int = 1,
    ) -> RequestDegradationPolicy:
        """Return a policy for defensive actions such as cancel-only or reduce-only exits."""

        return cls(
            minimum_resolved_resources=minimum_resolved_resources,
            accepted_actions=("read", "evaluate", "cancel_orders", "reduce_only_exit"),
            degraded_actions=("read", "cancel_orders", "reduce_only_exit"),
            reject_on_consistency_violation=False,
            reject_on_acceptance_violation=False,
        )


@dataclass(frozen=True)
class SnapshotResult:
    """Non-throwing result returned by degraded operational APIs."""

    state: SnapshotResultState
    snapshot: Snapshot
    request: SnapshotRequest
    allowed_actions: tuple[str, ...] = ()
    reason: str = ""
    required_errors: Mapping[ResourceKey, Exception] = field(default_factory=dict)
    optional_errors: Mapping[ResourceKey, Exception] = field(default_factory=dict)
    acceptance_violations: tuple[SnapshotAcceptanceViolation, ...] = ()
    consistency_violation: ObservationSkewViolation | None = None
    error: BaseException | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.state, SnapshotResultState):
            object.__setattr__(self, "state", SnapshotResultState(self.state))
        object.__setattr__(self, "allowed_actions", _normalize_actions(self.allowed_actions))
        object.__setattr__(self, "required_errors", MappingProxyType(dict(self.required_errors)))
        object.__setattr__(self, "optional_errors", MappingProxyType(dict(self.optional_errors)))
        object.__setattr__(self, "acceptance_violations", tuple(self.acceptance_violations))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

    @property
    def accepted(self) -> bool:
        return self.state is SnapshotResultState.ACCEPTED

    @property
    def degraded(self) -> bool:
        return self.state is SnapshotResultState.DEGRADED

    @property
    def rejected(self) -> bool:
        return self.state is SnapshotResultState.REJECTED

    @property
    def complete(self) -> bool:
        return self.accepted and self.snapshot.complete

    def raise_for_rejected(self) -> None:
        """Raise the underlying Coalestra error when the result is rejected."""

        if self.rejected:
            raise self._as_error()

    def require_accepted(self) -> Snapshot:
        """Return the snapshot only when fully accepted; otherwise raise a policy error."""

        if not self.accepted:
            raise self._as_error()
        return self.snapshot

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe operational summary for logs, audits, and readiness checks."""

        return {
            "state": self.state.value,
            "snapshot_id": self.snapshot.snapshot_id,
            "resources": len(self.snapshot.resources),
            "errors": len(self.snapshot.errors),
            "allowed_actions": list(self.allowed_actions),
            "reason": self.reason,
            "required_errors": {
                str(key): type(error).__name__ for key, error in self.required_errors.items()
            },
            "optional_errors": {
                str(key): type(error).__name__ for key, error in self.optional_errors.items()
            },
            "acceptance_violations": [
                {
                    "reason": violation.reason.value,
                    "keys": [str(key) for key in violation.keys],
                    "message": violation.message,
                    "current_age_seconds": violation.current_age_seconds,
                    "max_age_seconds": violation.max_age_seconds,
                    "authority_rank": violation.authority_rank,
                    "minimum_authority_rank": violation.minimum_authority_rank,
                    "accepted_count": violation.accepted_count,
                    "required_count": violation.required_count,
                }
                for violation in self.acceptance_violations
            ],
            "consistency_violation": _consistency_violation_to_dict(self.consistency_violation),
            "metadata": dict(self.metadata),
        }

    def _as_error(self) -> BaseException:
        if self.error is not None:
            return self.error
        if self.consistency_violation is not None:
            violation = self.consistency_violation
            return SnapshotConsistencyError(
                keys=violation.keys,
                oldest_key=violation.oldest_key,
                oldest_observed_at=violation.oldest_observed_at,
                newest_key=violation.newest_key,
                newest_observed_at=violation.newest_observed_at,
                observation_skew_seconds=violation.observation_skew_seconds,
                max_observation_skew_seconds=violation.max_observation_skew_seconds,
                snapshot=self.snapshot,
            )
        if self.acceptance_violations:
            return SnapshotAcceptanceError(
                violations=self.acceptance_violations,
                snapshot=self.snapshot,
            )
        errors = {**dict(self.required_errors), **dict(self.optional_errors)}
        if not errors:
            errors = dict(self.snapshot.errors)
        return SnapshotBuildError(errors, snapshot=self.snapshot)


async def try_resolve_request(
    session: Any,
    request: SnapshotRequest,
    *,
    retry_errors: bool = False,
    degradation_policy: RequestDegradationPolicy | None = None,
) -> SnapshotResult:
    """Resolve a request without throwing for operationally useful partial snapshots.

    This is the safe multi-symbol primitive: one symbol can be ``DEGRADED`` or ``REJECTED`` without
    aborting the whole scan, while callers can still use ``require_accepted()`` for fail-closed
    execution paths.
    """

    policy = degradation_policy or RequestDegradationPolicy()
    try:
        snapshot = await session.resolve(
            request.keys,
            strict=False,
            retry_errors=retry_errors,
        )
        result = _classify_snapshot_result(session, request, snapshot, policy)
    except SnapshotBuildError as error:
        snapshot = error.snapshot
        if snapshot is None:
            snapshot = _empty_snapshot_from_session(session, error.errors)
        result = _classify_snapshot_result(
            session,
            request,
            snapshot,
            policy,
            base_error=error,
        )
    except CoalestraError as error:
        snapshot = _empty_snapshot_from_session(session, {})
        result = SnapshotResult(
            state=SnapshotResultState.REJECTED,
            snapshot=snapshot,
            request=request,
            allowed_actions=tuple(policy.rejected_actions),
            reason=type(error).__name__,
            error=error,
            metadata=policy.metadata,
        )
    _record_try_result(session, result)
    return result


async def try_build_request(
    builder: Any,
    request: SnapshotRequest,
    *,
    deadline_seconds: float | None = None,
    metadata: Mapping[str, Any] | None = None,
    snapshot_id: str | None = None,
    degradation_policy: RequestDegradationPolicy | None = None,
) -> SnapshotResult:
    """Build one request and return ``SnapshotResult`` instead of raising on partial failure."""

    session = builder.session(
        deadline_seconds=deadline_seconds,
        metadata=metadata,
        snapshot_id=snapshot_id,
    )
    try:
        result = await try_resolve_request(
            session,
            request,
            degradation_policy=degradation_policy,
        )
        recorder = getattr(builder, "_record_snapshot_built", None)
        if callable(recorder):
            recorder(result.snapshot, strict=False, failed=not result.accepted)
        return result
    finally:
        await session.close()


async def try_build_requests(
    builder: Any,
    requests: Iterable[SnapshotRequest],
    *,
    deadline_seconds: float | None = None,
    metadata: Mapping[str, Any] | None = None,
    snapshot_id_prefix: str | None = None,
    degradation_policy: RequestDegradationPolicy | None = None,
    max_concurrent: int | None = None,
) -> tuple[SnapshotResult, ...]:
    """Build many independent requests with bounded fanout and per-request degradation.

    Results preserve input order. This is the preferred API for multi-symbol systems: each symbol
    can fail or degrade independently, and the caller can continue operating on the accepted subset.
    """

    request_list = tuple(requests)
    if not request_list:
        return ()
    if max_concurrent is None:
        max_concurrent = min(len(request_list), max(1, int(getattr(builder, "max_concurrency", 1))))
    if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int):
        raise TypeError("max_concurrent must be an integer or None")
    if max_concurrent < 1:
        raise ValueError("max_concurrent must be at least 1")

    semaphore = asyncio.Semaphore(max_concurrent)

    async def build_one(index: int, request: SnapshotRequest) -> SnapshotResult:
        async with semaphore:
            derived_snapshot_id = None
            if snapshot_id_prefix is not None:
                derived_snapshot_id = f"{snapshot_id_prefix}:{index}"
            return await try_build_request(
                builder,
                request,
                deadline_seconds=deadline_seconds,
                metadata=metadata,
                snapshot_id=derived_snapshot_id,
                degradation_policy=degradation_policy,
            )

    tasks = (build_one(i, req) for i, req in enumerate(request_list))
    return tuple(await asyncio.gather(*tasks))


def install_operational_methods() -> None:
    """Install ergonomic method aliases on the existing builder/session facades."""

    from coalestra.orchestration.builder import SnapshotBuilder
    from coalestra.orchestration.session import SnapshotSession
    from coalestra.sync import SyncSnapshotBuilder, SyncSnapshotSession

    if not hasattr(SnapshotSession, "try_resolve_request"):
        SnapshotSession.try_resolve_request = _session_try_resolve_request  # type: ignore[attr-defined]
    if not hasattr(SnapshotBuilder, "try_build_request"):
        SnapshotBuilder.try_build_request = _builder_try_build_request  # type: ignore[attr-defined]
    if not hasattr(SnapshotBuilder, "try_build_requests"):
        SnapshotBuilder.try_build_requests = _builder_try_build_requests  # type: ignore[attr-defined]
    if not hasattr(SyncSnapshotSession, "try_resolve_request"):
        SyncSnapshotSession.try_resolve_request = _sync_session_try_resolve_request  # type: ignore[attr-defined]
    if not hasattr(SyncSnapshotBuilder, "try_build_request"):
        SyncSnapshotBuilder.try_build_request = _sync_builder_try_build_request  # type: ignore[attr-defined]
    if not hasattr(SyncSnapshotBuilder, "try_build_requests"):
        SyncSnapshotBuilder.try_build_requests = _sync_builder_try_build_requests  # type: ignore[attr-defined]


async def _session_try_resolve_request(
    self: Any,
    request: SnapshotRequest,
    *,
    retry_errors: bool = False,
    degradation_policy: RequestDegradationPolicy | None = None,
) -> SnapshotResult:
    return await try_resolve_request(
        self,
        request,
        retry_errors=retry_errors,
        degradation_policy=degradation_policy,
    )


async def _builder_try_build_request(
    self: Any,
    request: SnapshotRequest,
    *,
    deadline_seconds: float | None = None,
    metadata: Mapping[str, Any] | None = None,
    snapshot_id: str | None = None,
    degradation_policy: RequestDegradationPolicy | None = None,
) -> SnapshotResult:
    return await try_build_request(
        self,
        request,
        deadline_seconds=deadline_seconds,
        metadata=metadata,
        snapshot_id=snapshot_id,
        degradation_policy=degradation_policy,
    )


async def _builder_try_build_requests(
    self: Any,
    requests: Iterable[SnapshotRequest],
    *,
    deadline_seconds: float | None = None,
    metadata: Mapping[str, Any] | None = None,
    snapshot_id_prefix: str | None = None,
    degradation_policy: RequestDegradationPolicy | None = None,
    max_concurrent: int | None = None,
) -> tuple[SnapshotResult, ...]:
    return await try_build_requests(
        self,
        requests,
        deadline_seconds=deadline_seconds,
        metadata=metadata,
        snapshot_id_prefix=snapshot_id_prefix,
        degradation_policy=degradation_policy,
        max_concurrent=max_concurrent,
    )


def _sync_session_try_resolve_request(
    self: Any,
    request: SnapshotRequest,
    *,
    retry_errors: bool = False,
    degradation_policy: RequestDegradationPolicy | None = None,
) -> SnapshotResult:
    self._ensure_open()
    return cast(
        SnapshotResult,
        self._owner._submit(
            try_resolve_request(
                self._session,
                request,
                retry_errors=retry_errors,
                degradation_policy=degradation_policy,
            )
        ),
    )


def _sync_builder_try_build_request(
    self: Any,
    request: SnapshotRequest,
    *,
    deadline_seconds: float | None = None,
    metadata: Mapping[str, Any] | None = None,
    snapshot_id: str | None = None,
    degradation_policy: RequestDegradationPolicy | None = None,
) -> SnapshotResult:
    self._ensure_open()
    return cast(
        SnapshotResult,
        self._submit(
            try_build_request(
                self.builder,
                request,
                deadline_seconds=deadline_seconds,
                metadata=metadata,
                snapshot_id=snapshot_id,
                degradation_policy=degradation_policy,
            )
        ),
    )


def _sync_builder_try_build_requests(
    self: Any,
    requests: Iterable[SnapshotRequest],
    *,
    deadline_seconds: float | None = None,
    metadata: Mapping[str, Any] | None = None,
    snapshot_id_prefix: str | None = None,
    degradation_policy: RequestDegradationPolicy | None = None,
    max_concurrent: int | None = None,
) -> tuple[SnapshotResult, ...]:
    self._ensure_open()
    prepared = tuple(requests)
    return cast(
        tuple[SnapshotResult, ...],
        self._submit(
            try_build_requests(
                self.builder,
                prepared,
                deadline_seconds=deadline_seconds,
                metadata=metadata,
                snapshot_id_prefix=snapshot_id_prefix,
                degradation_policy=degradation_policy,
                max_concurrent=max_concurrent,
            )
        ),
    )


def _classify_snapshot_result(
    session: Any,
    request: SnapshotRequest,
    snapshot: Snapshot,
    policy: RequestDegradationPolicy,
    *,
    base_error: BaseException | None = None,
) -> SnapshotResult:
    required_errors = {
        key: snapshot.errors[key] for key in request.required if key in snapshot.errors
    }
    optional_errors = {
        key: snapshot.errors[key] for key in request.optional if key in snapshot.errors
    }
    consistency_violation = _find_consistency_violation(request, snapshot)
    acceptance_violations = _find_acceptance_violations(session, request, snapshot)

    resolved_required = sum(1 for key in request.required if key in snapshot.resources)
    reason_parts: list[str] = []
    if required_errors:
        reason_parts.append("required_errors")
    if optional_errors:
        reason_parts.append("optional_errors")
    if consistency_violation is not None:
        reason_parts.append("consistency_violation")
    if acceptance_violations:
        reason_parts.append("acceptance_violation")

    if not reason_parts:
        return SnapshotResult(
            state=SnapshotResultState.ACCEPTED,
            snapshot=snapshot,
            request=request,
            allowed_actions=tuple(policy.accepted_actions),
            reason="accepted",
            metadata=policy.metadata,
        )

    reject = (
        policy.fail_closed
        or len(snapshot.resources) < policy.minimum_resolved_resources
        or resolved_required < policy.minimum_resolved_required_resources
        or (bool(required_errors) and policy.reject_on_required_error)
        or (bool(acceptance_violations) and policy.reject_on_acceptance_violation)
        or (consistency_violation is not None and policy.reject_on_consistency_violation)
    )
    state = SnapshotResultState.REJECTED if reject else SnapshotResultState.DEGRADED
    return SnapshotResult(
        state=state,
        snapshot=snapshot,
        request=request,
        allowed_actions=tuple(policy.rejected_actions if reject else policy.degraded_actions),
        reason=",".join(reason_parts),
        required_errors=required_errors,
        optional_errors=optional_errors,
        acceptance_violations=acceptance_violations,
        consistency_violation=consistency_violation,
        error=base_error if reject else None,
        metadata=policy.metadata,
    )


def _find_consistency_violation(
    request: SnapshotRequest,
    snapshot: Snapshot,
) -> ObservationSkewViolation | None:
    policy = request.consistency_policy
    if policy is None:
        return None
    keys = request.keys if policy.include_optional_resources else request.required
    return find_observation_skew_violation(snapshot.resources, keys, policy)


def _find_acceptance_violations(
    session: Any,
    request: SnapshotRequest,
    snapshot: Snapshot,
) -> tuple[SnapshotAcceptanceViolation, ...]:
    policy = request.acceptance_policy
    if policy is None:
        return ()
    builder = getattr(session, "_builder", None)
    if builder is None:
        return ()
    resolver = getattr(builder, "policy_resolver", None)
    if resolver is None:
        return ()
    now = builder.clock.now()
    return find_snapshot_acceptance_violations(
        snapshot.resources,
        required_keys=request.required,
        optional_keys=request.optional,
        policy=policy,
        now=now,
        freshness_policy_for=resolver.resolve,
    )


def _empty_snapshot_from_session(
    session: Any,
    errors: Mapping[ResourceKey, Exception],
) -> Snapshot:
    return Snapshot(
        snapshot_id=str(getattr(session, "snapshot_id", "")),
        created_at=float(getattr(session, "created_at", 0.0)),
        resources={},
        errors=dict(errors),
    )


def _record_try_result(session: Any, result: SnapshotResult) -> None:
    builder = getattr(session, "_builder", None)
    if builder is None:
        return
    try:
        builder.metrics.increment(
            "snapshot_try_request_total",
            status=result.state.value,
        )
        builder.events.emit(
            "snapshot_try_request_resolved",
            snapshot_id=result.snapshot.snapshot_id,
            state=result.state.value,
            resources=len(result.snapshot.resources),
            errors=len(result.snapshot.errors),
            required_errors=len(result.required_errors),
            optional_errors=len(result.optional_errors),
            acceptance_violations=len(result.acceptance_violations),
            consistency_failed=result.consistency_violation is not None,
            allowed_actions=",".join(result.allowed_actions),
            reason=result.reason,
        )
    except Exception:
        return


def _normalize_actions(actions: Iterable[str], *, name: str = "allowed_actions") -> tuple[str, ...]:
    normalized = tuple(dict.fromkeys(str(item).strip() for item in actions))
    if any(not item for item in normalized):
        raise ValueError(f"{name} cannot contain empty action names")
    return normalized


def _consistency_violation_to_dict(
    violation: ObservationSkewViolation | None,
) -> dict[str, Any] | None:
    if violation is None:
        return None
    return {
        "keys": [str(key) for key in violation.keys],
        "oldest_key": str(violation.oldest_key),
        "oldest_observed_at": violation.oldest_observed_at,
        "newest_key": str(violation.newest_key),
        "newest_observed_at": violation.newest_observed_at,
        "observation_skew_seconds": violation.observation_skew_seconds,
        "max_observation_skew_seconds": violation.max_observation_skew_seconds,
    }


__all__ = [
    "RequestDegradationPolicy",
    "SnapshotResult",
    "SnapshotResultState",
    "install_operational_methods",
    "try_build_request",
    "try_build_requests",
    "try_resolve_request",
]
