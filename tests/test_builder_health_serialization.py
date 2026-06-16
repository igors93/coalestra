from __future__ import annotations

import json
from dataclasses import replace

import pytest

from coalestra import (
    BUILDER_HEALTH_ASSESSMENT_SCHEMA,
    BUILDER_HEALTH_ASSESSMENT_SCHEMA_VERSION,
    BUILDER_HEALTH_SCHEMA,
    BUILDER_HEALTH_SCHEMA_VERSION,
    BufferedSinkStats,
    BuilderHealth,
    BuilderHealthAssessmentPolicy,
    BuilderHealthReason,
    BuilderHealthSeverity,
    CacheStats,
    CapacitySnapshot,
    CircuitIdentity,
    CircuitScope,
    CircuitSnapshot,
    CircuitState,
    PayloadCopyHealth,
)


def _health(**changes: object) -> BuilderHealth:
    base = BuilderHealth(
        closed=False,
        background_refreshes=0,
        singleflight_in_flight=0,
        source_support_cache_entries=0,
        capacity={"__global__": CapacitySnapshot(limit=4, in_use=0, waiting=0)},
        cache=CacheStats(
            size=0,
            max_entries=100,
            hits=0,
            misses=0,
            fresh_hits=0,
            stale_hits=0,
            sets=0,
            invalidations=0,
            evictions=0,
            expirations=0,
        ),
        circuits={
            CircuitIdentity("rest", CircuitScope.SOURCE): CircuitSnapshot(
                state=CircuitState.CLOSED,
                failures=0,
                opened_at=0.0,
                half_open_probe_active=False,
            )
        },
        payload_copy_components={
            "builder": PayloadCopyHealth(run_in_thread=True, max_concurrency=4)
        },
        observability_buffers={
            "events": BufferedSinkStats(
                enqueued=0,
                delivered=0,
                dropped=0,
                failures=0,
                pending=0,
                closed=False,
                max_pending=100,
                worker_alive=True,
            )
        },
    )
    return replace(base, **changes)


def test_builder_health_to_dict_is_versioned_complete_and_json_safe() -> None:
    health = _health()

    payload = health.to_dict()
    encoded = json.dumps(payload, allow_nan=False, sort_keys=True)

    assert encoded
    assert payload["schema"] == BUILDER_HEALTH_SCHEMA
    assert payload["schema_version"] == BUILDER_HEALTH_SCHEMA_VERSION
    assert payload["capacity"]["__global__"] == {
        "limit": 4,
        "in_use": 0,
        "waiting": 0,
    }
    assert payload["cache"]["max_entries"] == 100
    assert payload["circuits"]["rest[source]"]["state"] == "closed"
    assert payload["payload_copy_components"]["builder"]["max_concurrency"] == 4
    assert payload["observability_buffers"]["events"]["worker_alive"] is True
    assert payload["assessment"]["schema"] == BUILDER_HEALTH_ASSESSMENT_SCHEMA
    assert payload["assessment"]["schema_version"] == BUILDER_HEALTH_ASSESSMENT_SCHEMA_VERSION
    assert payload["assessment"]["severity"] == "healthy"


def test_builder_health_to_dict_can_omit_assessment_and_hides_opaque_cache_objects() -> None:
    class OpaqueStats:
        pass

    health = _health(cache=OpaqueStats())

    payload = health.to_dict(include_assessment=False)

    assert payload["assessment"] is None
    assert payload["cache"]["type"].endswith(".OpaqueStats")


def test_health_assessment_is_healthy_without_current_problems_or_baseline() -> None:
    health = _health(
        queue_timeout_count=100,
        source_timeout_count=100,
        payload_copy_timeout_count=100,
    )

    assessment = health.assess()

    assert assessment.severity is BuilderHealthSeverity.HEALTHY
    assert assessment.findings == ()
    assert assessment.baseline_used is False
    assert assessment.reasons == ()


def test_health_assessment_reports_degraded_capacity_and_open_circuit() -> None:
    identity = CircuitIdentity("rest", CircuitScope.SOURCE)
    health = _health(
        waiting_for_capacity=1,
        capacity={"__global__": CapacitySnapshot(limit=4, in_use=4, waiting=1)},
        circuits={
            identity: CircuitSnapshot(
                state=CircuitState.OPEN,
                failures=3,
                opened_at=10.0,
                half_open_probe_active=False,
            )
        },
    )

    assessment = health.assess()

    assert assessment.severity is BuilderHealthSeverity.DEGRADED
    assert assessment.reasons == (
        BuilderHealthReason.SOURCE_CAPACITY_WAITING,
        BuilderHealthReason.CIRCUIT_OPEN,
    )
    assert assessment.to_dict()["reasons"] == [
        "source_capacity_waiting",
        "circuit_open",
    ]


def test_health_assessment_reports_critical_current_state() -> None:
    health = _health(
        closed=True,
        waiting_for_copy_capacity=4,
        payload_copy_components={
            "builder": PayloadCopyHealth(
                run_in_thread=True,
                max_concurrency=4,
                waiting_for_capacity=4,
            )
        },
        pending_submissions=9,
        max_pending_submissions=10,
        payload_copy_shutdown_incomplete=True,
    )

    assessment = health.assess()

    assert assessment.severity is BuilderHealthSeverity.CRITICAL
    assert BuilderHealthReason.BUILDER_CLOSED in assessment.reasons
    assert BuilderHealthReason.PAYLOAD_COPY_CAPACITY_WAITING in assessment.reasons
    assert BuilderHealthReason.SUBMISSION_BACKLOG_HIGH in assessment.reasons
    assert BuilderHealthReason.PAYLOAD_COPY_SHUTDOWN_INCOMPLETE in assessment.reasons


def test_health_assessment_uses_previous_snapshot_for_counter_deltas() -> None:
    previous = _health()
    degraded = replace(previous, source_timeout_count=1)
    critical = replace(previous, source_timeout_count=5)

    degraded_assessment = degraded.assess(previous=previous)
    critical_assessment = critical.assess(previous=previous)

    assert degraded_assessment.severity is BuilderHealthSeverity.DEGRADED
    assert degraded_assessment.baseline_used is True
    assert degraded_assessment.reasons == (BuilderHealthReason.SOURCE_TIMEOUTS_INCREASED,)
    assert critical_assessment.severity is BuilderHealthSeverity.CRITICAL
    assert critical_assessment.findings[0].observed == 5


def test_health_assessment_policy_customizes_thresholds() -> None:
    health = _health(
        pending_submissions=4,
        max_pending_submissions=10,
    )
    default_assessment = health.assess()
    custom_assessment = health.assess(
        policy=BuilderHealthAssessmentPolicy(
            submission_backlog_degraded_ratio=0.3,
            submission_backlog_critical_ratio=0.4,
        )
    )

    assert default_assessment.severity is BuilderHealthSeverity.HEALTHY
    assert custom_assessment.severity is BuilderHealthSeverity.CRITICAL
    assert custom_assessment.reasons == (BuilderHealthReason.SUBMISSION_BACKLOG_HIGH,)


@pytest.mark.parametrize(
    ("arguments", "error_type"),
    [
        ({"capacity_waiting_degraded": 0}, ValueError),
        ({"counter_delta_degraded": True}, TypeError),
        (
            {
                "submission_backlog_degraded_ratio": 0.8,
                "submission_backlog_critical_ratio": 0.5,
            },
            ValueError,
        ),
        ({"observability_backlog_critical_ratio": 1.1}, ValueError),
    ],
)
def test_health_assessment_policy_rejects_invalid_thresholds(
    arguments: dict[str, object],
    error_type: type[Exception],
) -> None:
    with pytest.raises(error_type):
        BuilderHealthAssessmentPolicy(**arguments)  # type: ignore[arg-type]
