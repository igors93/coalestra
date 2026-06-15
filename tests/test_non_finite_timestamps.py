from __future__ import annotations

import math

import pytest

from coalestra import (
    FetchContext,
    FreshnessPolicy,
    ObservationPolicy,
    ResourceKey,
    ResourceUpdate,
    Snapshot,
    SnapshotValue,
    SourcePayload,
)

KEY = ResourceKey("time", "value", "A")
NON_FINITE_VALUES = (
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
)


@pytest.mark.parametrize("invalid_timestamp", NON_FINITE_VALUES)
def test_source_payload_rejects_non_finite_observed_at(
    invalid_timestamp: float,
) -> None:
    with pytest.raises(ValueError, match="observed_at must be finite"):
        SourcePayload(value="invalid", observed_at=invalid_timestamp)


@pytest.mark.parametrize("invalid_timestamp", NON_FINITE_VALUES)
def test_resource_update_rejects_non_finite_observed_at(
    invalid_timestamp: float,
) -> None:
    with pytest.raises(ValueError, match="observed_at must be finite"):
        ResourceUpdate(
            KEY,
            "invalid",
            source="stream",
            observed_at=invalid_timestamp,
        )


@pytest.mark.parametrize("invalid_timestamp", NON_FINITE_VALUES)
def test_snapshot_value_rejects_non_finite_observed_at(
    invalid_timestamp: float,
) -> None:
    with pytest.raises(ValueError, match="observed_at must be finite"):
        SnapshotValue(
            key=KEY,
            value="invalid",
            source="test",
            observed_at=invalid_timestamp,
            fetched_at=100.0,
            age_seconds=0.0,
            stale=False,
            from_cache=False,
            latency_ms=0.0,
        )


@pytest.mark.parametrize("invalid_timestamp", NON_FINITE_VALUES)
def test_snapshot_value_rejects_non_finite_fetched_at(
    invalid_timestamp: float,
) -> None:
    with pytest.raises(ValueError, match="fetched_at must be finite"):
        SnapshotValue(
            key=KEY,
            value="invalid",
            source="test",
            observed_at=100.0,
            fetched_at=invalid_timestamp,
            age_seconds=0.0,
            stale=False,
            from_cache=False,
            latency_ms=0.0,
        )


@pytest.mark.parametrize("invalid_timestamp", NON_FINITE_VALUES)
def test_fetch_context_rejects_non_finite_requested_at(
    invalid_timestamp: float,
) -> None:
    with pytest.raises(ValueError, match="requested_at must be finite"):
        FetchContext(requested_at=invalid_timestamp)


@pytest.mark.parametrize("invalid_timestamp", NON_FINITE_VALUES)
def test_snapshot_rejects_non_finite_created_at(
    invalid_timestamp: float,
) -> None:
    with pytest.raises(ValueError, match="created_at must be finite"):
        Snapshot(
            snapshot_id="invalid",
            created_at=invalid_timestamp,
            resources={},
        )


@pytest.mark.parametrize("invalid_tolerance", NON_FINITE_VALUES)
def test_observation_policy_rejects_non_finite_tolerance(
    invalid_tolerance: float,
) -> None:
    with pytest.raises(
        ValueError,
        match="future_tolerance_seconds must be finite",
    ):
        ObservationPolicy(future_tolerance_seconds=invalid_tolerance)


def test_finite_timestamp_inputs_are_normalized_to_float() -> None:
    payload = SourcePayload(value="valid", observed_at=123)
    update = ResourceUpdate(KEY, "valid", source="stream", observed_at=123)

    assert payload.observed_at == 123.0
    assert update.observed_at == 123.0
    assert isinstance(payload.observed_at, float)
    assert isinstance(update.observed_at, float)


def test_infinite_freshness_policy_remains_supported() -> None:
    policy = FreshnessPolicy(
        ttl_seconds=float("inf"),
        max_stale_seconds=float("inf"),
    )

    assert math.isinf(policy.ttl_seconds)
    assert math.isinf(policy.max_stale_seconds)
