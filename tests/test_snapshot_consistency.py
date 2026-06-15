from __future__ import annotations

import asyncio

import pytest

from coalestra import (
    ERROR_DIAGNOSTICS_SCHEMA_VERSION,
    CallableBatchSource,
    FreshnessPolicy,
    InMemoryMetrics,
    ResourceKey,
    SnapshotBuilder,
    SnapshotBuildError,
    SnapshotConsistencyError,
    SnapshotConsistencyPolicy,
    SnapshotRequest,
    SourcePayload,
    SyncSnapshotBuilder,
)

KEY_A = ResourceKey("test", "value", "A")
KEY_B = ResourceKey("test", "value", "B")
KEY_OPTIONAL = ResourceKey("test", "value", "optional")


def _builder(observed_at: dict[ResourceKey, float], *, metrics=None) -> SnapshotBuilder:
    async def fetch_many(keys, _context):
        return {key: SourcePayload(value=key.subject, observed_at=observed_at[key]) for key in keys}

    return SnapshotBuilder(
        [
            CallableBatchSource(
                name="batch",
                priority=1,
                supports=lambda key: key in observed_at,
                fetcher=fetch_many,
            )
        ],
        default_policy=FreshnessPolicy(float("inf"), float("inf")),
        metrics=metrics,
    )


def test_consistency_policy_rejects_invalid_skew_limits() -> None:
    with pytest.raises(TypeError, match="must be a number"):
        SnapshotConsistencyPolicy(True)
    with pytest.raises(ValueError, match="cannot be negative"):
        SnapshotConsistencyPolicy(-0.1)
    with pytest.raises(ValueError, match="must be finite"):
        SnapshotConsistencyPolicy(float("inf"))
    with pytest.raises(ValueError, match="must be finite"):
        SnapshotConsistencyPolicy(float("nan"))
    with pytest.raises(TypeError, match="must be a boolean"):
        SnapshotConsistencyPolicy(1.0, include_optional_resources=1)  # type: ignore[arg-type]


def test_snapshot_request_rejects_an_invalid_consistency_policy() -> None:
    with pytest.raises(TypeError, match="SnapshotConsistencyPolicy"):
        SnapshotRequest(required=[KEY_A], consistency_policy=object())  # type: ignore[arg-type]


def test_build_request_rejects_excessive_required_observation_skew() -> None:
    metrics = InMemoryMetrics()
    builder = _builder({KEY_A: 100.0, KEY_B: 104.0}, metrics=metrics)
    request = SnapshotRequest(
        required=[KEY_A, KEY_B],
        consistency_policy=SnapshotConsistencyPolicy(2.0),
    )

    with pytest.raises(SnapshotConsistencyError) as captured:
        asyncio.run(builder.build_request(request))

    error = captured.value
    assert isinstance(error, SnapshotBuildError)
    assert error.snapshot is not None
    assert error.snapshot.value(KEY_A, str) == "A"
    assert error.snapshot.value(KEY_B, str) == "B"
    assert error.oldest_key == KEY_A
    assert error.newest_key == KEY_B
    assert error.observation_skew_seconds == 4.0
    assert error.max_observation_skew_seconds == 2.0
    assert (
        metrics.counter(
            "snapshot_consistency_total",
            status="error",
            rule="observation_skew",
        )
        == 1
    )
    assert metrics.counter("snapshot_build_total", status="error") == 1

    serialized = error.to_dict()
    assert serialized["schema_version"] == ERROR_DIAGNOSTICS_SCHEMA_VERSION
    assert serialized["error_type"] == "SnapshotConsistencyError"
    assert serialized["partial_snapshot_available"] is True
    assert serialized["errors"] == []


def test_build_request_accepts_required_values_at_the_skew_boundary() -> None:
    builder = _builder({KEY_A: 100.0, KEY_B: 102.0})
    request = SnapshotRequest(
        required=[KEY_A, KEY_B],
        consistency_policy=SnapshotConsistencyPolicy(2.0),
    )

    snapshot = asyncio.run(builder.build_request(request))

    assert snapshot.value(KEY_A, str) == "A"
    assert snapshot.value(KEY_B, str) == "B"


def test_optional_resources_are_excluded_from_consistency_by_default() -> None:
    builder = _builder({KEY_A: 100.0, KEY_B: 101.0, KEY_OPTIONAL: 150.0})
    request = SnapshotRequest(
        required=[KEY_A, KEY_B],
        optional=[KEY_OPTIONAL],
        consistency_policy=SnapshotConsistencyPolicy(2.0),
    )

    snapshot = asyncio.run(builder.build_request(request))

    assert snapshot.value(KEY_OPTIONAL, str) == "optional"


def test_all_optional_requests_can_opt_into_consistency_validation() -> None:
    builder = _builder({KEY_A: 100.0, KEY_B: 105.0})
    request = SnapshotRequest(
        optional=[KEY_A, KEY_B],
        consistency_policy=SnapshotConsistencyPolicy(
            2.0,
            include_optional_resources=True,
        ),
    )

    with pytest.raises(SnapshotConsistencyError):
        asyncio.run(builder.build_request(request))


def test_optional_resources_can_be_included_explicitly() -> None:
    builder = _builder({KEY_A: 100.0, KEY_B: 101.0, KEY_OPTIONAL: 150.0})
    request = SnapshotRequest(
        required=[KEY_A, KEY_B],
        optional=[KEY_OPTIONAL],
        consistency_policy=SnapshotConsistencyPolicy(
            2.0,
            include_optional_resources=True,
        ),
    )

    with pytest.raises(SnapshotConsistencyError) as captured:
        asyncio.run(builder.build_request(request))

    assert captured.value.newest_key == KEY_OPTIONAL
    assert captured.value.keys == (KEY_A, KEY_B, KEY_OPTIONAL)


def test_session_request_checks_only_the_current_request_group() -> None:
    unrelated = ResourceKey("test", "value", "unrelated")
    builder = _builder({unrelated: 10.0, KEY_A: 100.0, KEY_B: 101.0})

    async def scenario():
        async with builder.session() as session:
            await session.resolve([unrelated])
            return await session.resolve_request(
                SnapshotRequest(
                    required=[KEY_A, KEY_B],
                    consistency_policy=SnapshotConsistencyPolicy(2.0),
                )
            )

    snapshot = asyncio.run(scenario())

    assert snapshot.value(unrelated, str) == "unrelated"
    assert snapshot.diagnostics.observation_skew_ms == 91_000.0


def test_revalidation_rejects_inconsistent_candidate_and_retains_previous_state() -> None:
    observations = {KEY_A: 100.0, KEY_B: 100.5}
    builder = _builder(observations)
    policy = SnapshotConsistencyPolicy(2.0)

    async def scenario():
        async with builder.session() as session:
            first = await session.resolve([KEY_A, KEY_B])
            observations[KEY_A] = 200.0
            observations[KEY_B] = 205.0
            with pytest.raises(SnapshotConsistencyError) as captured:
                await session.revalidate(
                    [KEY_A, KEY_B],
                    strict=False,
                    force_refresh=True,
                    consistency_policy=policy,
                )
            return first, captured.value, session.snapshot(), builder.health_snapshot

    first, error, retained, health_factory = asyncio.run(scenario())
    health = asyncio.run(health_factory())

    assert error.snapshot is not None
    assert retained[KEY_A].version == first[KEY_A].version
    assert retained[KEY_B].version == first[KEY_B].version
    assert retained.value(KEY_A, str) == "A"
    assert retained.value(KEY_B, str) == "B"
    assert health.revalidation_attempt_count == 1
    assert health.revalidation_failure_count == 1


def test_sync_facade_propagates_consistency_failures() -> None:
    builder = _builder({KEY_A: 100.0, KEY_B: 110.0})
    request = SnapshotRequest(
        required=[KEY_A, KEY_B],
        consistency_policy=SnapshotConsistencyPolicy(1.0),
    )

    with SyncSnapshotBuilder(builder) as sync_builder, pytest.raises(SnapshotConsistencyError):
        sync_builder.build_request(request)
