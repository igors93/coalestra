from __future__ import annotations

import asyncio

import pytest

from coalestra import (
    ERROR_DIAGNOSTICS_SCHEMA_VERSION,
    CallableBatchSource,
    FreshnessPolicy,
    InMemoryMetrics,
    ResourceAcceptanceRule,
    ResourceKey,
    SnapshotAcceptanceError,
    SnapshotAcceptancePolicy,
    SnapshotAcceptanceReason,
    SnapshotBuilder,
    SnapshotBuildError,
    SnapshotRequest,
    SnapshotRequirement,
    SourceAuthorityPolicy,
    SourcePayload,
    SyncSnapshotBuilder,
)

KEY_A = ResourceKey("test", "value", "A")
KEY_B = ResourceKey("test", "value", "B")
KEY_C = ResourceKey("test", "value", "C")


class ManualClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def now(self) -> float:
        return self.value

    def monotonic(self) -> float:
        return self.value


def _builder(
    payloads: dict[ResourceKey, SourcePayload[str]],
    *,
    clock: ManualClock | None = None,
    ttl_seconds: float = 10.0,
    authority_rank: int = 0,
    metrics=None,
) -> SnapshotBuilder:
    async def fetch_many(keys, _context):
        return {key: payloads[key] for key in keys if key in payloads}

    return SnapshotBuilder(
        [
            CallableBatchSource(
                name="source",
                priority=1,
                supports=lambda key: key in payloads,
                fetcher=fetch_many,
            )
        ],
        clock=clock,
        default_policy=FreshnessPolicy(ttl_seconds, max(ttl_seconds, 1000.0)),
        authority_policy=SourceAuthorityPolicy({"source": authority_rank}),
        metrics=metrics,
    )


def test_acceptance_configuration_rejects_invalid_values() -> None:
    with pytest.raises(TypeError, match="max_age_seconds"):
        ResourceAcceptanceRule(max_age_seconds=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cannot be negative"):
        ResourceAcceptanceRule(max_age_seconds=-1.0)
    with pytest.raises(TypeError, match="minimum_authority_rank"):
        ResourceAcceptanceRule(minimum_authority_rank=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="at least one constraint"):
        SnapshotAcceptancePolicy()
    with pytest.raises(ValueError, match="at least one resource key"):
        SnapshotRequirement.any_of([])
    with pytest.raises(ValueError, match="between 1"):
        SnapshotRequirement.at_least(3, [KEY_A, KEY_B])


def test_snapshot_request_rejects_acceptance_references_outside_request() -> None:
    policy = SnapshotAcceptancePolicy(requirements=(SnapshotRequirement.any_of([KEY_A, KEY_B]),))

    with pytest.raises(ValueError, match="outside the snapshot request"):
        SnapshotRequest(required=[KEY_A], acceptance_policy=policy)


def test_build_request_rejects_current_age_beyond_limit() -> None:
    clock = ManualClock(100.0)
    metrics = InMemoryMetrics()
    builder = _builder(
        {KEY_A: SourcePayload("A", observed_at=95.0)},
        clock=clock,
        metrics=metrics,
    )
    request = SnapshotRequest(
        required=[KEY_A],
        acceptance_policy=SnapshotAcceptancePolicy(
            default_rule=ResourceAcceptanceRule(max_age_seconds=3.0)
        ),
    )

    with pytest.raises(SnapshotAcceptanceError) as captured:
        asyncio.run(builder.build_request(request))

    error = captured.value
    assert isinstance(error, SnapshotBuildError)
    assert error.snapshot is not None
    assert error.snapshot.value(KEY_A, str) == "A"
    assert error.violations[0].reason is SnapshotAcceptanceReason.TOO_OLD
    assert error.violations[0].current_age_seconds == 5.0
    assert metrics.counter("snapshot_acceptance_total", status="error") == 1
    assert metrics.counter("snapshot_acceptance_violation_total", reason="too_old") == 1
    assert metrics.counter("snapshot_build_total", status="error") == 1
    serialized = error.to_dict()
    assert serialized["schema_version"] == ERROR_DIAGNOSTICS_SCHEMA_VERSION
    assert serialized["error_type"] == "SnapshotAcceptanceError"
    assert serialized["partial_snapshot_available"] is True
    assert serialized["errors"][0]["error_type"] == "ResourceAcceptanceError"


def test_acceptance_recomputes_stale_state_at_evaluation_time() -> None:
    clock = ManualClock(100.0)
    builder = _builder(
        {KEY_A: SourcePayload("A", observed_at=100.0)},
        clock=clock,
        ttl_seconds=2.0,
    )

    async def scenario() -> None:
        async with builder.session() as session:
            await session.resolve([KEY_A])
            clock.value = 103.0
            snapshot = await session.snapshot_async()
            policy = SnapshotAcceptancePolicy(
                default_rule=ResourceAcceptanceRule(allow_stale=False)
            )
            request = SnapshotRequest(required=[KEY_A], acceptance_policy=policy)
            with pytest.raises(SnapshotAcceptanceError) as captured:
                await session.resolve_request(request)
            violation = captured.value.violations[0]
            assert snapshot[KEY_A].stale is False
            assert violation.reason is SnapshotAcceptanceReason.STALE
            assert violation.current_age_seconds == 3.0

    asyncio.run(scenario())


def test_build_request_rejects_insufficient_source_authority() -> None:
    builder = _builder(
        {KEY_A: SourcePayload("A", observed_at=100.0)},
        clock=ManualClock(),
        authority_rank=40,
    )
    request = SnapshotRequest(
        required=[KEY_A],
        acceptance_policy=SnapshotAcceptancePolicy(
            default_rule=ResourceAcceptanceRule(minimum_authority_rank=100)
        ),
    )

    with pytest.raises(SnapshotAcceptanceError) as captured:
        asyncio.run(builder.build_request(request))

    violation = captured.value.violations[0]
    assert violation.reason is SnapshotAcceptanceReason.INSUFFICIENT_AUTHORITY
    assert violation.authority_rank == 40
    assert violation.minimum_authority_rank == 100


def test_any_of_requirement_accepts_one_healthy_optional_resource() -> None:
    builder = _builder(
        {KEY_A: SourcePayload("A", observed_at=100.0)},
        clock=ManualClock(),
    )
    policy = SnapshotAcceptancePolicy(
        requirements=(SnapshotRequirement.any_of([KEY_A, KEY_B], name="position"),)
    )
    request = SnapshotRequest(
        optional=[KEY_A, KEY_B],
        acceptance_policy=policy,
    )

    snapshot = asyncio.run(builder.build_request(request))

    assert snapshot.value(KEY_A, str) == "A"
    assert KEY_B in snapshot.errors


def test_requirement_counts_only_resources_that_pass_quality_rules() -> None:
    builder = _builder(
        {
            KEY_A: SourcePayload("A", observed_at=90.0),
            KEY_B: SourcePayload("B", observed_at=99.0),
        },
        clock=ManualClock(),
    )
    policy = SnapshotAcceptancePolicy(
        default_rule=ResourceAcceptanceRule(max_age_seconds=5.0),
        requirements=(SnapshotRequirement.any_of([KEY_A, KEY_B]),),
    )
    request = SnapshotRequest(optional=[KEY_A, KEY_B], acceptance_policy=policy)

    snapshot = asyncio.run(builder.build_request(request))

    assert snapshot.value(KEY_B, str) == "B"


def test_resource_rule_override_can_exempt_one_required_resource() -> None:
    builder = _builder(
        {
            KEY_A: SourcePayload("A", observed_at=50.0),
            KEY_B: SourcePayload("B", observed_at=100.0),
        },
        clock=ManualClock(),
    )
    policy = SnapshotAcceptancePolicy(
        default_rule=ResourceAcceptanceRule(max_age_seconds=2.0),
        resource_rules={KEY_A: ResourceAcceptanceRule()},
        requirements=(SnapshotRequirement.all_of([KEY_A, KEY_B]),),
    )
    request = SnapshotRequest(
        required=[KEY_A, KEY_B],
        acceptance_policy=policy,
    )

    snapshot = asyncio.run(builder.build_request(request))

    assert snapshot.value(KEY_A, str) == "A"
    assert snapshot.value(KEY_B, str) == "B"


def test_unsatisfied_requirement_reports_group_details() -> None:
    builder = _builder({}, clock=ManualClock())
    policy = SnapshotAcceptancePolicy(
        requirements=(SnapshotRequirement.at_least(2, [KEY_A, KEY_B, KEY_C], name="quorum"),)
    )
    request = SnapshotRequest(optional=[KEY_A, KEY_B, KEY_C], acceptance_policy=policy)

    with pytest.raises(SnapshotAcceptanceError) as captured:
        asyncio.run(builder.build_request(request))

    violation = captured.value.violations[0]
    assert violation.reason is SnapshotAcceptanceReason.REQUIREMENT_UNSATISFIED
    assert violation.requirement_name == "quorum"
    assert violation.accepted_count == 0
    assert violation.required_count == 2
    assert violation.keys == (KEY_A, KEY_B, KEY_C)


def test_optional_resource_is_not_quality_checked_by_default() -> None:
    builder = _builder(
        {
            KEY_A: SourcePayload("A", observed_at=100.0),
            KEY_B: SourcePayload("B", observed_at=50.0),
        },
        clock=ManualClock(),
    )
    policy = SnapshotAcceptancePolicy(default_rule=ResourceAcceptanceRule(max_age_seconds=2.0))
    request = SnapshotRequest(
        required=[KEY_A],
        optional=[KEY_B],
        acceptance_policy=policy,
    )

    snapshot = asyncio.run(builder.build_request(request))

    assert snapshot.value(KEY_B, str) == "B"


def test_optional_resource_can_be_checked_explicitly() -> None:
    builder = _builder(
        {
            KEY_A: SourcePayload("A", observed_at=100.0),
            KEY_B: SourcePayload("B", observed_at=50.0),
        },
        clock=ManualClock(),
    )
    policy = SnapshotAcceptancePolicy(
        default_rule=ResourceAcceptanceRule(max_age_seconds=2.0),
        include_optional_resources=True,
    )
    request = SnapshotRequest(
        required=[KEY_A],
        optional=[KEY_B],
        acceptance_policy=policy,
    )

    with pytest.raises(SnapshotAcceptanceError) as captured:
        asyncio.run(builder.build_request(request))

    assert any(violation.key == KEY_B for violation in captured.value.violations)


def test_revalidation_rejects_candidate_and_retains_previous_state() -> None:
    clock = ManualClock(100.0)
    observed_at = 100.0

    async def fetch(_key, _context):
        return SourcePayload("value", observed_at=observed_at)

    builder = SnapshotBuilder(
        [
            CallableBatchSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda keys, context: _batch_from_single(keys, context, fetch),
            )
        ],
        clock=clock,
        default_policy=FreshnessPolicy(100.0, 100.0),
    )
    policy = SnapshotAcceptancePolicy(default_rule=ResourceAcceptanceRule(max_age_seconds=5.0))

    async def scenario():
        nonlocal observed_at
        async with builder.session() as session:
            first = await session.resolve([KEY_A])
            clock.value = 120.0
            observed_at = 100.0
            with pytest.raises(SnapshotAcceptanceError) as captured:
                await session.revalidate(
                    [KEY_A],
                    strict=False,
                    force_refresh=True,
                    acceptance_policy=policy,
                )
            return first, captured.value, session.snapshot(), await builder.health_snapshot()

    first, error, retained, health = asyncio.run(scenario())

    assert error.snapshot is not None
    assert retained[KEY_A].version == first[KEY_A].version
    assert health.revalidation_attempt_count == 1
    assert health.revalidation_failure_count == 1


async def _batch_from_single(keys, context, fetch):
    return {key: await fetch(key, context) for key in keys}


def test_sync_facade_propagates_acceptance_failures() -> None:
    builder = _builder(
        {KEY_A: SourcePayload("A", observed_at=90.0)},
        clock=ManualClock(),
    )
    request = SnapshotRequest(
        required=[KEY_A],
        acceptance_policy=SnapshotAcceptancePolicy(
            default_rule=ResourceAcceptanceRule(max_age_seconds=1.0)
        ),
    )

    with SyncSnapshotBuilder(builder) as sync_builder, pytest.raises(SnapshotAcceptanceError):
        sync_builder.build_request(request)
