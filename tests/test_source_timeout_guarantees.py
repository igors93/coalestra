from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from coalestra import (
    BuilderHealthReason,
    BuilderHealthSeverity,
    CallableBatchSource,
    CallableDerivedSource,
    CallableSource,
    ResourceKey,
    SnapshotBuilder,
    SnapshotDeadlineExceededError,
    SourceTimeoutGuaranteeStatus,
)


@dataclass
class _RecordedEvent:
    event_type: str
    payload: dict[str, Any]


class _RecordingEvents:
    def __init__(self) -> None:
        self.records: list[_RecordedEvent] = []

    def emit(self, event_type: str, **payload: Any) -> None:
        self.records.append(_RecordedEvent(event_type, dict(payload)))


KEY = ResourceKey("test", "value")


def test_callable_adapters_expose_non_blocking_declarations_by_default() -> None:
    source = CallableSource(
        name="single",
        priority=1,
        supports=lambda _key: True,
        fetcher=lambda _key, _context: "ok",
    )
    batch = CallableBatchSource(
        name="batch",
        priority=1,
        supports=lambda _key: True,
        fetcher=lambda keys, _context: dict.fromkeys(keys, "ok"),
    )
    derived = CallableDerivedSource(
        name="derived",
        priority=1,
        supports=lambda _key: True,
        dependencies=lambda _key: (),
        deriver=lambda _key, _snapshot, _context: "ok",
    )

    for adapter in (source, batch, derived):
        assert adapter.blocking_io is False
        assert adapter.transport_timeout_seconds is None


def test_builder_rejects_blocking_source_without_transport_timeout() -> None:
    source = CallableSource(
        name="blocking",
        priority=1,
        supports=lambda _key: True,
        fetcher=lambda _key, _context: "ok",
        timeout_seconds=2.0,
        blocking_io=True,
    )

    with pytest.raises(ValueError, match="unsafe_timeout_missing"):
        SnapshotBuilder([source])


def test_builder_rejects_blocking_source_not_offloaded() -> None:
    source = CallableSource(
        name="blocking",
        priority=1,
        supports=lambda _key: True,
        fetcher=lambda _key, _context: "ok",
        timeout_seconds=2.0,
        run_sync_in_thread=False,
        blocking_io=True,
        transport_timeout_seconds=1.0,
    )

    with pytest.raises(ValueError, match="unsafe_not_offloaded"):
        SnapshotBuilder([source])


def test_builder_rejects_transport_timeout_not_inside_source_budget() -> None:
    source = CallableSource(
        name="blocking",
        priority=1,
        supports=lambda _key: True,
        fetcher=lambda _key, _context: "ok",
        timeout_seconds=1.0,
        blocking_io=True,
        transport_timeout_seconds=1.0,
    )

    with pytest.raises(ValueError, match="unsafe_timeout_not_within_source"):
        SnapshotBuilder([source])


def test_protected_blocking_source_is_reported_in_health() -> None:
    async def scenario() -> None:
        source = CallableSource(
            name="blocking",
            priority=1,
            supports=lambda _key: True,
            fetcher=lambda _key, _context: "ok",
            timeout_seconds=2.0,
            blocking_io=True,
            transport_timeout_seconds=1.0,
        )
        builder = SnapshotBuilder([source])
        try:
            snapshot = await builder.build([KEY])
            health = await builder.health_snapshot()

            assert snapshot.value(KEY) == "ok"
            assert health.blocking_source_count == 1
            assert health.protected_blocking_source_count == 1
            assert health.unsafe_blocking_source_count == 0
            guarantee = health.source_timeout_guarantees["blocking"]
            assert guarantee.status is SourceTimeoutGuaranteeStatus.PROTECTED
            assert guarantee.protected is True
            assert health.assess().severity is BuilderHealthSeverity.HEALTHY
            assert health.to_dict()["source_timeout_guarantees"]["blocking"]["status"] == (
                "protected"
            )
        finally:
            await builder.aclose()

    asyncio.run(scenario())


def test_short_snapshot_budget_rejects_blocking_call_before_it_starts() -> None:
    async def scenario() -> None:
        calls = 0

        def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return "unexpected"

        source = CallableSource(
            name="blocking",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            timeout_seconds=2.0,
            blocking_io=True,
            transport_timeout_seconds=1.0,
        )
        builder = SnapshotBuilder([source])
        try:
            snapshot = await builder.build(
                [KEY],
                strict=False,
                deadline_seconds=0.1,
            )
            assert calls == 0
            error = snapshot.errors[KEY]
            assert "transport timeout" in str(error)
            assert any(
                failure.error_type == SnapshotDeadlineExceededError.__name__
                for failure in error.failures
            )
        finally:
            await builder.aclose()

    asyncio.run(scenario())


def test_deadline_dispatch_grace_allows_call_when_budget_is_below_transport_timeout() -> None:
    async def scenario() -> None:
        calls = 0

        def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return "ok"

        source = CallableSource(
            name="blocking",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            timeout_seconds=2.0,
            blocking_io=True,
            transport_timeout_seconds=1.0,
        )
        # deadline_seconds=0.5 < transport_timeout=1.0 — normally rejected.
        # With grace=0.6: transport_timeout(1.0) < budget(0.5) + grace(0.6) → allowed.
        builder = SnapshotBuilder([source], source_deadline_dispatch_grace_seconds=0.6)
        try:
            snapshot = await builder.build([KEY], strict=True, deadline_seconds=0.5)
            assert calls == 1
            assert snapshot.value(KEY) == "ok"
        finally:
            await builder.aclose()

    asyncio.run(scenario())


def test_deadline_dispatch_grace_emits_pressure_event_when_operating_within_window() -> None:
    async def scenario() -> None:
        source = CallableSource(
            name="blocking",
            priority=1,
            supports=lambda _key: True,
            fetcher=lambda _key, _context: "ok",
            timeout_seconds=2.0,
            blocking_io=True,
            transport_timeout_seconds=1.0,
        )
        events = _RecordingEvents()
        builder = SnapshotBuilder(
            [source],
            source_deadline_dispatch_grace_seconds=0.6,
            events=events,
        )
        try:
            await builder.build([KEY], strict=True, deadline_seconds=0.5)
            pressure_events = [
                r
                for r in events.records
                if r.event_type == "source_dispatch_under_deadline_pressure"
            ]
            assert len(pressure_events) == 1
            ev = pressure_events[0]
            assert ev.payload["source"] == "blocking"
            assert ev.payload["transport_timeout_seconds"] == 1.0
            assert ev.payload["gap_seconds"] > 0
        finally:
            await builder.aclose()

    asyncio.run(scenario())


def test_deadline_dispatch_grace_zero_preserves_rejection_behavior() -> None:
    async def scenario() -> None:
        calls = 0

        def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return "unexpected"

        source = CallableSource(
            name="blocking",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            timeout_seconds=2.0,
            blocking_io=True,
            transport_timeout_seconds=1.0,
        )
        builder = SnapshotBuilder([source], source_deadline_dispatch_grace_seconds=0.0)
        try:
            snapshot = await builder.build([KEY], strict=False, deadline_seconds=0.1)
            assert calls == 0
            error = snapshot.errors[KEY]
            assert "transport timeout" in str(error)
            assert any(
                failure.error_type == SnapshotDeadlineExceededError.__name__
                for failure in error.failures
            )
        finally:
            await builder.aclose()

    asyncio.run(scenario())


def test_rejection_error_includes_budget_and_gap_details() -> None:
    async def scenario() -> None:
        source = CallableSource(
            name="blocking",
            priority=1,
            supports=lambda _key: True,
            fetcher=lambda _key, _context: "unexpected",
            timeout_seconds=2.0,
            blocking_io=True,
            transport_timeout_seconds=1.0,
        )
        builder = SnapshotBuilder([source])
        try:
            snapshot = await builder.build([KEY], strict=False, deadline_seconds=0.1)
            failure = next(
                f
                for f in snapshot.errors[KEY].failures
                if f.error_type == SnapshotDeadlineExceededError.__name__
            )
            assert "budget=" in failure.message
            assert "transport_timeout=" in failure.message
            assert "gap=" in failure.message
        finally:
            await builder.aclose()

    asyncio.run(scenario())


def test_builder_rejects_negative_deadline_dispatch_grace() -> None:
    source = CallableSource(
        name="blocking",
        priority=1,
        supports=lambda _key: True,
        fetcher=lambda _key, _context: "ok",
        timeout_seconds=2.0,
        blocking_io=True,
        transport_timeout_seconds=1.0,
    )
    with pytest.raises(
        ValueError, match="source_deadline_dispatch_grace_seconds cannot be negative"
    ):
        SnapshotBuilder([source], source_deadline_dispatch_grace_seconds=-0.1)


def test_unsafe_blocking_source_can_be_allowed_but_health_is_critical() -> None:
    async def scenario() -> None:
        source = CallableSource(
            name="legacy-blocking",
            priority=1,
            supports=lambda _key: True,
            fetcher=lambda _key, _context: "ok",
            timeout_seconds=1.0,
            blocking_io=True,
        )
        builder = SnapshotBuilder(
            [source],
            allow_unsafe_blocking_sources=True,
        )
        try:
            health = await builder.health_snapshot()
            assessment = health.assess()

            assert health.unsafe_blocking_source_count == 1
            assert assessment.severity is BuilderHealthSeverity.CRITICAL
            assert BuilderHealthReason.UNSAFE_BLOCKING_SOURCE in assessment.reasons
        finally:
            await builder.aclose()

    asyncio.run(scenario())


def test_strict_declaration_mode_rejects_legacy_custom_source() -> None:
    class LegacySource:
        name = "legacy"
        priority = 1
        timeout_seconds = 1.0

        def supports(self, _key: ResourceKey) -> bool:
            return True

        async def fetch(self, _key, _context):
            return "ok"

    with pytest.raises(ValueError, match="must explicitly declare"):
        SnapshotBuilder(
            [LegacySource()],
            require_source_timeout_declarations=True,
        )


def test_builder_requires_custom_source_timeout_declarations_by_default() -> None:
    class LegacySource:
        name = "legacy-default"
        priority = 1
        timeout_seconds = 1.0

        def supports(self, _key: ResourceKey) -> bool:
            return True

        async def fetch(self, _key, _context):
            return "ok"

    with pytest.raises(ValueError, match="must explicitly declare"):
        SnapshotBuilder([LegacySource()])


def test_runtime_transport_timeout_contract_violation_is_reported() -> None:
    import time

    async def scenario() -> None:
        source = CallableSource(
            name="slow-transport",
            priority=1,
            supports=lambda _key: True,
            fetcher=lambda _key, _context: (time.sleep(0.03), "ok")[1],
            timeout_seconds=0.2,
            blocking_io=True,
            transport_timeout_seconds=0.005,
        )
        builder = SnapshotBuilder(
            [source],
            source_transport_timeout_grace_seconds=0.0,
        )
        try:
            snapshot = await builder.build([KEY])
            baseline = await builder.health_snapshot()

            assert snapshot.value(KEY) == "ok"
            assert baseline.source_transport_timeout_violation_count == 1
            assert baseline.source_transport_timeout_violations == {"slow-transport": 1}
            assert baseline.to_dict()["source_transport_timeout_violations"] == {
                "slow-transport": 1
            }

            # A baseline is required because violation counters are cumulative.
            assessment = baseline.assess(previous=BuilderHealthLike.zero(baseline))
            assert assessment.severity is BuilderHealthSeverity.DEGRADED
            assert (
                BuilderHealthReason.SOURCE_TRANSPORT_TIMEOUT_VIOLATIONS_INCREASED
                in assessment.reasons
            )
        finally:
            await builder.aclose()

    # Build a compatible zero-counter baseline without manually listing BuilderHealth fields.
    from dataclasses import replace

    class BuilderHealthLike:
        @staticmethod
        def zero(health):
            return replace(
                health,
                source_transport_timeout_violation_count=0,
                source_transport_timeout_violations={},
            )

    asyncio.run(scenario())
