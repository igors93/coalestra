from __future__ import annotations

import asyncio

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
