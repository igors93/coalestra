from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from coalestra import (
    LEGACY_KEY_NORMALIZER,
    CallableBatchSource,
    CallableSource,
    ObservationPolicy,
    ResourceKey,
    ResourceUpdate,
    SnapshotBuilder,
    SnapshotBuildError,
    SnapshotRequest,
    SourcePayload,
    SourceProtocolError,
    SourceUnavailableError,
    SyncSnapshotBuilder,
)
from coalestra.resilience import RetryPolicy
from coalestra.resilience.retry import attempts_for, run_with_retry


def run(coro):
    return asyncio.run(coro)


def test_snapshot_request_rejects_overlap() -> None:
    key = ResourceKey("x", "y")
    with pytest.raises(ValueError):
        SnapshotRequest(required=[key], optional=[key])


def test_build_request_raises_only_for_required_resources() -> None:
    required = ResourceKey("x", "required")
    optional = ResourceKey("x", "optional")

    async def fetch(key, _context):
        if key == optional:
            raise SourceUnavailableError("optional missing")
        return 1

    builder = SnapshotBuilder(
        [CallableSource(name="source", priority=1, supports=lambda _key: True, fetcher=fetch)],
        retry_policy=RetryPolicy(max_attempts=1),
    )
    snapshot = run(builder.build_request(SnapshotRequest(required=[required], optional=[optional])))
    assert snapshot.value(required, int) == 1
    assert optional in snapshot.errors


def test_build_request_error_contains_partial_snapshot() -> None:
    required = ResourceKey("x", "required")
    optional = ResourceKey("x", "optional")

    async def fetch(key, _context):
        if key == optional:
            return 2
        raise SourceUnavailableError("required missing")

    builder = SnapshotBuilder(
        [CallableSource(name="source", priority=1, supports=lambda _key: True, fetcher=fetch)],
        retry_policy=RetryPolicy(max_attempts=1),
    )
    with pytest.raises(SnapshotBuildError) as captured:
        run(builder.build_request(SnapshotRequest(required=[required], optional=[optional])))
    assert captured.value.snapshot is not None
    assert captured.value.snapshot.value(optional, int) == 2


def test_batch_source_respects_max_batch_size() -> None:
    keys = tuple(ResourceKey("batch", "item", str(index)) for index in range(7))
    seen: list[tuple[ResourceKey, ...]] = []

    async def fetch_many(requested, _context):
        chunk = tuple(requested)
        seen.append(chunk)
        return {key: key.subject for key in chunk}

    builder = SnapshotBuilder(
        [
            CallableBatchSource(
                name="batch",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch_many,
                max_batch_size=3,
            )
        ]
    )
    snapshot = run(builder.build(keys))
    assert len(snapshot) == 7
    assert sorted(len(chunk) for chunk in seen) == [1, 3, 3]
    assert snapshot.diagnostics.batch_chunks == 3


def test_source_support_predicate_is_cached() -> None:
    key = ResourceKey("cache", "supports")
    supports_calls = 0

    def supports(_key):
        nonlocal supports_calls
        supports_calls += 1
        return True

    builder = SnapshotBuilder(
        [CallableSource(name="source", priority=1, supports=supports, fetcher=lambda *_: 1)],
    )
    run(builder.build([key]))
    run(builder.publisher.invalidate(key))
    second = run(builder.build([key]))
    assert supports_calls == 1
    assert second.diagnostics.support_cache_hits >= 1


def test_source_can_disable_support_cache() -> None:
    key = ResourceKey("cache", "dynamic")
    calls = 0

    def supports(_key):
        nonlocal calls
        calls += 1
        return True

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=supports,
                fetcher=lambda *_: 1,
                cache_supports=False,
            )
        ]
    )
    run(builder.build([key]))
    run(builder.publisher.invalidate(key))
    run(builder.build([key]))
    assert calls == 2


def test_future_source_payload_falls_back_to_next_source() -> None:
    key = ResourceKey("time", "value")

    async def future(_key, _context):
        return SourcePayload(value=1, observed_at=time.time() + 60)

    builder = SnapshotBuilder(
        [
            CallableSource(name="future", priority=2, supports=lambda _key: True, fetcher=future),
            CallableSource(
                name="fallback", priority=1, supports=lambda _key: True, fetcher=lambda *_: 2
            ),
        ],
        observation_policy=ObservationPolicy(future_tolerance_seconds=0),
        retry_policy=RetryPolicy(max_attempts=1),
    )
    snapshot = run(builder.build([key]))
    assert snapshot.value(key, int) == 2
    assert snapshot[key].source == "fallback"
    assert snapshot.diagnostics.future_timestamp_rejections >= 1


def test_small_future_clock_skew_is_recorded() -> None:
    key = ResourceKey("time", "skew")
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda *_: SourcePayload(value=1, observed_at=time.time() + 0.5),
            )
        ],
        observation_policy=ObservationPolicy(future_tolerance_seconds=1),
    )
    snapshot = run(builder.build([key]))
    assert snapshot[key].metadata["clock_skew_seconds"] > 0


def test_publisher_rejects_future_event() -> None:
    key = ResourceKey("time", "publish")
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source", priority=1, supports=lambda _key: True, fetcher=lambda *_: 1
            )
        ],
        observation_policy=ObservationPolicy(future_tolerance_seconds=0),
    )
    with pytest.raises(SourceProtocolError):
        run(builder.publisher.publish(key, 1, source="stream", observed_at=time.time() + 60))


def test_publish_many_selects_newest_duplicate_update() -> None:
    key = ResourceKey("stream", "position")
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source", priority=1, supports=lambda _key: True, fetcher=lambda *_: 0
            )
        ]
    )
    now = time.time()
    run(
        builder.publisher.publish_many(
            [
                ResourceUpdate(key, "new", source="stream", observed_at=now),
                ResourceUpdate(key, "old", source="stream", observed_at=now - 1),
            ]
        )
    )
    snapshot = run(builder.build([key]))
    assert snapshot.value(key, str) == "new"


def test_legacy_resource_key_keeps_normalizer_for_qualifier_operations() -> None:
    key = ResourceKey(
        " Market ",
        " Candles ",
        "btcusdt",
        {"Interval": "1m"},
        normalizer=LEGACY_KEY_NORMALIZER,
    )
    updated = key.with_qualifiers({"LIMIT": 100})
    assert updated.qualifier("interval") == "1m"
    assert updated.qualifier("INTERVAL") == "1m"
    assert updated.qualifier("limit") == "100"
    assert updated.without_qualifiers("LIMIT").qualifier("limit") is None


def test_retry_does_not_sleep_past_deadline() -> None:
    attempts = 0
    sleeps: list[float] = []

    async def operation() -> int:
        nonlocal attempts
        attempts += 1
        raise SourceUnavailableError("down")

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    with pytest.raises(SourceUnavailableError) as captured:
        run(
            run_with_retry(
                operation,
                policy=RetryPolicy(
                    max_attempts=5,
                    base_delay_seconds=1,
                    max_delay_seconds=1,
                    jitter_ratio=0,
                ),
                retryable=lambda _error: True,
                deadline_monotonic=0.5,
                monotonic=lambda: 0.0,
                sleep=fake_sleep,
            )
        )
    assert attempts == 1
    assert sleeps == []
    assert attempts_for(captured.value) == 1


def test_health_snapshot_exposes_runtime_state() -> None:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source", priority=1, supports=lambda _key: True, fetcher=lambda *_: 1
            )
        ]
    )
    health = run(builder.health_snapshot())
    assert health.closed is False
    assert health.singleflight_in_flight == 0
    assert "__global__" in health.capacity


def test_sync_builder_close_closes_underlying_builder() -> None:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source", priority=1, supports=lambda _key: True, fetcher=lambda *_: 1
            )
        ]
    )
    sync = SyncSnapshotBuilder(builder)
    sync.close()
    assert builder.closed is True


def test_managed_lifecycle_closes_components() -> None:
    class ClosingSource:
        name = "source"
        priority = 1
        timeout_seconds = None
        blocking_io = False
        blocking_io_offloaded = False
        transport_timeout_seconds = None
        closed = False

        def supports(self, _key: ResourceKey) -> bool:
            return True

        async def fetch(self, _key: ResourceKey, _context: Any) -> SourcePayload[int]:
            return SourcePayload(1)

        def close(self) -> None:
            self.closed = True

    source = ClosingSource()
    builder = SnapshotBuilder([source], manage_lifecycle=True)
    run(builder.aclose())
    assert source.closed is True


def test_non_blocking_sync_source_can_run_inline() -> None:
    import threading

    key = ResourceKey("local", "state")
    caller_thread = threading.get_ident()
    seen_thread = 0

    def fetch(_key, _context):
        nonlocal seen_thread
        seen_thread = threading.get_ident()
        return 1

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="local",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
                run_sync_in_thread=False,
            )
        ]
    )
    run(builder.build([key]))
    assert seen_thread == caller_thread
