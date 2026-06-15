from __future__ import annotations

import asyncio
import time

import pytest

from coalestra import (
    CallableSource,
    FreshnessPolicy,
    ResourceKey,
    SnapshotBuilder,
    SnapshotBuildError,
    SourcePayload,
    SourceUnavailableError,
)
from coalestra.resilience import RetryPolicy

PRICE = ResourceKey("market", "price", "BTCUSDT")
ACCOUNT = ResourceKey("account", "summary")


def run(coro):
    return asyncio.run(coro)


def test_falls_back_to_lower_priority_source() -> None:
    calls: list[str] = []

    async def primary(_key, _context):
        calls.append("primary")
        raise SourceUnavailableError("not ready")

    async def fallback(_key, _context):
        calls.append("fallback")
        return 42

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="primary",
                priority=100,
                supports=lambda _key: True,
                fetcher=primary,
            ),
            CallableSource(
                name="fallback",
                priority=10,
                supports=lambda _key: True,
                fetcher=fallback,
            ),
        ],
        retry_policy=RetryPolicy(max_attempts=1),
    )

    snapshot = run(builder.build([PRICE]))

    assert snapshot.value(PRICE, int) == 42
    assert snapshot[PRICE].source == "fallback"
    assert calls == ["primary", "fallback"]


def test_fresh_cache_avoids_second_source_call() -> None:
    calls = 0

    async def fetch(_key, _context):
        nonlocal calls
        calls += 1
        return {"value": calls}

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ],
        default_policy=FreshnessPolicy(60.0, 60.0),
    )

    first = run(builder.build([PRICE]))
    second = run(builder.build([PRICE]))

    assert calls == 1
    assert first[PRICE].from_cache is False
    assert second[PRICE].from_cache is True
    assert second[PRICE].latency_ms == 0.0


def test_stale_cache_is_used_when_sources_fail() -> None:
    should_fail = False

    async def fetch(_key, _context):
        if should_fail:
            raise SourceUnavailableError("temporary outage")
        return SourcePayload(value=100, observed_at=time.time() - 1.0)

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ],
        default_policy=FreshnessPolicy(0.0, 60.0),
        retry_policy=RetryPolicy(max_attempts=1),
    )

    first = run(builder.build([PRICE]))
    should_fail = True
    second = run(builder.build([PRICE]))

    assert first[PRICE].value == 100
    assert second[PRICE].value == 100
    assert second[PRICE].stale is True
    assert second[PRICE].from_cache is True
    assert second[PRICE].metadata["fallback_error_type"] == "ResourceResolutionError"


def test_non_strict_build_returns_partial_snapshot() -> None:
    async def fetch(key, _context):
        if key == PRICE:
            return 10
        raise SourceUnavailableError("unavailable")

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ],
        retry_policy=RetryPolicy(max_attempts=1),
    )

    snapshot = run(builder.build([PRICE, ACCOUNT], strict=False))

    assert snapshot.complete is False
    assert snapshot.value(PRICE) == 10
    assert ACCOUNT in snapshot.errors


def test_strict_build_raises_aggregate_error() -> None:
    async def fetch(_key, _context):
        raise SourceUnavailableError("unavailable")

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ],
        retry_policy=RetryPolicy(max_attempts=1),
    )

    with pytest.raises(SnapshotBuildError) as captured:
        run(builder.build([PRICE]))

    assert PRICE in captured.value.errors


def test_snapshot_values_are_immutable_mappings() -> None:
    async def fetch(_key, _context):
        return SourcePayload(value=1, metadata={"transport": "test"})

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ]
    )
    snapshot = run(builder.build([PRICE]))

    with pytest.raises(TypeError):
        snapshot.resources[PRICE] = snapshot[PRICE]  # type: ignore[index]
    with pytest.raises(TypeError):
        snapshot[PRICE].metadata["transport"] = "changed"  # type: ignore[index]


def test_duplicate_keys_are_resolved_once_per_snapshot() -> None:
    calls = 0

    async def fetch(_key, _context):
        nonlocal calls
        calls += 1
        return 7

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ]
    )

    snapshot = run(builder.build([PRICE, PRICE, PRICE]))

    assert len(snapshot) == 1
    assert calls == 1
