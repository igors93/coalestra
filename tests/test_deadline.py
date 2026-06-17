from __future__ import annotations

import asyncio

import pytest

from coalestra import (
    CallableSource,
    FreshnessPolicy,
    ResourceKey,
    ResourceResolutionError,
    SnapshotBuilder,
    SnapshotBuildError,
    SnapshotDeadlineExceededError,
)
from coalestra.resilience import RetryPolicy

KEY = ResourceKey("test", "slow")


def test_snapshot_deadline_limits_source_resolution() -> None:
    async def slow_fetch(_key, _context):
        await asyncio.sleep(0.2)
        return 1

    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="slow",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=slow_fetch,
                    timeout_seconds=1.0,
                )
            ],
            default_policy=FreshnessPolicy(1.0, 1.0),
            retry_policy=RetryPolicy(max_attempts=1),
        )

        with pytest.raises(SnapshotBuildError) as captured:
            await builder.build([KEY], deadline_seconds=0.02)

        resolution = captured.value.errors[KEY]
        assert isinstance(resolution, ResourceResolutionError)
        assert resolution.failures[0].error_type == SnapshotDeadlineExceededError.__name__

    asyncio.run(scenario())


class _SlowCache:
    async def get(self, key, *, now, policy):
        await asyncio.sleep(0.2)
        raise AssertionError("cache read should be cancelled by the snapshot deadline")

    async def set(self, value) -> None:
        return None

    async def invalidate(self, key) -> None:
        return None

    async def clear(self) -> None:
        return None


def test_snapshot_deadline_during_cache_read_is_reported_per_resource() -> None:
    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="unused",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: 1,
                )
            ],
            cache=_SlowCache(),
            default_policy=FreshnessPolicy(1.0, 1.0),
            retry_policy=RetryPolicy(max_attempts=1),
        )

        with pytest.raises(SnapshotBuildError) as captured:
            await builder.build([KEY], deadline_seconds=0.02)

        resolution = captured.value.errors[KEY]
        assert isinstance(resolution, ResourceResolutionError)
        assert resolution.failures[0].source == "snapshot"
        assert resolution.failures[0].error_type == SnapshotDeadlineExceededError.__name__
        assert captured.value.snapshot is None

    asyncio.run(scenario())
