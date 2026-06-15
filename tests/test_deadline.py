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
