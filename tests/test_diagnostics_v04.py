from __future__ import annotations

import asyncio

from coalestra import (
    CallableBatchSource,
    FreshnessPolicy,
    ResourceKey,
    SnapshotBuilder,
    SourcePayload,
)

KEY_A = ResourceKey("test", "value", "A")
KEY_B = ResourceKey("test", "value", "B")


def test_snapshot_diagnostics_cover_batch_cache_and_observation_skew() -> None:
    async def fetch(keys, _context):
        return {
            key: SourcePayload(value=key.subject, observed_at=100.0 if key == KEY_A else 102.0)
            for key in keys
        }

    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="batch",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                )
            ],
            default_policy=FreshnessPolicy(float("inf"), float("inf")),
        )
        first = await builder.build([KEY_A, KEY_B])
        diagnostics = first.diagnostics

        assert diagnostics.requested_resources == 2
        assert diagnostics.resolved_resources == 2
        assert diagnostics.failed_resources == 0
        assert diagnostics.source_calls == 1
        assert diagnostics.batch_calls == 1
        assert diagnostics.source_calls_by_source == {"batch": 1}
        assert diagnostics.cache_batch_reads == 1
        assert diagnostics.cache_batch_writes == 1
        assert diagnostics.observation_skew_ms == 2000.0

        second = await builder.build([KEY_A, KEY_B])
        assert second.diagnostics.cache_hits == 2
        assert second.diagnostics.source_calls == 0

    asyncio.run(scenario())
