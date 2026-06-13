from __future__ import annotations

import asyncio

from coalestra import CallableSource, ResourceKey, SnapshotBuilder


def test_independent_resources_are_fetched_concurrently() -> None:
    active = 0
    peak = 0
    lock = asyncio.Lock()

    async def fetch(key, _context):
        nonlocal active, peak
        async with lock:
            active += 1
            peak = max(peak, active)
        await asyncio.sleep(0.03)
        async with lock:
            active -= 1
        return key.subject

    keys = [ResourceKey("market", "price", symbol) for symbol in ("A", "B", "C")]

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                )
            ],
            max_concurrency=3,
        )
        return await builder.build(keys)

    snapshot = asyncio.run(scenario())

    assert peak == 3
    assert {snapshot[key].value for key in keys} == {"A", "B", "C"}
