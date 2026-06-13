from __future__ import annotations

import asyncio

import pytest

from coalestra import CallableSource, FreshnessPolicy, ResourceKey, SnapshotBuilder
from coalestra.orchestration.singleflight import SingleFlight

PRICE = ResourceKey("market", "price", "BTCUSDT")


def test_concurrent_builds_coalesce_identical_resource() -> None:
    calls = 0

    async def fetch(_key, _context):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return 123

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
            default_policy=FreshnessPolicy(0.0, 10.0),
        )
        return await asyncio.gather(
            builder.build([PRICE]),
            builder.build([PRICE]),
        )

    first, second = asyncio.run(scenario())

    assert calls == 1
    assert first[PRICE].value == 123
    assert second[PRICE].value == 123
    assert any(
        item.metadata.get("coalesced_request") is True for item in (first[PRICE], second[PRICE])
    )


def test_cancelled_waiter_does_not_cancel_shared_work_or_leak_registry() -> None:
    async def scenario() -> None:
        flight: SingleFlight[str, int] = SingleFlight()
        started = asyncio.Event()
        release = asyncio.Event()

        async def factory() -> int:
            started.set()
            await release.wait()
            return 42

        first = asyncio.create_task(flight.run("key", factory))
        await started.wait()
        second = asyncio.create_task(flight.run("key", factory))
        await asyncio.sleep(0)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second

        release.set()
        assert await first == (42, False)
        await asyncio.sleep(0)
        assert await flight.in_flight() == 0

    asyncio.run(scenario())
