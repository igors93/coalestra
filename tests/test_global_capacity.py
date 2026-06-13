from __future__ import annotations

import asyncio
from contextlib import suppress

from coalestra import CallableSource, ResourceKey, SnapshotBuilder


def test_global_capacity_is_shared_across_concurrent_builds() -> None:
    active = 0
    peak = 0
    lock = asyncio.Lock()

    async def fetch(key, _context):
        nonlocal active, peak
        async with lock:
            active += 1
            peak = max(peak, active)
        await asyncio.sleep(0.02)
        async with lock:
            active -= 1
        return key.subject

    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="shared",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                )
            ],
            max_concurrency=2,
        )
        first = [ResourceKey("test", "value", f"A{i}") for i in range(4)]
        second = [ResourceKey("test", "value", f"B{i}") for i in range(4)]
        await asyncio.gather(builder.build(first), builder.build(second))
        capacity = await builder.capacity.snapshot()
        assert capacity["__global__"].in_use == 0
        assert capacity["__global__"].waiting == 0

    asyncio.run(scenario())
    assert peak == 2


def test_source_capacity_is_independent_and_explicit_override_wins() -> None:
    active_a = 0
    active_total = 0
    peak_a = 0
    peak_total = 0
    lock = asyncio.Lock()

    async def fetch(key, _context):
        nonlocal active_a, active_total, peak_a, peak_total
        async with lock:
            active_total += 1
            peak_total = max(peak_total, active_total)
            if key.namespace == "a":
                active_a += 1
                peak_a = max(peak_a, active_a)
        await asyncio.sleep(0.02)
        async with lock:
            active_total -= 1
            if key.namespace == "a":
                active_a -= 1
        return str(key)

    async def scenario() -> None:
        source_a = CallableSource(
            name="source-a",
            priority=10,
            supports=lambda key: key.namespace == "a",
            fetcher=fetch,
            max_concurrency=3,
        )
        source_b = CallableSource(
            name="source-b",
            priority=10,
            supports=lambda key: key.namespace == "b",
            fetcher=fetch,
            max_concurrency=2,
        )
        builder = SnapshotBuilder(
            [source_a, source_b],
            max_concurrency=3,
            source_concurrency={"source-a": 1},
        )
        keys = [
            *(ResourceKey("a", "value", str(index)) for index in range(4)),
            *(ResourceKey("b", "value", str(index)) for index in range(4)),
        ]
        await builder.build(keys)
        assert builder.capacity.limit_for("source-a") == 1
        assert builder.capacity.limit_for("source-b") == 2

    asyncio.run(scenario())
    assert peak_a == 1
    assert 2 <= peak_total <= 3


def test_cancelled_work_releases_global_and_source_capacity() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def fetch(key, _context):
        if key.subject == "BLOCK":
            started.set()
            await release.wait()
        return key.subject

    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="limited",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                    max_concurrency=1,
                )
            ],
            max_concurrency=1,
        )
        blocked = asyncio.create_task(builder.build([ResourceKey("test", "value", "BLOCK")]))
        await started.wait()
        blocked.cancel()
        with suppress(asyncio.CancelledError):
            await blocked

        # Single-flight shields shared work from one caller's cancellation. Release the actual
        # source operation, then verify both limiters return their slots.
        release.set()
        for _ in range(20):
            capacity = await builder.capacity.snapshot()
            if capacity["__global__"].in_use == 0:
                break
            await asyncio.sleep(0.01)
        assert capacity["__global__"].in_use == 0
        assert capacity["limited"].in_use == 0

        result = await asyncio.wait_for(
            builder.build([ResourceKey("test", "value", "NEXT")]),
            timeout=0.5,
        )
        assert next(iter(result.values())).value == "NEXT"

    asyncio.run(scenario())
