from __future__ import annotations

import asyncio
import statistics
import time

from coalestra import CallableSource, FreshnessPolicy, ResourceKey, SnapshotBuilder

SYMBOLS = tuple(f"ASSET{index}" for index in range(20))
KEYS = tuple(ResourceKey("market", "price", symbol) for symbol in SYMBOLS)
SOURCE_LATENCY_SECONDS = 0.025
RUNS = 10


async def fetch(key: ResourceKey, _context):
    await asyncio.sleep(SOURCE_LATENCY_SECONDS)
    return {"symbol": key.subject, "price": "100.0"}


async def concurrent_run() -> float:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="benchmark-source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ],
        default_policy=FreshnessPolicy(0.0, 0.0),
        max_concurrency=len(KEYS),
    )
    started = time.perf_counter()
    await builder.build(KEYS)
    return time.perf_counter() - started


async def serial_run() -> float:
    started = time.perf_counter()
    for key in KEYS:
        await fetch(key, None)
    return time.perf_counter() - started


async def main() -> None:
    serial = [await serial_run() for _ in range(RUNS)]
    concurrent = [await concurrent_run() for _ in range(RUNS)]
    serial_median = statistics.median(serial)
    concurrent_median = statistics.median(concurrent)
    speedup = serial_median / concurrent_median

    print(f"resources: {len(KEYS)}")
    print(f"source latency: {SOURCE_LATENCY_SECONDS * 1000:.1f} ms")
    print(f"serial median: {serial_median * 1000:.1f} ms")
    print(f"concurrent median: {concurrent_median * 1000:.1f} ms")
    print(f"speedup: {speedup:.2f}x")


if __name__ == "__main__":
    asyncio.run(main())
