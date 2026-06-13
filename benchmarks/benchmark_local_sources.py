from __future__ import annotations

import asyncio
import statistics
import time

from coalestra import CallableSource, FreshnessPolicy, ResourceKey, SnapshotBuilder

KEYS = tuple(ResourceKey("local", "state", str(index)) for index in range(200))
RUNS = 12


def read_local(key: ResourceKey, _context):
    return key.subject


async def measure(*, run_sync_in_thread: bool) -> float:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="local-state",
                priority=1,
                supports=lambda _key: True,
                fetcher=read_local,
                run_sync_in_thread=run_sync_in_thread,
            )
        ],
        default_policy=FreshnessPolicy(0.0, 0.0),
        max_concurrency=32,
    )
    started = time.perf_counter()
    await builder.build(KEYS)
    return time.perf_counter() - started


async def main() -> None:
    threaded = [await measure(run_sync_in_thread=True) for _ in range(RUNS)]
    inline = [await measure(run_sync_in_thread=False) for _ in range(RUNS)]
    threaded_median = statistics.median(threaded)
    inline_median = statistics.median(inline)
    print(f"resources: {len(KEYS)}")
    print(f"threaded median: {threaded_median * 1000:.3f} ms")
    print(f"inline median: {inline_median * 1000:.3f} ms")
    print(f"inline speedup: {threaded_median / inline_median:.2f}x")


if __name__ == "__main__":
    asyncio.run(main())
