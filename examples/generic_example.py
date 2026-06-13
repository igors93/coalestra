from __future__ import annotations

import asyncio
import time

from coalestra import (
    CallableSource,
    FreshnessPolicy,
    InMemoryMetrics,
    ResourceKey,
    SnapshotBuilder,
    SourcePayload,
    SourceUnavailableError,
)

PRICE = ResourceKey("market", "price", "BTCUSDT")
ACCOUNT = ResourceKey("account", "summary")


async def stream_fetch(key: ResourceKey, _context):
    if key != PRICE:
        raise SourceUnavailableError("stream does not contain this resource")
    await asyncio.sleep(0.01)
    return SourcePayload(
        value={"price": "65000.00"},
        observed_at=time.time(),
        metadata={"transport": "websocket"},
    )


def rest_fetch(key: ResourceKey, _context):
    time.sleep(0.03)
    if key == PRICE:
        return {"price": "65001.00"}
    if key == ACCOUNT:
        return {"available_balance": "1000.00"}
    raise SourceUnavailableError(f"unknown resource {key}")


async def main() -> None:
    metrics = InMemoryMetrics()
    builder = SnapshotBuilder(
        sources=[
            CallableSource(
                name="event-stream",
                priority=100,
                supports=lambda key: key.namespace == "market",
                fetcher=stream_fetch,
                timeout_seconds=0.2,
            ),
            CallableSource(
                name="rest-api",
                priority=10,
                supports=lambda _key: True,
                fetcher=rest_fetch,
                timeout_seconds=1.0,
            ),
        ],
        default_policy=FreshnessPolicy(1.0, 10.0),
        metrics=metrics,
        max_concurrency=4,
    )

    snapshot = await builder.build([PRICE, ACCOUNT])
    for key, item in snapshot.items():
        print(key, item.value, item.source, f"{item.latency_ms:.2f}ms")


if __name__ == "__main__":
    asyncio.run(main())
