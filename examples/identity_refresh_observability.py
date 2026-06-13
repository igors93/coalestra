from __future__ import annotations

import asyncio
import logging

from coalestra import (
    BufferedEventSink,
    CallableSource,
    FreshnessPolicy,
    LoggingEventSink,
    RefreshMode,
    ResourceKey,
    SnapshotBuilder,
    SourcePayload,
)

PRICE = ResourceKey(
    "market",
    "price",
    "BTCUSDT",
    {"venue": "futures"},
)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    sequence = 0

    async def fetch(_key, context):
        nonlocal sequence
        sequence += 1
        return SourcePayload(
            value={"price": 65_000 + sequence},
            observed_at=context.requested_at,
        )

    events = BufferedEventSink(LoggingEventSink(), max_pending=1_000)
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="price-source",
                priority=10,
                supports=lambda key: key == PRICE,
                fetcher=fetch,
            )
        ],
        default_policy=FreshnessPolicy(
            ttl_seconds=1.0,
            max_stale_seconds=10.0,
            refresh_mode=RefreshMode.STALE_WHILE_REVALIDATE,
        ),
        events=events,
    )

    snapshot = await builder.build([PRICE])
    print(snapshot.value(PRICE, dict))
    print(snapshot.diagnostics)

    await builder.wait_for_refreshes()
    await builder.aclose()
    events.close()


if __name__ == "__main__":
    asyncio.run(main())
