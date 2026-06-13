from __future__ import annotations

import asyncio

from coalestra import (
    CallableSource,
    CircuitBreakerPolicy,
    CircuitScope,
    ResourceKey,
    RetryPolicy,
    SnapshotBuilder,
    SourceResiliencePolicy,
)


def price(symbol: str) -> ResourceKey:
    return ResourceKey("market", "price", symbol)


async def fetch_rest(key, _context):
    await asyncio.sleep(0.01)
    return {"symbol": key.subject, "price": "100.00"}


async def main() -> None:
    rest_policy = SourceResiliencePolicy(
        retry=RetryPolicy(max_attempts=2, base_delay_seconds=0.01),
        circuit=CircuitBreakerPolicy(
            scope=CircuitScope.SUBJECT,
            failure_threshold=2,
            recovery_timeout_seconds=2.0,
        ),
    )
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="rest",
                priority=10,
                supports=lambda key: key.namespace == "market",
                fetcher=fetch_rest,
                max_concurrency=2,
                resilience_policy=rest_policy,
            )
        ],
        max_concurrency=4,
    )

    await builder.publisher.publish(
        price("BTCUSDT"),
        {"symbol": "BTCUSDT", "price": "101.00"},
        source="market-stream",
    )

    snapshot = await builder.build([price("BTCUSDT"), price("ETHUSDT")])
    print(snapshot.value(price("BTCUSDT"), dict))  # published cache value
    print(snapshot.value(price("ETHUSDT"), dict))  # REST fallback
    print(await builder.capacity.snapshot())


if __name__ == "__main__":
    asyncio.run(main())
