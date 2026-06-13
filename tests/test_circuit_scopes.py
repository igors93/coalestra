from __future__ import annotations

import asyncio

from coalestra import (
    CallableSource,
    CircuitBreakerPolicy,
    CircuitScope,
    FreshnessPolicy,
    ResourceKey,
    RetryPolicy,
    SnapshotBuilder,
    SourceResiliencePolicy,
    SourceUnavailableError,
)


def _builder_for_scope(scope: CircuitScope, calls: list[ResourceKey]) -> SnapshotBuilder:
    policy = SourceResiliencePolicy(
        retry=RetryPolicy(max_attempts=1),
        circuit=CircuitBreakerPolicy(
            scope=scope,
            failure_threshold=1,
            recovery_timeout_seconds=60.0,
        ),
    )

    async def primary(key, _context):
        calls.append(key)
        if key.name == "price" and key.subject == "BTC":
            raise SourceUnavailableError("BTC price unavailable")
        return f"primary:{key}"

    return SnapshotBuilder(
        [
            CallableSource(
                name="primary",
                priority=100,
                supports=lambda _key: True,
                fetcher=primary,
                resilience_policy=policy,
            ),
            CallableSource(
                name="fallback",
                priority=10,
                supports=lambda _key: True,
                fetcher=lambda key, _context: f"fallback:{key}",
            ),
        ],
        default_policy=FreshnessPolicy(ttl_seconds=0.0, max_stale_seconds=0.0),
    )


def test_subject_scope_isolates_one_subject_across_resource_names() -> None:
    calls: list[ResourceKey] = []
    btc_price = ResourceKey("market", "price", "BTC")
    btc_mark = ResourceKey("market", "mark", "BTC")
    eth_price = ResourceKey("market", "price", "ETH")

    async def scenario() -> tuple[str, str, str]:
        builder = _builder_for_scope(CircuitScope.SUBJECT, calls)
        first = await builder.build([btc_price])
        second = await builder.build([btc_mark])
        third = await builder.build([eth_price])
        return (
            first.value(btc_price, str),
            second.value(btc_mark, str),
            third.value(eth_price, str),
        )

    first, second, third = asyncio.run(scenario())
    assert first.startswith("fallback:")
    assert second.startswith("fallback:")
    assert third.startswith("primary:")
    assert btc_mark not in calls
    assert eth_price in calls


def test_source_scope_blocks_all_resources_after_one_failure() -> None:
    calls: list[ResourceKey] = []
    btc_price = ResourceKey("market", "price", "BTC")
    eth_price = ResourceKey("market", "price", "ETH")

    async def scenario() -> str:
        builder = _builder_for_scope(CircuitScope.SOURCE, calls)
        await builder.build([btc_price])
        second = await builder.build([eth_price])
        return second.value(eth_price, str)

    assert asyncio.run(scenario()).startswith("fallback:")
    assert eth_price not in calls


def test_resource_scope_keeps_other_resources_for_same_subject_available() -> None:
    calls: list[ResourceKey] = []
    btc_price = ResourceKey("market", "price", "BTC")
    btc_mark = ResourceKey("market", "mark", "BTC")

    async def scenario() -> str:
        builder = _builder_for_scope(CircuitScope.RESOURCE, calls)
        await builder.build([btc_price])
        second = await builder.build([btc_mark])
        return second.value(btc_mark, str)

    assert asyncio.run(scenario()).startswith("primary:")
    assert btc_mark in calls


def test_namespace_scope_blocks_only_the_failed_namespace() -> None:
    calls: list[ResourceKey] = []
    btc_price = ResourceKey("market", "price", "BTC")
    eth_mark = ResourceKey("market", "mark", "ETH")
    account = ResourceKey("account", "summary")

    async def scenario() -> tuple[str, str]:
        builder = _builder_for_scope(CircuitScope.NAMESPACE, calls)
        await builder.build([btc_price])
        market_result = await builder.build([eth_mark])
        account_result = await builder.build([account])
        return market_result.value(eth_mark, str), account_result.value(account, str)

    market_result, account_result = asyncio.run(scenario())
    assert market_result.startswith("fallback:")
    assert account_result.startswith("primary:")
    assert eth_mark not in calls
    assert account in calls


def test_retry_policy_can_be_configured_per_source() -> None:
    attempts = {"short": 0, "long": 0}
    short_key = ResourceKey("short", "value")
    long_key = ResourceKey("long", "value")

    async def short_fetch(_key, _context):
        attempts["short"] += 1
        raise SourceUnavailableError("short failure")

    async def long_fetch(_key, _context):
        attempts["long"] += 1
        if attempts["long"] < 3:
            raise SourceUnavailableError("transient")
        return "long-success"

    short_policy = SourceResiliencePolicy(
        retry=RetryPolicy(max_attempts=1),
        circuit=CircuitBreakerPolicy(failure_threshold=10),
    )
    long_policy = SourceResiliencePolicy(
        retry=RetryPolicy(max_attempts=3, base_delay_seconds=0.0, max_delay_seconds=0.0),
        circuit=CircuitBreakerPolicy(failure_threshold=10),
    )

    async def scenario() -> tuple[str, str]:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="short",
                    priority=100,
                    supports=lambda key: key == short_key,
                    fetcher=short_fetch,
                    resilience_policy=short_policy,
                ),
                CallableSource(
                    name="long",
                    priority=100,
                    supports=lambda key: key == long_key,
                    fetcher=long_fetch,
                    resilience_policy=long_policy,
                ),
                CallableSource(
                    name="fallback",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda key, _context: f"fallback:{key.namespace}",
                ),
            ]
        )
        result = await builder.build([short_key, long_key])
        return result.value(short_key, str), result.value(long_key, str)

    short_result, long_result = asyncio.run(scenario())
    assert short_result == "fallback:short"
    assert long_result == "long-success"
    assert attempts == {"short": 1, "long": 3}


def test_stale_data_opens_only_its_subject_circuit() -> None:
    from coalestra import SourcePayload

    calls: list[ResourceKey] = []
    btc_price = ResourceKey("market", "price", "BTC")
    btc_mark = ResourceKey("market", "mark", "BTC")
    eth_price = ResourceKey("market", "price", "ETH")
    policy = SourceResiliencePolicy(
        retry=RetryPolicy(max_attempts=1),
        circuit=CircuitBreakerPolicy(
            scope=CircuitScope.SUBJECT,
            failure_threshold=1,
            recovery_timeout_seconds=60.0,
        ),
    )

    async def primary(key, context):
        calls.append(key)
        if key.subject == "BTC":
            return SourcePayload(value="stale", observed_at=context.requested_at - 10.0)
        return "fresh"

    async def scenario() -> tuple[str, str, str]:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="stream",
                    priority=100,
                    supports=lambda _key: True,
                    fetcher=primary,
                    resilience_policy=policy,
                ),
                CallableSource(
                    name="rest",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda key, _context: f"rest:{key.subject}",
                ),
            ],
            default_policy=FreshnessPolicy(ttl_seconds=1.0, max_stale_seconds=20.0),
        )
        first = await builder.build([btc_price])
        second = await builder.build([btc_mark])
        third = await builder.build([eth_price])
        return (
            first.value(btc_price, str),
            second.value(btc_mark, str),
            third.value(eth_price, str),
        )

    first, second, third = asyncio.run(scenario())
    assert first == "rest:BTC"
    assert second == "rest:BTC"
    assert third == "fresh"
    assert btc_mark not in calls
    assert eth_price in calls


def test_batch_source_applies_subject_circuits_per_group() -> None:
    from coalestra import CallableBatchSource

    btc_price = ResourceKey("market", "price", "BTC")
    eth_price = ResourceKey("market", "price", "ETH")
    btc_mark = ResourceKey("market", "mark", "BTC")
    eth_mark = ResourceKey("market", "mark", "ETH")
    batches: list[tuple[ResourceKey, ...]] = []
    policy = SourceResiliencePolicy(
        retry=RetryPolicy(max_attempts=1),
        circuit=CircuitBreakerPolicy(
            scope=CircuitScope.SUBJECT,
            failure_threshold=1,
            recovery_timeout_seconds=60.0,
        ),
    )

    async def batch_fetch(keys, _context):
        requested = tuple(keys)
        batches.append(requested)
        if btc_price in requested:
            return {key: f"batch:{key.subject}" for key in requested if key != btc_price}
        return {key: f"batch:{key.subject}" for key in requested}

    async def scenario() -> tuple[str, str]:
        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="batch",
                    priority=100,
                    supports=lambda _key: True,
                    fetcher=batch_fetch,
                    resilience_policy=policy,
                ),
                CallableSource(
                    name="fallback",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda key, _context: f"fallback:{key.subject}",
                ),
            ]
        )
        await builder.build([btc_price, eth_price])
        result = await builder.build([btc_mark, eth_mark])
        return result.value(btc_mark, str), result.value(eth_mark, str)

    btc, eth = asyncio.run(scenario())
    assert btc == "fallback:BTC"
    assert eth == "batch:ETH"
    assert batches[1] == (eth_mark,)
