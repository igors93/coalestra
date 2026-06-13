"""Illustrative integration. This file deliberately does not import Alphora."""

from __future__ import annotations

import time
from typing import Any

from coalestra import (
    CallableSource,
    FreshnessPolicy,
    PolicyResolver,
    ResourceKey,
    SnapshotBuilder,
    SourcePayload,
    SourceUnavailableError,
    SyncSnapshotBuilder,
)


def price_key(symbol: str) -> ResourceKey:
    return ResourceKey("market", "price", symbol)


def position_key(symbol: str) -> ResourceKey:
    return ResourceKey("account", "position", symbol)


def open_orders_key(symbol: str) -> ResourceKey:
    return ResourceKey("orders", "open", symbol)


ACCOUNT = ResourceKey("account", "summary")


def build_alphora_snapshot_provider(
    *,
    market_data_hub: Any,
    user_data_cache: Any,
    binance_client: Any,
) -> SyncSnapshotBuilder:
    def stream_supports(key: ResourceKey) -> bool:
        return key.namespace == "market" and key.name == "price"

    def stream_fetch(key: ResourceKey, _context):
        snapshot = market_data_hub.latest_price(key.subject)
        if snapshot is None:
            raise SourceUnavailableError("market stream has no recent price")
        return SourcePayload(
            value=snapshot.price,
            observed_at=snapshot.received_at,
            metadata={"transport": "market_websocket"},
        )

    def user_data_supports(key: ResourceKey) -> bool:
        return key.namespace in {"account", "orders"}

    def user_data_fetch(key: ResourceKey, _context):
        if key == ACCOUNT:
            value = user_data_cache.account_info_snapshot()
        elif key.namespace == "account" and key.name == "position":
            value = user_data_cache.futures_position_for_symbol(key.subject)
        elif key.namespace == "orders" and key.name == "open":
            value = user_data_cache.open_orders_for_symbol(key.subject)
        else:
            value = None
        if value is None:
            raise SourceUnavailableError("user data cache does not contain the resource")
        return SourcePayload(
            value=value,
            observed_at=user_data_cache.last_event_at,
            metadata={"transport": "user_data_stream"},
        )

    def rest_fetch(key: ResourceKey, _context):
        if key.namespace == "market" and key.name == "price":
            return binance_client.ticker_price(key.subject)
        if key == ACCOUNT:
            return binance_client.account_info()
        if key.namespace == "account" and key.name == "position":
            return binance_client.position_risk(key.subject)
        if key.namespace == "orders" and key.name == "open":
            return binance_client.open_orders(key.subject)
        raise SourceUnavailableError(f"REST adapter does not support {key}")

    price_policy = FreshnessPolicy(ttl_seconds=2.0, max_stale_seconds=5.0)
    private_policy = FreshnessPolicy(ttl_seconds=5.0, max_stale_seconds=15.0)

    policy_resolver = PolicyResolver(
        default=private_policy,
        dynamic=lambda key: price_policy if key.namespace == "market" else private_policy,
    )

    builder = SnapshotBuilder(
        sources=[
            CallableSource(
                name="market-stream",
                priority=100,
                supports=stream_supports,
                fetcher=stream_fetch,
                timeout_seconds=0.1,
            ),
            CallableSource(
                name="user-data-cache",
                priority=90,
                supports=user_data_supports,
                fetcher=user_data_fetch,
                timeout_seconds=0.1,
            ),
            CallableSource(
                name="binance-rest",
                priority=10,
                supports=lambda _key: True,
                fetcher=rest_fetch,
                timeout_seconds=2.0,
            ),
        ],
        policy_resolver=policy_resolver,
        max_concurrency=6,
    )
    return SyncSnapshotBuilder(builder)


def build_cycle_snapshot(provider: SyncSnapshotBuilder, symbols: list[str]):
    keys = [ACCOUNT]
    for symbol in symbols:
        keys.extend([price_key(symbol), position_key(symbol), open_orders_key(symbol)])
    return provider.build(
        keys,
        strict=False,
        deadline_seconds=3.0,
        metadata={"cycle_started_at": time.time()},
    )
