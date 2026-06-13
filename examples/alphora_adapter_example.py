"""Illustrative integration. This file deliberately does not import Alphora."""

from __future__ import annotations

from typing import Any

from coalestra import (
    CallableBatchSource,
    CallableDerivedSource,
    CallableSource,
    FreshnessPolicy,
    PolicyResolver,
    ResourceKey,
    SnapshotBuilder,
    SnapshotRequest,
    SourcePayload,
    SourceUnavailableError,
    SyncSnapshotBuilder,
)

ACCOUNT = ResourceKey("account", "summary")
ALL_POSITIONS = ResourceKey("account", "positions")
EXCHANGE_INFO = ResourceKey("exchange", "info")


def market_state(symbol: str) -> ResourceKey:
    return ResourceKey("market", "state", symbol)


def position(symbol: str) -> ResourceKey:
    return ResourceKey("account", "position", symbol)


def exchange_rules(symbol: str) -> ResourceKey:
    return ResourceKey("exchange", "rules", symbol)


def build_alphora_snapshot_provider(
    *,
    market_data_hub: Any,
    binance_client: Any,
    extract_position: Any,
    extract_rules: Any,
) -> SyncSnapshotBuilder:
    def market_batch_fetch(keys, _context):
        resolved = {}
        for key in keys:
            snapshot = market_data_hub.latest_price(key.subject)
            if snapshot is not None:
                resolved[key] = SourcePayload(
                    value=snapshot,
                    observed_at=snapshot.received_at,
                    metadata={"transport": "market_websocket"},
                )
        return resolved

    def rest_fetch(key: ResourceKey, _context):
        if key == ACCOUNT:
            return binance_client.account_info()
        if key == ALL_POSITIONS:
            return binance_client.all_positions()
        if key == EXCHANGE_INFO:
            return binance_client.exchange_info()
        raise SourceUnavailableError(f"REST adapter does not support {key}")

    position_view = CallableDerivedSource(
        name="position-view",
        priority=100,
        supports=lambda key: key.namespace == "account" and key.name == "position",
        dependencies=lambda _key: (ALL_POSITIONS,),
        deriver=lambda key, snapshot, _context: extract_position(
            snapshot.value(ALL_POSITIONS, list), key.subject
        ),
    )
    rules_view = CallableDerivedSource(
        name="rules-view",
        priority=100,
        supports=lambda key: key.namespace == "exchange" and key.name == "rules",
        dependencies=lambda _key: (EXCHANGE_INFO,),
        deriver=lambda key, snapshot, _context: extract_rules(
            snapshot.value(EXCHANGE_INFO, dict), key.subject
        ),
    )

    policy_resolver = PolicyResolver(
        default=FreshnessPolicy(5.0, 15.0),
        dynamic=lambda key: (
            FreshnessPolicy(2.0, 5.0) if key.namespace == "market" else FreshnessPolicy(5.0, 15.0)
        ),
    )

    builder = SnapshotBuilder(
        sources=[
            position_view,
            rules_view,
            CallableBatchSource(
                name="market-stream",
                priority=90,
                supports=lambda key: key.namespace == "market" and key.name == "state",
                fetcher=market_batch_fetch,
                timeout_seconds=0.1,
                max_batch_size=100,
                run_sync_in_thread=False,
            ),
            CallableSource(
                name="binance-rest",
                priority=10,
                supports=lambda key: key in {ACCOUNT, ALL_POSITIONS, EXCHANGE_INFO},
                fetcher=rest_fetch,
                timeout_seconds=2.0,
            ),
        ],
        policy_resolver=policy_resolver,
        max_concurrency=6,
        source_concurrency={"binance-rest": 4},
        manage_lifecycle=True,
    )
    return SyncSnapshotBuilder(builder)


def build_cycle_snapshot(
    provider: SyncSnapshotBuilder,
    configured_symbols: list[str],
    selected_symbols: list[str],
    cycle_id: str,
):
    with provider.session(
        snapshot_id=cycle_id,
        deadline_seconds=3.0,
        metadata={"cycle_id": cycle_id},
    ) as session:
        baseline = SnapshotRequest(
            required=[ACCOUNT, ALL_POSITIONS],
            optional=[market_state(symbol) for symbol in configured_symbols],
        )
        session.resolve_request(baseline)

        heavy = SnapshotRequest(
            required=[
                EXCHANGE_INFO,
                *(position(symbol) for symbol in selected_symbols),
                *(exchange_rules(symbol) for symbol in selected_symbols),
            ]
        )
        return session.resolve_request(heavy)
