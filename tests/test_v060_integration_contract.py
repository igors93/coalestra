from __future__ import annotations

import asyncio

from coalestra import (
    CallableSource,
    ResourceAcceptanceRule,
    ResourceKey,
    SnapshotAcceptancePolicy,
    SnapshotBuilder,
    SnapshotRequest,
    require_capabilities,
)

ACCOUNT = ResourceKey("account", "state")
POSITION = ResourceKey("account", "position", "BTCUSDT")


def test_v060_async_integration_contract_smoke() -> None:
    async def scenario() -> None:
        require_capabilities(
            features=(
                "snapshot_acceptance",
                "transactional_revalidation",
                "builder_health_serialization",
                "blocking_source_timeout_guarantees",
            ),
            schemas={"builder_health": 1, "error_diagnostics": 1},
        )

        local_values = {
            ACCOUNT: {"availableBalance": "100"},
            POSITION: {"positionAmt": "0"},
        }
        source = CallableSource(
            name="local-reconciled",
            priority=100,
            supports=lambda key: key in local_values,
            fetcher=lambda key, _context: local_values[key],
            run_sync_in_thread=False,
            blocking_io=False,
        )
        builder = SnapshotBuilder([source])
        try:
            request = SnapshotRequest(
                required=(ACCOUNT, POSITION),
                acceptance_policy=SnapshotAcceptancePolicy(
                    default_rule=ResourceAcceptanceRule(
                        max_age_seconds=1.0,
                        allow_stale=False,
                    )
                ),
            )
            snapshot = await builder.build_request(request, deadline_seconds=1.0)
            health = await builder.health_snapshot()
            payload = health.to_dict()

            assert snapshot.complete is True
            assert snapshot.value(POSITION)["positionAmt"] == "0"
            assert payload["schema"] == "coalestra.builder-health"
            assert payload["assessment"]["severity"] == "healthy"
            assert health.undeclared_source_timeout_count == 0
            assert health.unsafe_blocking_source_count == 0
        finally:
            await builder.aclose()

    asyncio.run(scenario())
