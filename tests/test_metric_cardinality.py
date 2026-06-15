from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from coalestra import CallableSource, ResourceKey, SnapshotBuilder
from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.errors import SourceUnavailableError
from coalestra.core.models import FetchContext
from coalestra.observability.labels import resource_metric_labels
from coalestra.orchestration.refresh import RefreshManager
from coalestra.orchestration.runtime import ResolutionResult
from coalestra.orchestration.singleflight import SingleFlight


@dataclass(frozen=True)
class RecordedMetric:
    name: str
    value: float
    labels: dict[str, str]


class RecordingMetrics:
    def __init__(self) -> None:
        self.records: list[RecordedMetric] = []

    def increment(self, metric: str, value: int = 1, **labels: str) -> None:
        self.records.append(RecordedMetric(metric, float(value), dict(labels)))

    def observe(self, metric: str, value: float, **labels: str) -> None:
        self.records.append(RecordedMetric(metric, float(value), dict(labels)))

    def matching(self, metric: str) -> list[RecordedMetric]:
        return [record for record in self.records if record.name == metric]


@dataclass(frozen=True)
class RecordedEvent:
    event_type: str
    payload: dict[str, Any]


class RecordingEvents:
    def __init__(self) -> None:
        self.records: list[RecordedEvent] = []

    def emit(self, event_type: str, **payload: Any) -> None:
        self.records.append(RecordedEvent(event_type, dict(payload)))


def run(coro):
    return asyncio.run(coro)


def assert_low_cardinality_resource_labels(
    records: list[RecordedMetric],
    *,
    namespace: str,
    name: str,
) -> None:
    assert records
    for record in records:
        assert record.labels["resource_namespace"] == namespace
        assert record.labels["resource_name"] == name
        assert "resource" not in record.labels
        assert "subject" not in record.labels
        assert "qualifiers" not in record.labels


def test_resource_metric_labels_exclude_subject_and_qualifier_values() -> None:
    btc = ResourceKey(
        "market",
        "price",
        "BTCUSDT",
        qualifiers={"venue": "spot", "account": "primary"},
    )
    eth = ResourceKey(
        "market",
        "price",
        "ETHUSDT",
        qualifiers={"venue": "futures", "account": "secondary"},
    )

    assert resource_metric_labels(btc) == {
        "resource_namespace": "market",
        "resource_name": "price",
    }
    assert resource_metric_labels(eth) == resource_metric_labels(btc)


def test_builder_and_publisher_metrics_group_resource_instances_by_type() -> None:
    async def scenario() -> None:
        metrics = RecordingMetrics()
        events = RecordingEvents()
        btc = ResourceKey("market", "price", "BTCUSDT", qualifiers={"venue": "spot"})
        eth = ResourceKey("market", "price", "ETHUSDT", qualifiers={"venue": "futures"})

        source = CallableSource(
            name="market-source",
            priority=1,
            supports=lambda key: key.namespace == "market" and key.name == "price",
            fetcher=lambda key, _context: {"subject": key.subject},
            run_sync_in_thread=False,
        )
        builder = SnapshotBuilder(
            [source],
            metrics=metrics,
            events=events,
        )

        try:
            await builder.build((btc, eth))
            await builder.publisher.publish(
                btc,
                {"price": "65000"},
                source="user-data-stream",
                force=True,
            )
            await builder.publisher.publish(
                eth,
                {"price": "3500"},
                source="user-data-stream",
                force=True,
            )
            await builder.publisher.invalidate_many((btc, eth), reason="test")
        finally:
            await builder.aclose(cancel_refreshes=True)

        assert_low_cardinality_resource_labels(
            metrics.matching("cache_access_total"),
            namespace="market",
            name="price",
        )
        assert_low_cardinality_resource_labels(
            metrics.matching("resource_publish_total"),
            namespace="market",
            name="price",
        )
        assert_low_cardinality_resource_labels(
            metrics.matching("resource_invalidation_total"),
            namespace="market",
            name="price",
        )

        dynamic_values = {"BTCUSDT", "ETHUSDT", "spot", "futures"}
        for record in metrics.records:
            assert dynamic_values.isdisjoint(record.labels.values())

        event_resources = {
            str(record.payload["resource"])
            for record in events.records
            if "resource" in record.payload
        }
        assert str(btc) in event_resources
        assert str(eth) in event_resources

    run(scenario())


def test_singleflight_join_metric_uses_resource_type_labels() -> None:
    async def scenario() -> None:
        metrics = RecordingMetrics()
        started = asyncio.Event()
        release = asyncio.Event()
        key = ResourceKey("account", "position", "BTCUSDT", qualifiers={"side": "long"})

        async def fetch(_key, _context):
            started.set()
            await release.wait()
            return {"quantity": 1}

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="positions",
                    priority=1,
                    supports=lambda candidate: candidate == key,
                    fetcher=fetch,
                )
            ],
            metrics=metrics,
        )

        try:
            first = asyncio.create_task(builder.build((key,)))
            await started.wait()
            second = asyncio.create_task(builder.build((key,)))
            await asyncio.sleep(0)
            release.set()
            await asyncio.gather(first, second)
        finally:
            await builder.aclose(cancel_refreshes=True)

        assert_low_cardinality_resource_labels(
            metrics.matching("singleflight_join_total"),
            namespace="account",
            name="position",
        )

    run(scenario())


def test_background_refresh_metrics_use_resource_type_labels() -> None:
    async def scenario() -> None:
        metrics = RecordingMetrics()
        events = RecordingEvents()
        key = ResourceKey(
            "account",
            "open_orders",
            "BTCUSDT",
            qualifiers={"account": "primary"},
        )

        async def resolve_owned(
            keys,
            *,
            context,
            runtime,
            ancestry,
            local_owned,
        ):
            del context, runtime, ancestry, local_owned
            return {
                requested: ResolutionResult(
                    error=SourceUnavailableError("synthetic refresh failure")
                )
                for requested in keys
            }

        manager = RefreshManager(
            clock=type(
                "Clock",
                (),
                {
                    "now": staticmethod(time.time),
                    "monotonic": staticmethod(time.monotonic),
                },
            )(),
            single_flight=SingleFlight(),
            resolve_owned=resolve_owned,
            metrics=metrics,
            events=events,
        )
        diagnostics = DiagnosticsCollector(started_monotonic=time.monotonic())
        context = FetchContext(
            requested_at=time.time(),
            snapshot_id="refresh-test",
        )

        assert manager.schedule(
            key,
            parent_context=context,
            diagnostics=diagnostics,
            reason="test",
        )
        await manager.wait()
        await manager.close()

        assert_low_cardinality_resource_labels(
            metrics.matching("resource_refresh_total"),
            namespace="account",
            name="open_orders",
        )
        assert {
            record.labels["status"] for record in metrics.matching("resource_refresh_total")
        } == {
            "scheduled",
            "failure",
        }
        assert any(record.payload.get("resource") == str(key) for record in events.records)

    run(scenario())
