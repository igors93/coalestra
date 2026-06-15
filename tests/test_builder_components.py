from __future__ import annotations

import asyncio

from coalestra import CallableBatchSource, CallableSource, ResourceKey, SnapshotBuilder
from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.orchestration.cache_access import CacheAccess
from coalestra.orchestration.lifecycle import close_components
from coalestra.orchestration.refresh import RefreshManager
from coalestra.orchestration.source_calls import SourceCalls
from coalestra.orchestration.source_catalog import SourceCatalog
from coalestra.orchestration.source_executor import SourceExecutor

KEY = ResourceKey("components", "value")


def test_builder_delegates_to_focused_components_without_changing_public_behavior() -> None:
    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda *_: 7,
                )
            ]
        )

        assert isinstance(builder._source_catalog, SourceCatalog)
        assert isinstance(builder._cache_access, CacheAccess)
        assert isinstance(builder._source_calls, SourceCalls)
        assert isinstance(builder._source_executor, SourceExecutor)
        assert isinstance(builder._refresh_manager, RefreshManager)

        snapshot = await builder.build([KEY])
        assert snapshot.value(KEY, int) == 7

    asyncio.run(scenario())


def test_source_catalog_preserves_priority_kind_and_support_cache() -> None:
    async def batch_fetch(keys, _context):
        return {key: key.subject for key in keys}

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="low",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda *_: 1,
            ),
            CallableBatchSource(
                name="high",
                priority=10,
                supports=lambda _key: True,
                fetcher=batch_fetch,
            ),
        ]
    )
    catalog = builder._source_catalog
    diagnostics = DiagnosticsCollector(started_monotonic=builder.clock.monotonic())

    assert [source.name for source in catalog.sources] == ["high", "low"]
    assert catalog.kind(catalog.sources[0]) == "batch"
    assert catalog.supports(catalog.sources[0], KEY, diagnostics) is True
    assert catalog.supports(catalog.sources[0], KEY, diagnostics) is True
    assert diagnostics.support_cache_misses == 1
    assert diagnostics.support_cache_hits == 1

    builder.clear_source_support_cache()
    assert catalog.support_cache_size == 0


def test_component_lifecycle_closes_shared_component_once() -> None:
    class SharedComponent:
        def __init__(self) -> None:
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    async def scenario() -> None:
        component = SharedComponent()
        await close_components((component, component, component))
        assert component.close_calls == 1

    asyncio.run(scenario())
