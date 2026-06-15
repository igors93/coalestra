from __future__ import annotations

import asyncio

from coalestra import (
    CallableSource,
    FreshnessPolicy,
    RefreshMode,
    ResourceKey,
    SnapshotBuilder,
    SourcePayload,
)

KEY = ResourceKey("test", "value")


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def now(self) -> float:
        return self.value

    def monotonic(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def test_stale_while_revalidate_returns_immediately_and_refreshes_cache() -> None:
    async def scenario() -> None:
        clock = FakeClock()
        calls = 0

        async def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return SourcePayload(value=calls, observed_at=clock.now())

        builder = SnapshotBuilder(
            [CallableSource(name="source", priority=1, supports=lambda _key: True, fetcher=fetch)],
            clock=clock,
            default_policy=FreshnessPolicy(
                1.0,
                10.0,
                refresh_mode=RefreshMode.STALE_WHILE_REVALIDATE,
            ),
        )
        first = await builder.build([KEY])
        clock.advance(2.0)
        stale = await builder.build([KEY])

        assert first.value(KEY, int) == 1
        assert stale.value(KEY, int) == 1
        assert stale[KEY].stale is True
        assert stale[KEY].metadata["refresh_scheduled"] is True
        assert stale.diagnostics.refresh_scheduled == 1

        await builder.wait_for_refreshes()
        refreshed = await builder.build([KEY])
        assert refreshed.value(KEY, int) == 2
        assert calls == 2

    asyncio.run(scenario())


def test_refresh_ahead_keeps_fresh_value_and_updates_in_background() -> None:
    async def scenario() -> None:
        clock = FakeClock()
        calls = 0

        async def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return SourcePayload(value=calls, observed_at=clock.now())

        builder = SnapshotBuilder(
            [CallableSource(name="source", priority=1, supports=lambda _key: True, fetcher=fetch)],
            clock=clock,
            default_policy=FreshnessPolicy(
                10.0,
                20.0,
                refresh_mode=RefreshMode.REFRESH_AHEAD,
                refresh_ahead_seconds=3.0,
            ),
        )
        await builder.build([KEY])
        clock.advance(8.0)
        cached = await builder.build([KEY])

        assert cached.value(KEY, int) == 1
        assert cached[KEY].stale is False
        assert cached[KEY].metadata["refresh_scheduled"] is True

        await builder.wait_for_refreshes()
        refreshed = await builder.build([KEY])
        assert refreshed.value(KEY, int) == 2
        assert calls == 2

    asyncio.run(scenario())


def test_failed_background_refresh_does_not_replace_cache_with_older_stale_value() -> None:
    async def scenario() -> None:
        clock = FakeClock()

        async def fetch(_key, _context):
            return SourcePayload(value="older", observed_at=90.0)

        builder = SnapshotBuilder(
            [CallableSource(name="source", priority=1, supports=lambda _key: True, fetcher=fetch)],
            clock=clock,
            default_policy=FreshnessPolicy(
                1.0,
                20.0,
                refresh_mode=RefreshMode.STALE_WHILE_REVALIDATE,
            ),
        )
        await builder.publisher.publish(
            KEY,
            "newer",
            source="stream",
            observed_at=100.0,
        )
        clock.advance(2.0)
        stale = await builder.build([KEY])
        assert stale.value(KEY, str) == "newer"
        await builder.wait_for_refreshes()

        lookup = await builder.cache.get(
            KEY,
            now=clock.now(),
            policy=FreshnessPolicy(1.0, 20.0),
        )
        assert lookup.value is not None
        assert lookup.value.value == "newer"
        assert lookup.value.observed_at == 100.0

    asyncio.run(scenario())
