from __future__ import annotations

import asyncio
import time

from coalestra import (
    AsyncMemoryCache,
    CallableSource,
    FreshnessPolicy,
    PublishStatus,
    ResourceKey,
    ResourceUpdate,
    SnapshotBuilder,
    SourcePayload,
    SyncSnapshotBuilder,
)

KEY = ResourceKey("stream", "state", "A")
KEY_B = ResourceKey("stream", "state", "B")


def test_published_value_is_reused_without_calling_a_source() -> None:
    calls = 0

    async def fetch(_key, _context):
        nonlocal calls
        calls += 1
        return "remote"

    async def scenario() -> str:
        builder = SnapshotBuilder(
            [CallableSource(name="remote", priority=1, supports=lambda _key: True, fetcher=fetch)]
        )
        result = await builder.publisher.publish(
            KEY,
            "stream-value",
            source="event-stream",
        )
        assert result.status is PublishStatus.PUBLISHED
        snapshot = await builder.build([KEY])
        assert snapshot[KEY].from_cache is True
        return snapshot.value(KEY, str)

    assert asyncio.run(scenario()) == "stream-value"
    assert calls == 0


def test_publication_is_monotonic_and_force_can_replace_newer_data() -> None:
    async def scenario() -> tuple[PublishStatus, PublishStatus, str]:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="remote",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: "remote",
                )
            ]
        )
        now = builder.clock.now()
        await builder.publisher.publish(KEY, "new", source="stream", observed_at=now)
        older = await builder.publisher.publish(KEY, "old", source="stream", observed_at=now - 1.0)
        duplicate = await builder.publisher.publish(
            KEY,
            "duplicate",
            source="stream",
            observed_at=now,
        )
        forced = await builder.publisher.publish(
            KEY,
            "forced",
            source="repair",
            observed_at=now - 0.1,
            force=True,
        )
        assert forced.published is True
        snapshot = await builder.build([KEY])
        return older.status, duplicate.status, snapshot.value(KEY, str)

    older, duplicate, value = asyncio.run(scenario())
    assert older is PublishStatus.IGNORED_OLDER
    assert duplicate is PublishStatus.IGNORED_DUPLICATE
    assert value == "forced"


def test_publish_many_and_invalidate_many() -> None:
    calls: list[ResourceKey] = []

    async def fetch(key, _context):
        calls.append(key)
        return f"remote:{key.subject}"

    async def scenario() -> tuple[str, str, str, str]:
        builder = SnapshotBuilder(
            [CallableSource(name="remote", priority=1, supports=lambda _key: True, fetcher=fetch)]
        )
        results = await builder.publisher.publish_many(
            [
                ResourceUpdate(KEY, "A1", source="stream"),
                ResourceUpdate(KEY_B, "B1", source="stream"),
            ]
        )
        assert all(result.published for result in results.values())
        cached = await builder.build([KEY, KEY_B])
        await builder.publisher.invalidate_many([KEY, KEY_B], reason="reconcile")
        remote = await builder.build([KEY, KEY_B])
        return (
            cached.value(KEY, str),
            cached.value(KEY_B, str),
            remote.value(KEY, str),
            remote.value(KEY_B, str),
        )

    values = asyncio.run(scenario())
    assert values == ("A1", "B1", "remote:A", "remote:B")
    assert calls == [KEY, KEY_B]


def test_session_values_remain_pinned_after_publication() -> None:
    async def scenario() -> tuple[str, str]:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="remote",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: "remote",
                )
            ]
        )
        first_publish = await builder.publisher.publish(KEY, "v1", source="stream")
        async with builder.session() as session:
            first = await session.resolve([KEY])
            await builder.publisher.publish(
                KEY,
                "v2",
                source="stream",
                observed_at=first_publish.value.observed_at + 1.0,
            )
            pinned = await session.resolve([KEY])
            assert first.value(KEY, str) == "v1"
            assert pinned.value(KEY, str) == "v1"
        fresh = await builder.build([KEY])
        return pinned.value(KEY, str), fresh.value(KEY, str)

    assert asyncio.run(scenario()) == ("v1", "v2")


def test_late_source_result_cannot_overwrite_newer_publication() -> None:
    async def scenario() -> tuple[str, str]:
        source_started = asyncio.Event()
        release_source = asyncio.Event()
        published_at = time.time()

        async def fetch(_key, _context):
            source_started.set()
            await release_source.wait()
            return SourcePayload(
                value="old-source",
                observed_at=published_at - 0.1,
            )

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="remote",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                )
            ],
            default_policy=FreshnessPolicy(60.0, 60.0),
        )

        build_task = asyncio.create_task(builder.build([KEY]))
        await source_started.wait()

        await builder.publisher.publish(
            KEY,
            "new-stream",
            source="stream",
            observed_at=published_at,
        )

        release_source.set()
        in_flight_snapshot = await build_task
        next_snapshot = await builder.build([KEY])
        return (
            in_flight_snapshot.value(KEY, str),
            next_snapshot.value(KEY, str),
        )

    assert asyncio.run(scenario()) == ("old-source", "new-stream")


def test_concurrent_publishers_report_authoritative_atomic_result() -> None:
    class CoordinatedCache(AsyncMemoryCache):
        def __init__(self) -> None:
            super().__init__()
            self._initial_reads = 0
            self._both_read = asyncio.Event()
            self._new_written = asyncio.Event()

        async def get_many(self, keys, *, now, policies):
            results = await super().get_many(keys, now=now, policies=policies)
            self._initial_reads += 1
            if self._initial_reads == 2:
                self._both_read.set()
            elif self._initial_reads == 1:
                await self._both_read.wait()
            return results

        async def set_many_if_newer(
            self,
            values,
            *,
            force=False,
            replace_equal=False,
        ):
            items = tuple(values)
            if items and items[0].value == "old":
                await self._new_written.wait()
            results = await super().set_many_if_newer(
                items,
                force=force,
                replace_equal=replace_equal,
            )
            if items and items[0].value == "new":
                self._new_written.set()
            return results

    async def scenario() -> tuple[PublishStatus, PublishStatus, str]:
        cache = CoordinatedCache()
        source = CallableSource(
            name="remote",
            priority=1,
            supports=lambda _key: True,
            fetcher=lambda _key, _context: "remote",
        )
        newer_builder = SnapshotBuilder([source], cache=cache)
        older_builder = SnapshotBuilder([source], cache=cache)
        now = time.time()

        newer_task = asyncio.create_task(
            newer_builder.publisher.publish(
                KEY,
                "new",
                source="new-stream",
                observed_at=now,
            )
        )
        older_task = asyncio.create_task(
            older_builder.publisher.publish(
                KEY,
                "old",
                source="old-stream",
                observed_at=now - 1.0,
            )
        )
        newer, older = await asyncio.gather(newer_task, older_task)
        snapshot = await newer_builder.build([KEY])
        return newer.status, older.status, snapshot.value(KEY, str)

    assert asyncio.run(scenario()) == (
        PublishStatus.PUBLISHED,
        PublishStatus.IGNORED_OLDER,
        "new",
    )


def test_sync_publisher_supports_blocking_and_non_blocking_updates() -> None:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="remote",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda _key, _context: "remote",
            )
        ]
    )

    with SyncSnapshotBuilder(builder) as sync_builder:
        first = sync_builder.publisher.publish(KEY, "v1", source="stream")
        future = sync_builder.publisher.submit_publish(
            KEY,
            "v2",
            source="stream",
            observed_at=first.value.observed_at + 1.0,
        )
        second = future.result(timeout=1.0)
        snapshot = sync_builder.build([KEY])

    assert first.published is True
    assert second.published is True
    assert snapshot.value(KEY, str) == "v2"
