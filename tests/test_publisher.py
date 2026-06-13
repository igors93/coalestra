from __future__ import annotations

import asyncio

from coalestra import (
    CallableSource,
    PublishStatus,
    ResourceKey,
    ResourceUpdate,
    SnapshotBuilder,
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
