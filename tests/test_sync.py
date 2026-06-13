from __future__ import annotations

from coalestra import CallableSource, ResourceKey, SnapshotBuilder, SyncSnapshotBuilder

KEY = ResourceKey("test", "value")


def test_sync_facade_builds_snapshot_and_reuses_cache() -> None:
    calls = 0

    def fetch(_key, _context):
        nonlocal calls
        calls += 1
        return 99

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ]
    )

    with SyncSnapshotBuilder(builder) as sync_builder:
        first = sync_builder.build([KEY])
        second = sync_builder.build([KEY])

    assert first.value(KEY, int) == 99
    assert second.value(KEY, int) == 99
    assert second[KEY].from_cache is True
    assert calls == 1


def test_sync_facade_rejects_calls_after_close() -> None:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda _key, _context: 99,
            )
        ]
    )
    sync_builder = SyncSnapshotBuilder(builder)
    sync_builder.close()

    try:
        sync_builder.build([KEY])
    except RuntimeError as error:
        assert "closed" in str(error)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("closed builder accepted a new call")


def test_sync_publisher_rejects_calls_after_builder_close() -> None:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda _key, _context: 99,
            )
        ]
    )
    sync_builder = SyncSnapshotBuilder(builder)
    sync_builder.close()

    try:
        sync_builder.publisher.publish(KEY, 100, source="stream")
    except RuntimeError as error:
        assert "closed" in str(error)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("closed publisher accepted a new call")
