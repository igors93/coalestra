from __future__ import annotations

import asyncio
import threading

import pytest

from coalestra import (
    AsyncMemoryCache,
    CallableSource,
    ResourceKey,
    ResourceUpdate,
    SnapshotBuilder,
    SubmissionBacklogFullError,
    SyncSnapshotBuilder,
)

KEY_A = ResourceKey("test", "value", "A")
KEY_B = ResourceKey("test", "value", "B")
KEY_C = ResourceKey("test", "value", "C")


class BlockingMemoryCache(AsyncMemoryCache):
    def __init__(self, *, expected_starts: int = 1) -> None:
        super().__init__()
        self.expected_starts = expected_starts
        self.started = threading.Event()
        self.release = threading.Event()
        self._started_count = 0

    async def set_many_if_newer(
        self,
        values,
        *,
        force: bool = False,
        replace_equal: bool = False,
    ):
        self._started_count += 1
        if self._started_count >= self.expected_starts:
            self.started.set()
        await asyncio.to_thread(self.release.wait)
        return await super().set_many_if_newer(
            values,
            force=force,
            replace_equal=replace_equal,
        )


class FailingMemoryCache(AsyncMemoryCache):
    async def set_many_if_newer(
        self,
        values,
        *,
        force: bool = False,
        replace_equal: bool = False,
    ):
        raise RuntimeError("cache write failed")


def make_builder(*, cache=None) -> SnapshotBuilder:
    return SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda _key, _context: "source-value",
            )
        ],
        cache=cache,
    )


def test_non_blocking_submission_backlog_is_bounded() -> None:
    cache = BlockingMemoryCache(expected_starts=2)
    sync_builder = SyncSnapshotBuilder(
        make_builder(cache=cache),
        max_pending_submissions=2,
    )

    try:
        first = sync_builder.publisher.submit_publish(KEY_A, "A", source="stream")
        second = sync_builder.publisher.submit_publish(KEY_B, "B", source="stream")

        assert cache.started.wait(timeout=1.0)
        assert sync_builder.pending_submissions == 2
        assert sync_builder.publisher.pending_submissions == 2

        with pytest.raises(SubmissionBacklogFullError) as captured:
            sync_builder.publisher.submit_publish(KEY_C, "C", source="stream")

        assert captured.value.limit == 2
        assert captured.value.pending == 2

        cache.release.set()
        assert first.result(timeout=1.0).published is True
        assert second.result(timeout=1.0).published is True
        sync_builder.flush_submissions(timeout_seconds=1.0)
        assert sync_builder.pending_submissions == 0
    finally:
        cache.release.set()
        sync_builder.close()


def test_bulk_publication_and_invalidations_can_be_submitted() -> None:
    sync_builder = SyncSnapshotBuilder(make_builder())

    try:
        publication = sync_builder.publisher.submit_publish_many(
            (
                ResourceUpdate(KEY_A, "A", source="stream"),
                ResourceUpdate(KEY_B, "B", source="stream"),
            )
        )
        results = publication.result(timeout=1.0)

        assert results[KEY_A].published is True
        assert results[KEY_B].published is True
        assert sync_builder.build([KEY_A, KEY_B]).value(KEY_A, str) == "A"

        single = sync_builder.publisher.submit_invalidate(KEY_A, reason="test")
        bulk = sync_builder.publisher.submit_invalidate_many(
            (KEY_B, KEY_C),
            reason="test",
        )

        assert single.result(timeout=1.0) is None
        assert bulk.result(timeout=1.0) is None
        sync_builder.publisher.flush(timeout_seconds=1.0)

        refreshed = sync_builder.build([KEY_A, KEY_B])
        assert refreshed.value(KEY_A, str) == "source-value"
        assert refreshed.value(KEY_B, str) == "source-value"
    finally:
        sync_builder.close()


def test_submit_publish_update_is_non_blocking() -> None:
    cache = BlockingMemoryCache()
    sync_builder = SyncSnapshotBuilder(make_builder(cache=cache))

    try:
        future = sync_builder.publisher.submit_publish_update(
            ResourceUpdate(KEY_A, "A", source="stream")
        )

        assert cache.started.wait(timeout=1.0)
        assert future.done() is False

        cache.release.set()
        assert future.result(timeout=1.0).published is True
    finally:
        cache.release.set()
        sync_builder.close()


def test_flush_waits_for_operations_accepted_before_the_call() -> None:
    cache = BlockingMemoryCache()
    sync_builder = SyncSnapshotBuilder(make_builder(cache=cache))
    release = threading.Timer(0.05, cache.release.set)

    try:
        future = sync_builder.publisher.submit_publish(KEY_A, "A", source="stream")
        assert cache.started.wait(timeout=1.0)

        release.start()
        sync_builder.flush_submissions(timeout_seconds=1.0)

        assert future.done() is True
        assert sync_builder.pending_submissions == 0
    finally:
        release.cancel()
        cache.release.set()
        sync_builder.close()


def test_flush_reports_timeout_without_cancelling_the_submission() -> None:
    cache = BlockingMemoryCache()
    sync_builder = SyncSnapshotBuilder(make_builder(cache=cache))

    try:
        future = sync_builder.publisher.submit_publish(KEY_A, "A", source="stream")
        assert cache.started.wait(timeout=1.0)

        with pytest.raises(TimeoutError, match="Timed out waiting"):
            sync_builder.flush_submissions(timeout_seconds=0.01)

        assert future.cancelled() is False
        cache.release.set()
        assert future.result(timeout=1.0).published is True
    finally:
        cache.release.set()
        sync_builder.close()


def test_submission_failures_remain_available_on_returned_future() -> None:
    sync_builder = SyncSnapshotBuilder(make_builder(cache=FailingMemoryCache()))

    try:
        future = sync_builder.publisher.submit_publish(KEY_A, "A", source="stream")

        with pytest.raises(RuntimeError, match="cache write failed"):
            future.result(timeout=1.0)

        sync_builder.flush_submissions(timeout_seconds=1.0)
        assert sync_builder.pending_submissions == 0
    finally:
        sync_builder.close()


def test_close_drains_accepted_submissions_before_stopping_the_loop() -> None:
    cache = BlockingMemoryCache()
    sync_builder = SyncSnapshotBuilder(
        make_builder(cache=cache),
        shutdown_timeout_seconds=1.0,
    )
    future = sync_builder.publisher.submit_publish(KEY_A, "A", source="stream")
    assert cache.started.wait(timeout=1.0)
    release = threading.Timer(0.05, cache.release.set)
    release.start()

    try:
        sync_builder.close()
    finally:
        release.cancel()
        cache.release.set()

    assert future.result(timeout=1.0).published is True
    assert sync_builder.closed is True


def test_max_pending_submissions_must_be_positive() -> None:
    builder = make_builder()

    with pytest.raises(ValueError, match="max_pending_submissions"):
        SyncSnapshotBuilder(builder, max_pending_submissions=0)

    asyncio.run(builder.aclose())


def test_close_cancels_submissions_that_exceed_shutdown_timeout() -> None:
    cache = BlockingMemoryCache()
    sync_builder = SyncSnapshotBuilder(
        make_builder(cache=cache),
        shutdown_timeout_seconds=0.02,
    )
    future = sync_builder.publisher.submit_publish(KEY_A, "A", source="stream")
    assert cache.started.wait(timeout=1.0)

    try:
        sync_builder.close()
        assert future.cancelled() is True
        assert sync_builder.closed is True
    finally:
        cache.release.set()


def test_backlog_limit_is_atomic_across_producer_threads() -> None:
    producer_count = 20
    cache = BlockingMemoryCache(expected_starts=3)
    sync_builder = SyncSnapshotBuilder(
        make_builder(cache=cache),
        max_pending_submissions=3,
    )
    barrier = threading.Barrier(producer_count)
    result_lock = threading.Lock()
    accepted = []
    rejected: list[SubmissionBacklogFullError] = []

    def submit(index: int) -> None:
        barrier.wait()
        try:
            future = sync_builder.publisher.submit_publish(
                ResourceKey("test", "threaded", str(index)),
                index,
                source="stream",
            )
        except SubmissionBacklogFullError as error:
            with result_lock:
                rejected.append(error)
        else:
            with result_lock:
                accepted.append(future)

    threads = [threading.Thread(target=submit, args=(index,)) for index in range(producer_count)]

    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=1.0)

        assert all(not thread.is_alive() for thread in threads)
        assert len(accepted) == 3
        assert len(rejected) == producer_count - 3
        assert sync_builder.pending_submissions == 3

        cache.release.set()
        for future in accepted:
            assert future.result(timeout=1.0).published is True
    finally:
        cache.release.set()
        sync_builder.close()


def test_non_blocking_publication_captures_mutable_input_before_return(monkeypatch) -> None:
    sync_builder = SyncSnapshotBuilder(make_builder())
    publisher = sync_builder.publisher._publisher
    original_publish_update = publisher.publish_update
    release = threading.Event()

    async def delayed_publish_update(update, *, force=False, replace_equal=False):
        await asyncio.to_thread(release.wait)
        return await original_publish_update(
            update,
            force=force,
            replace_equal=replace_equal,
        )

    monkeypatch.setattr(publisher, "publish_update", delayed_publish_update)
    payload = {"positions": [1]}
    metadata = {"labels": ["original"]}

    try:
        future = sync_builder.publisher.submit_publish(
            KEY_A,
            payload,
            source="stream",
            metadata=metadata,
        )
        payload["positions"].append(2)
        metadata["labels"].append("mutated")
        release.set()

        result = future.result(timeout=1.0)
        assert result.value.value == {"positions": [1]}
        assert result.value.metadata["labels"] == ["original"]
    finally:
        release.set()
        sync_builder.close()
