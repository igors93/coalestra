from __future__ import annotations

import asyncio
import threading

from coalestra import (
    CallableSource,
    ResourceKey,
    SnapshotBuilder,
    SourceUnavailableError,
    SyncSnapshotBuilder,
)
from coalestra.resilience import RetryPolicy

KEY = ResourceKey("health", "resource", "A")


def run(coro):
    return asyncio.run(coro)


async def wait_until(predicate, *, attempts: int = 200) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition was not reached")


def test_health_reports_active_dispatch_workers_and_capacity_waiters() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        keys = tuple(ResourceKey("health", "resource", str(index)) for index in range(8))

        async def fetch(_key, _context):
            started.set()
            await release.wait()
            return "ok"

        source = CallableSource(
            name="source",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            max_concurrency=1,
        )
        builder = SnapshotBuilder(
            [source],
            max_concurrency=4,
            max_pending_tasks=4,
        )
        task = asyncio.create_task(builder.build(keys))
        await started.wait()

        health = await builder.health_snapshot()
        for _ in range(200):
            if health.active_dispatch_workers == 4 and health.waiting_for_capacity >= 3:
                break
            await asyncio.sleep(0)
            health = await builder.health_snapshot()

        assert health.active_dispatch_workers == 4
        assert health.waiting_for_capacity >= 3

        release.set()
        await task
        settled = await builder.health_snapshot()
        assert settled.active_dispatch_workers == 0
        assert settled.waiting_for_capacity == 0
        await builder.aclose()

    run(scenario())


def test_health_counts_queue_source_and_deadline_timeouts() -> None:
    async def scenario() -> None:
        async def fast(_key, _context):
            return "ok"

        queue_source = CallableSource(
            name="queue-source",
            priority=1,
            supports=lambda _key: True,
            fetcher=fast,
            timeout_seconds=1.0,
            queue_timeout_seconds=0.01,
            max_concurrency=1,
        )
        queue_builder = SnapshotBuilder(
            [queue_source],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        lease = await queue_builder.capacity.acquire(queue_source.name)
        try:
            await queue_builder.build([KEY], strict=False)
        finally:
            lease.release()
        assert (await queue_builder.health_snapshot()).queue_timeout_count == 1
        await queue_builder.aclose()

        async def slow(_key, _context):
            await asyncio.sleep(0.05)
            return "late"

        timeout_source = CallableSource(
            name="timeout-source",
            priority=1,
            supports=lambda _key: True,
            fetcher=slow,
            timeout_seconds=0.01,
        )
        timeout_builder = SnapshotBuilder(
            [timeout_source],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        await timeout_builder.build([KEY], strict=False)
        assert (await timeout_builder.health_snapshot()).source_timeout_count == 1
        await timeout_builder.aclose()

        deadline_source = CallableSource(
            name="deadline-source",
            priority=1,
            supports=lambda _key: True,
            fetcher=fast,
            timeout_seconds=1.0,
            queue_timeout_seconds=1.0,
            max_concurrency=1,
        )
        deadline_builder = SnapshotBuilder(
            [deadline_source],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        lease = await deadline_builder.capacity.acquire(deadline_source.name)
        try:
            await deadline_builder.build([KEY], strict=False, deadline_seconds=0.01)
        finally:
            lease.release()
        assert (await deadline_builder.health_snapshot()).deadline_exceeded_count == 1
        await deadline_builder.aclose()

    run(scenario())


def test_health_counts_revalidation_attempts_and_failures() -> None:
    async def scenario() -> None:
        state = {"fail": False, "value": 1}

        async def fetch(_key, _context):
            if state["fail"]:
                raise SourceUnavailableError("temporary failure")
            return state["value"]

        builder = SnapshotBuilder(
            [CallableSource(name="source", priority=1, supports=lambda _key: True, fetcher=fetch)],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        session = builder.session()
        await session.resolve([KEY])

        state["fail"] = True
        await session.revalidate([KEY], strict=False, force_refresh=True)
        failed = await builder.health_snapshot()
        assert failed.revalidation_attempt_count == 1
        assert failed.revalidation_failure_count == 1

        state["fail"] = False
        state["value"] = 2
        await session.revalidate([KEY], force_refresh=True)
        recovered = await builder.health_snapshot()
        assert recovered.revalidation_attempt_count == 2
        assert recovered.revalidation_failure_count == 1

        await session.close()
        await builder.aclose()

    run(scenario())


def test_sync_health_includes_submission_backlog() -> None:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source", priority=1, supports=lambda _key: True, fetcher=lambda *_: 1
            )
        ]
    )
    started = threading.Event()
    release = threading.Event()
    original = builder.publisher.publish_update

    async def blocked_publish(*args, **kwargs):
        started.set()
        await asyncio.to_thread(release.wait)
        return await original(*args, **kwargs)

    builder.publisher.publish_update = blocked_publish  # type: ignore[method-assign]
    sync = SyncSnapshotBuilder(builder, max_pending_submissions=2)
    try:
        future = sync.publisher.submit_publish(KEY, {"value": 1}, source="stream")
        assert started.wait(timeout=1.0)
        health = sync.health_snapshot()
        assert health.pending_submissions == 1
        assert health.max_pending_submissions == 2

        release.set()
        future.result(timeout=1.0)
        assert sync.health_snapshot().pending_submissions == 0
    finally:
        release.set()
        sync.close()
