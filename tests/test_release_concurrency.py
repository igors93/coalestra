from __future__ import annotations

import asyncio
import threading

import pytest

from coalestra import (
    AsyncMemoryCache,
    CallableSource,
    ResourceKey,
    SnapshotBuilder,
    SourceAuthorityPolicy,
    SyncSnapshotBuilder,
)
from coalestra.concurrency import CapacityController

pytestmark = pytest.mark.release_concurrency

POSITION = ResourceKey("account", "position", "BTCUSDT")


def run(coro):
    return asyncio.run(coro)


async def _wait_for_partial_capacity_acquisition(
    controller: CapacityController,
) -> None:
    while True:
        snapshot = await controller.snapshot()
        if snapshot["rest"].in_use == 1 and snapshot["__global__"].waiting == 1:
            return
        await asyncio.sleep(0)


def test_concurrent_high_authority_publication_supersedes_inflight_source() -> None:
    async def scenario() -> None:
        source_started = asyncio.Event()
        release_source = asyncio.Event()

        async def fetch_position(_key, _context):
            source_started.set()
            await release_source.wait()
            return "rest-position"

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="rest",
                    priority=1,
                    supports=lambda key: key == POSITION,
                    fetcher=fetch_position,
                )
            ],
            authority_policy=SourceAuthorityPolicy(
                source_ranks={
                    "reconciled-local": 300,
                    "rest": 100,
                }
            ),
        )

        try:
            build_task = asyncio.create_task(builder.build([POSITION]))
            await asyncio.wait_for(source_started.wait(), timeout=1.0)

            await builder.publisher.publish(
                POSITION,
                "local-position",
                source="reconciled-local",
            )
            release_source.set()

            snapshot = await asyncio.wait_for(build_task, timeout=1.0)
            assert snapshot.value(POSITION, str) == "local-position"
            assert snapshot[POSITION].source == "reconciled-local"
        finally:
            release_source.set()
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


def test_cancellation_while_waiting_for_capacity_releases_partial_acquisition() -> None:
    async def scenario() -> None:
        controller = CapacityController(
            global_limit=1,
            source_limits={"rest": 1},
        )
        global_lease = await controller.acquire("unlimited-source")
        waiting_task = asyncio.create_task(controller.acquire("rest"))

        try:
            await asyncio.wait_for(
                _wait_for_partial_capacity_acquisition(controller),
                timeout=1.0,
            )

            waiting_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting_task

            snapshot = await controller.snapshot()
            assert snapshot["rest"].in_use == 0
            assert snapshot["rest"].waiting == 0
            assert snapshot["__global__"].in_use == 1
            assert snapshot["__global__"].waiting == 0
        finally:
            global_lease.release()

    run(scenario())


def test_session_close_waits_for_transactional_revalidation() -> None:
    async def scenario() -> None:
        calls = 0
        revalidation_started = asyncio.Event()
        release_revalidation = asyncio.Event()

        async def fetch_position(_key, _context):
            nonlocal calls
            calls += 1
            if calls == 1:
                return "initial"
            revalidation_started.set()
            await release_revalidation.wait()
            return "updated"

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="position-source",
                    priority=1,
                    supports=lambda key: key == POSITION,
                    fetcher=fetch_position,
                )
            ]
        )
        session = builder.session()

        try:
            initial = await session.resolve([POSITION])
            assert initial.value(POSITION, str) == "initial"

            revalidation = asyncio.create_task(session.revalidate([POSITION], force_refresh=True))
            await asyncio.wait_for(revalidation_started.wait(), timeout=1.0)

            close_task = asyncio.create_task(session.close())
            await asyncio.sleep(0)
            assert close_task.done() is False

            release_revalidation.set()
            updated = await asyncio.wait_for(revalidation, timeout=1.0)
            await asyncio.wait_for(close_task, timeout=1.0)

            assert updated.value(POSITION, str) == "updated"
            assert session.closed is True
        finally:
            release_revalidation.set()
            await session.close()
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


class _BlockingMemoryCache(AsyncMemoryCache):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    async def set_many_if_newer(
        self,
        values,
        *,
        force: bool = False,
        replace_equal: bool = False,
    ):
        self.started.set()
        await asyncio.to_thread(self.release.wait)
        return await super().set_many_if_newer(
            values,
            force=force,
            replace_equal=replace_equal,
        )


def test_sync_shutdown_drains_accepted_submission_without_timer() -> None:
    cache = _BlockingMemoryCache()
    builder = SnapshotBuilder(
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
    sync_builder = SyncSnapshotBuilder(
        builder,
        shutdown_timeout_seconds=1.0,
    )
    close_started = threading.Event()
    close_finished = threading.Event()

    def close_builder() -> None:
        close_started.set()
        sync_builder.close()
        close_finished.set()

    future = sync_builder.publisher.submit_publish(
        POSITION,
        "published",
        source="stream",
    )
    assert cache.started.wait(timeout=1.0)

    closer = threading.Thread(target=close_builder)
    closer.start()
    assert close_started.wait(timeout=1.0)
    assert closer.is_alive()
    assert close_finished.is_set() is False

    cache.release.set()
    closer.join(timeout=1.0)

    assert closer.is_alive() is False
    assert close_finished.is_set() is True
    assert future.result(timeout=1.0).published is True
    assert sync_builder.closed is True
