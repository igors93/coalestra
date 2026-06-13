from __future__ import annotations

import asyncio

import pytest

from coalestra import (
    CallableSource,
    FreshnessPolicy,
    ResourceKey,
    SessionClosedError,
    SnapshotBuilder,
    SnapshotBuildError,
    SourceUnavailableError,
    SyncSnapshotBuilder,
)
from coalestra.resilience import RetryPolicy

KEY_A = ResourceKey("test", "value", "A")
KEY_B = ResourceKey("test", "value", "B")


def test_session_accumulates_stages_with_one_identity_and_pins_values() -> None:
    calls: dict[ResourceKey, int] = {}

    async def fetch(key, _context):
        calls[key] = calls.get(key, 0) + 1
        return f"{key.subject}-{calls[key]}"

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                )
            ],
            default_policy=FreshnessPolicy(0.0, 0.0),
        )
        async with builder.session(snapshot_id="cycle-1") as session:
            first = await session.resolve([KEY_A])
            second = await session.resolve([KEY_A, KEY_B])
            return first, second, session.snapshot()

    first, second, final = asyncio.run(scenario())

    assert first.snapshot_id == second.snapshot_id == final.snapshot_id == "cycle-1"
    assert first.created_at == second.created_at == final.created_at
    assert final.value(KEY_A) == "A-1"
    assert final.value(KEY_B) == "B-1"
    assert calls == {KEY_A: 1, KEY_B: 1}


def test_session_can_retry_a_previously_failed_resource() -> None:
    available = False

    async def fetch(_key, _context):
        if not available:
            raise SourceUnavailableError("not ready")
        return 99

    async def scenario():
        nonlocal available
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                )
            ],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        async with builder.session() as session:
            partial = await session.resolve([KEY_A], strict=False)
            available = True
            recovered = await session.resolve([KEY_A], retry_errors=True)
            return partial, recovered

    partial, recovered = asyncio.run(scenario())

    assert KEY_A in partial.errors
    assert recovered.value(KEY_A, int) == 99
    assert KEY_A not in recovered.errors


def test_session_strict_mode_raises_for_requested_existing_error() -> None:
    async def fetch(_key, _context):
        raise SourceUnavailableError("unavailable")

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                )
            ],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        async with builder.session() as session:
            await session.resolve([KEY_A], strict=False)
            with pytest.raises(SnapshotBuildError):
                await session.resolve([KEY_A], strict=True)

    asyncio.run(scenario())


def test_session_deadline_is_shared_across_stages() -> None:
    async def fetch(key, _context):
        if key == KEY_A:
            await asyncio.sleep(0.03)
        else:
            await asyncio.sleep(0.04)
        return key.subject

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch,
                    timeout_seconds=1.0,
                )
            ],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        async with builder.session(deadline_seconds=0.05) as session:
            await session.resolve([KEY_A])
            return await session.resolve([KEY_B], strict=False)

    snapshot = asyncio.run(scenario())
    assert KEY_B in snapshot.errors


def test_closed_async_session_rejects_more_work() -> None:
    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: 1,
                )
            ]
        )
        session = builder.session()
        await session.close()
        with pytest.raises(SessionClosedError):
            await session.resolve([KEY_A])

    asyncio.run(scenario())


def test_sync_session_supports_incremental_resolution() -> None:
    calls = 0

    def fetch(key, _context):
        nonlocal calls
        calls += 1
        return key.subject

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

    with (
        SyncSnapshotBuilder(builder) as sync_builder,
        sync_builder.session(snapshot_id="sync-cycle") as session,
    ):
        session.resolve([KEY_A])
        final = session.resolve([KEY_B])

    assert final.snapshot_id == "sync-cycle"
    assert final.value(KEY_A) == "A"
    assert final.value(KEY_B) == "B"
    assert calls == 2
