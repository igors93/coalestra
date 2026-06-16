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


def test_session_revalidation_observes_newer_published_cache_value() -> None:
    source_calls = 0

    async def fetch(_key, _context):
        nonlocal source_calls
        source_calls += 1
        return "source"

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
            default_policy=FreshnessPolicy(60.0, 60.0),
        )
        first_publish = await builder.publisher.publish(
            KEY_A,
            "published-1",
            source="stream",
        )
        async with builder.session() as session:
            first = await session.resolve([KEY_A, KEY_B])
            await builder.publisher.publish(
                KEY_A,
                "published-2",
                source="stream",
                observed_at=first_publish.value.observed_at + 1.0,
            )
            refreshed = await session.revalidate([KEY_A])
            return first, refreshed

    first, refreshed = asyncio.run(scenario())

    assert first.value(KEY_A) == "published-1"
    assert refreshed.value(KEY_A) == "published-2"
    assert refreshed.value(KEY_B) == "source"
    assert source_calls == 1


def test_session_revalidates_selected_resource_and_preserves_unrelated_values() -> None:
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
            default_policy=FreshnessPolicy(60.0, 60.0),
        )
        async with builder.session(snapshot_id="cycle-revalidate") as session:
            first = await session.resolve([KEY_A, KEY_B])
            refreshed = await session.revalidate([KEY_A], force_refresh=True)
            return first, refreshed

    first, refreshed = asyncio.run(scenario())

    assert refreshed.snapshot_id == first.snapshot_id == "cycle-revalidate"
    assert refreshed.created_at == first.created_at
    assert first.value(KEY_A) == "A-1"
    assert refreshed.value(KEY_A) == "A-2"
    assert refreshed.value(KEY_B) == "B-1"
    assert refreshed[KEY_A].version != first[KEY_A].version
    assert refreshed[KEY_B].version == first[KEY_B].version
    assert calls == {KEY_A: 2, KEY_B: 1}


def test_session_revalidation_refreshes_pinned_derived_dependents_transitively() -> None:
    from coalestra import CallableDerivedSource

    raw = ResourceKey("raw", "value")
    doubled = ResourceKey("derived", "doubled")
    chained = ResourceKey("derived", "chained")
    unrelated = ResourceKey("other", "value")
    raw_calls = 0
    doubled_calls = 0
    chained_calls = 0

    async def fetch(key, _context):
        nonlocal raw_calls
        if key == raw:
            raw_calls += 1
            return raw_calls
        return 100

    async def derive_doubled(_key, dependencies, _context):
        nonlocal doubled_calls
        doubled_calls += 1
        return dependencies.value(raw, int) * 2

    async def derive_chained(_key, dependencies, _context):
        nonlocal chained_calls
        chained_calls += 1
        return dependencies.value(doubled, int) + 1

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="chained",
                    priority=300,
                    supports=lambda key: key == chained,
                    dependencies=lambda _key: (doubled,),
                    deriver=derive_chained,
                ),
                CallableDerivedSource(
                    name="doubled",
                    priority=200,
                    supports=lambda key: key == doubled,
                    dependencies=lambda _key: (raw,),
                    deriver=derive_doubled,
                ),
                CallableSource(
                    name="source",
                    priority=100,
                    supports=lambda key: key in {raw, unrelated},
                    fetcher=fetch,
                ),
            ],
            default_policy=FreshnessPolicy(60.0, 60.0),
        )
        async with builder.session() as session:
            first = await session.resolve([raw, doubled, chained, unrelated])
            refreshed = await session.revalidate([raw], force_refresh=True)
            return first, refreshed

    first, refreshed = asyncio.run(scenario())

    assert first.value(raw, int) == 1
    assert first.value(doubled, int) == 2
    assert first.value(chained, int) == 3
    assert refreshed.value(raw, int) == 2
    assert refreshed.value(doubled, int) == 4
    assert refreshed.value(chained, int) == 5
    assert refreshed.value(unrelated, int) == 100
    assert refreshed[unrelated].version == first[unrelated].version
    assert raw_calls == 2
    assert doubled_calls == 2
    assert chained_calls == 2


def test_session_can_revalidate_internal_dependency_and_refresh_visible_dependent() -> None:
    from coalestra import CallableDerivedSource

    raw = ResourceKey("raw", "value")
    doubled = ResourceKey("derived", "doubled")
    chained = ResourceKey("derived", "chained")
    raw_calls = 0

    async def fetch_raw(_key, _context):
        nonlocal raw_calls
        raw_calls += 1
        return raw_calls

    async def derive_doubled(_key, dependencies, _context):
        return dependencies.value(raw, int) * 2

    async def derive_chained(_key, dependencies, _context):
        return dependencies.value(doubled, int) + 1

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="chained",
                    priority=300,
                    supports=lambda key: key == chained,
                    dependencies=lambda _key: (doubled,),
                    deriver=derive_chained,
                ),
                CallableDerivedSource(
                    name="doubled",
                    priority=200,
                    supports=lambda key: key == doubled,
                    dependencies=lambda _key: (raw,),
                    deriver=derive_doubled,
                ),
                CallableSource(
                    name="raw",
                    priority=100,
                    supports=lambda key: key == raw,
                    fetcher=fetch_raw,
                ),
            ],
            default_policy=FreshnessPolicy(60.0, 60.0),
        )
        async with builder.session() as session:
            first = await session.resolve([chained])
            refreshed = await session.revalidate([raw], force_refresh=True)
            return first, refreshed

    first, refreshed = asyncio.run(scenario())

    assert tuple(first.resources) == (chained,)
    assert tuple(refreshed.resources) == (chained,)
    assert first.value(chained, int) == 3
    assert refreshed.value(chained, int) == 5
    assert raw_calls == 2


def test_failed_session_revalidation_retains_previous_state_transactionally() -> None:
    from coalestra import CallableDerivedSource

    raw = ResourceKey("raw", "value")
    doubled = ResourceKey("derived", "doubled")
    should_fail = False

    async def fetch_raw(_key, _context):
        if should_fail:
            raise SourceUnavailableError("refresh failed")
        return 4

    async def derive(_key, dependencies, _context):
        return dependencies.value(raw, int) * 2

    async def scenario():
        nonlocal should_fail
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="derived",
                    priority=100,
                    supports=lambda key: key == doubled,
                    dependencies=lambda _key: (raw,),
                    deriver=derive,
                ),
                CallableSource(
                    name="raw",
                    priority=10,
                    supports=lambda key: key == raw,
                    fetcher=fetch_raw,
                ),
            ],
            default_policy=FreshnessPolicy(60.0, 60.0),
            retry_policy=RetryPolicy(max_attempts=1),
        )
        async with builder.session() as session:
            first = await session.resolve([raw, doubled])
            should_fail = True
            failed = await session.revalidate([raw], strict=False, force_refresh=True)
            retained = session.snapshot()
            return first, failed, retained

    first, failed, retained = asyncio.run(scenario())

    assert failed.value(raw, int) == first.value(raw, int) == 4
    assert failed.value(doubled, int) == first.value(doubled, int) == 8
    assert raw in failed.errors
    assert retained.errors == {}
    assert retained[raw].version == first[raw].version
    assert retained[doubled].version == first[doubled].version


def test_strict_session_revalidation_raises_with_retained_snapshot() -> None:
    available = True

    async def fetch(_key, _context):
        if not available:
            raise SourceUnavailableError("refresh failed")
        return 7

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
            first = await session.resolve([KEY_A])
            available = False
            with pytest.raises(SnapshotBuildError) as captured:
                await session.revalidate([KEY_A], force_refresh=True)
            return first, captured.value, session.snapshot()

    first, error, retained = asyncio.run(scenario())

    assert error.snapshot is not None
    assert error.snapshot.value(KEY_A, int) == 7
    assert KEY_A in error.errors
    assert retained[KEY_A].version == first[KEY_A].version
    assert retained.errors == {}


def test_session_revalidation_rejects_resources_not_yet_resolved() -> None:
    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda key, _context: key.subject,
                )
            ]
        )
        async with builder.session() as session:
            await session.resolve([KEY_A])
            with pytest.raises(ValueError, match="already resolved"):
                await session.revalidate([KEY_B])

    asyncio.run(scenario())


def test_sync_session_supports_selective_revalidation() -> None:
    calls: dict[ResourceKey, int] = {}

    def fetch(key, _context):
        calls[key] = calls.get(key, 0) + 1
        return f"{key.subject}-{calls[key]}"

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="source",
                priority=1,
                supports=lambda _key: True,
                fetcher=fetch,
            )
        ],
        default_policy=FreshnessPolicy(60.0, 60.0),
    )

    with (
        SyncSnapshotBuilder(builder) as sync_builder,
        sync_builder.session(snapshot_id="sync-revalidate") as session,
    ):
        first = session.resolve([KEY_A, KEY_B])
        refreshed = session.revalidate([KEY_A], force_refresh=True)

    assert first.value(KEY_A) == "A-1"
    assert refreshed.value(KEY_A) == "A-2"
    assert refreshed.value(KEY_B) == "B-1"
    assert refreshed.snapshot_id == "sync-revalidate"
    assert calls == {KEY_A: 2, KEY_B: 1}


def test_session_revalidation_uses_original_deadline_and_retains_previous_value() -> None:
    calls = 0

    async def fetch(_key, _context):
        nonlocal calls
        calls += 1
        return calls

    class ManualClock:
        def __init__(self) -> None:
            self.wall_time = 1_000.0
            self.monotonic_time = 100.0

        def now(self) -> float:
            return self.wall_time

        def monotonic(self) -> float:
            return self.monotonic_time

        def advance(self, seconds: float) -> None:
            self.wall_time += seconds
            self.monotonic_time += seconds

    async def scenario():
        clock = ManualClock()
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
            clock=clock,
        )
        async with builder.session(deadline_seconds=1.0) as session:
            first = await session.resolve([KEY_A])
            clock.advance(2.0)
            failed = await session.revalidate(
                [KEY_A],
                strict=False,
                force_refresh=True,
            )
            retained = session.snapshot()
            return first, failed, retained

    first, failed, retained = asyncio.run(scenario())

    assert failed.value(KEY_A, int) == first.value(KEY_A, int) == 1
    assert KEY_A in failed.errors
    assert retained.errors == {}
    assert retained[KEY_A].version == first[KEY_A].version
    assert calls == 1


def test_session_revalidation_cascades_from_published_dependency_update() -> None:
    from coalestra import CallableDerivedSource

    raw = ResourceKey("raw", "value")
    doubled = ResourceKey("derived", "doubled")
    source_calls = 0
    derivations = 0

    async def fetch_raw(_key, _context):
        nonlocal source_calls
        source_calls += 1
        return 2

    async def derive(_key, dependencies, _context):
        nonlocal derivations
        derivations += 1
        return dependencies.value(raw, int) * 2

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="derived",
                    priority=100,
                    supports=lambda key: key == doubled,
                    dependencies=lambda _key: (raw,),
                    deriver=derive,
                ),
                CallableSource(
                    name="raw",
                    priority=10,
                    supports=lambda key: key == raw,
                    fetcher=fetch_raw,
                ),
            ],
            default_policy=FreshnessPolicy(60.0, 60.0),
        )
        async with builder.session() as session:
            first = await session.resolve([raw, doubled])
            await builder.publisher.publish(
                raw,
                5,
                source="stream",
                observed_at=first[raw].observed_at + 1.0,
            )
            refreshed = await session.revalidate([raw])
            return first, refreshed

    first, refreshed = asyncio.run(scenario())

    assert first.value(raw, int) == 2
    assert first.value(doubled, int) == 4
    assert refreshed.value(raw, int) == 5
    assert refreshed.value(doubled, int) == 10
    assert source_calls == 1
    assert derivations == 2
