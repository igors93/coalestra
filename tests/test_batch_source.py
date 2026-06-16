from __future__ import annotations

import asyncio
import threading

from coalestra import (
    CallableBatchSource,
    CallableSource,
    ResourceKey,
    SnapshotBuilder,
    SourceProtocolError,
)
from coalestra.resilience import RetryPolicy

KEYS = tuple(ResourceKey("market", "price", symbol) for symbol in ("BTC", "ETH", "SOL"))


def test_batch_source_resolves_many_resources_with_one_call() -> None:
    calls: list[tuple[ResourceKey, ...]] = []

    async def fetch_many(keys, _context):
        requested = tuple(keys)
        calls.append(requested)
        return {key: key.subject for key in requested}

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="batch",
                    priority=100,
                    supports=lambda key: key.namespace == "market",
                    fetcher=fetch_many,
                )
            ]
        )
        return await builder.build(KEYS)

    snapshot = asyncio.run(scenario())

    assert calls == [KEYS]
    assert [snapshot[key].value for key in KEYS] == ["BTC", "ETH", "SOL"]


def test_partial_batch_result_falls_back_only_for_missing_resources() -> None:
    fallback_calls: list[ResourceKey] = []

    async def fetch_many(keys, _context):
        return {key: key.subject for key in keys if key != KEYS[1]}

    async def fallback(key, _context):
        fallback_calls.append(key)
        return f"fallback-{key.subject}"

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="batch",
                    priority=100,
                    supports=lambda _key: True,
                    fetcher=fetch_many,
                ),
                CallableSource(
                    name="fallback",
                    priority=10,
                    supports=lambda _key: True,
                    fetcher=fallback,
                ),
            ],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        return await builder.build(KEYS)

    snapshot = asyncio.run(scenario())

    assert snapshot[KEYS[0]].source == "batch"
    assert snapshot[KEYS[1]].source == "fallback"
    assert snapshot[KEYS[2]].source == "batch"
    assert fallback_calls == [KEYS[1]]


def test_concurrent_identical_builds_share_one_batch_operation() -> None:
    calls = 0

    async def fetch_many(keys, _context):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return dict.fromkeys(keys, calls)

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="batch",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch_many,
                )
            ]
        )
        return await asyncio.gather(builder.build(KEYS), builder.build(KEYS))

    first, second = asyncio.run(scenario())

    assert calls == 1
    assert all(first[key].value == 1 for key in KEYS)
    assert all(second[key].value == 1 for key in KEYS)
    assert all(second[key].metadata["coalesced_request"] is True for key in KEYS)


def test_synchronous_batch_fetcher_runs_in_worker_thread() -> None:
    caller_thread = threading.get_ident()
    worker_thread = caller_thread

    def fetch_many(keys, _context):
        nonlocal worker_thread
        worker_thread = threading.get_ident()
        return dict.fromkeys(keys, 1)

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="batch",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch_many,
                )
            ]
        )
        return await builder.build(KEYS)

    asyncio.run(scenario())

    assert worker_thread != caller_thread


def test_batch_protocol_error_can_fall_back_to_another_source() -> None:
    unexpected = ResourceKey("market", "price", "DOGE")

    async def invalid_batch(_keys, _context):
        return {unexpected: 1}

    async def fallback(key, _context):
        return key.subject

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="invalid",
                    priority=100,
                    supports=lambda _key: True,
                    fetcher=invalid_batch,
                ),
                CallableSource(
                    name="fallback",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fallback,
                ),
            ],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        return await builder.build(KEYS)

    snapshot = asyncio.run(scenario())
    assert all(snapshot[key].source == "fallback" for key in KEYS)


class InvalidBatchSource:
    name = "invalid"
    priority = 1
    timeout_seconds = None
    blocking_io = False
    blocking_io_offloaded = False
    transport_timeout_seconds = None

    def supports(self, _key: ResourceKey) -> bool:
        return True

    async def fetch_many(self, _keys, _context):
        return 42


def test_custom_batch_source_must_return_mapping() -> None:
    async def scenario():
        builder = SnapshotBuilder(
            [InvalidBatchSource()],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        return await builder.build([KEYS[0]], strict=False)

    snapshot = asyncio.run(scenario())
    resolution = snapshot.errors[KEYS[0]]
    assert resolution.failures[0].error_type == SourceProtocolError.__name__  # type: ignore[attr-defined]


def test_overlapping_batch_builds_coalesce_shared_keys() -> None:
    calls: list[tuple[ResourceKey, ...]] = []
    release = asyncio.Event()

    async def fetch_many(keys, _context):
        requested = tuple(keys)
        calls.append(requested)
        await release.wait()
        return {key: key.subject for key in requested}

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="batch",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch_many,
                )
            ]
        )
        first = asyncio.create_task(builder.build(KEYS[:2]))
        await asyncio.sleep(0)
        second = asyncio.create_task(builder.build(KEYS[1:]))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(first, second)

    first, second = asyncio.run(scenario())

    assert len(calls) == 2
    assert KEYS[1] in first
    assert KEYS[1] in second
    assert sum(KEYS[1] in call for call in calls) == 1
