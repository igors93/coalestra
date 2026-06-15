from __future__ import annotations

import asyncio

import pytest

from coalestra import CallableDerivedSource, CallableSource, ResourceKey, SnapshotBuilder


def run(coro):
    return asyncio.run(coro)


def keys(count: int, *, namespace: str) -> tuple[ResourceKey, ...]:
    return tuple(ResourceKey(namespace, "value", str(index)) for index in range(count))


def test_single_source_dispatch_does_not_queue_one_task_per_key() -> None:
    async def scenario() -> None:
        requested = keys(100, namespace="single")
        release = asyncio.Event()
        workers_started = asyncio.Event()
        started = 0

        async def fetch(key, _context):
            nonlocal started
            started += 1
            if started == 4:
                workers_started.set()
            await release.wait()
            return key.subject

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="single",
                    priority=1,
                    supports=lambda key: key.namespace == "single",
                    fetcher=fetch,
                )
            ],
            max_concurrency=4,
        )

        try:
            build_task = asyncio.create_task(builder.build(requested))
            await asyncio.wait_for(workers_started.wait(), timeout=1.0)
            await asyncio.sleep(0)

            capacity = await builder.capacity.snapshot()
            assert started == 4
            assert capacity["__global__"].in_use == 4
            assert capacity["__global__"].waiting == 0

            release.set()
            snapshot = await build_task
            assert tuple(snapshot.resources) == requested
        finally:
            release.set()
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


def test_derived_source_dispatch_does_not_queue_one_task_per_key() -> None:
    async def scenario() -> None:
        requested = keys(100, namespace="derived")
        release = asyncio.Event()
        workers_started = asyncio.Event()
        started = 0

        async def derive(key, _dependencies, _context):
            nonlocal started
            started += 1
            if started == 4:
                workers_started.set()
            await release.wait()
            return key.subject

        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="derived",
                    priority=1,
                    supports=lambda key: key.namespace == "derived",
                    dependencies=lambda _key: (),
                    deriver=derive,
                )
            ],
            max_concurrency=4,
        )

        try:
            build_task = asyncio.create_task(builder.build(requested))
            await asyncio.wait_for(workers_started.wait(), timeout=1.0)
            await asyncio.sleep(0)

            capacity = await builder.capacity.snapshot()
            assert started == 4
            assert capacity["__global__"].in_use == 4
            assert capacity["__global__"].waiting == 0

            release.set()
            snapshot = await build_task
            assert tuple(snapshot.resources) == requested
        finally:
            release.set()
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


def test_pending_task_limit_can_reduce_dispatch_parallelism() -> None:
    async def scenario() -> None:
        requested = keys(20, namespace="limited")
        release = asyncio.Event()
        workers_started = asyncio.Event()
        started = 0

        async def fetch(key, _context):
            nonlocal started
            started += 1
            if started == 2:
                workers_started.set()
            await release.wait()
            return key.subject

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="limited",
                    priority=1,
                    supports=lambda key: key.namespace == "limited",
                    fetcher=fetch,
                )
            ],
            max_concurrency=8,
            max_pending_tasks=2,
        )

        try:
            build_task = asyncio.create_task(builder.build(requested))
            await asyncio.wait_for(workers_started.wait(), timeout=1.0)
            await asyncio.sleep(0)

            capacity = await builder.capacity.snapshot()
            assert builder.max_pending_tasks == 2
            assert started == 2
            assert capacity["__global__"].in_use == 2
            assert capacity["__global__"].waiting == 0

            release.set()
            snapshot = await build_task
            assert tuple(snapshot.resources) == requested
        finally:
            release.set()
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


def test_pending_task_limit_must_be_positive() -> None:
    source = CallableSource(
        name="source",
        priority=1,
        supports=lambda _key: True,
        fetcher=lambda *_: 1,
    )

    with pytest.raises(ValueError, match="max_pending_tasks"):
        SnapshotBuilder([source], max_pending_tasks=0)
