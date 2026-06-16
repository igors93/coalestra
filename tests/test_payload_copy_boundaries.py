from __future__ import annotations

import asyncio
import threading
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import pytest

from coalestra import (
    CacheLookup,
    CallableBatchSource,
    CallableDerivedSource,
    CallableSource,
    FreshnessPolicy,
    ResourceKey,
    SnapshotBuilder,
    SnapshotValue,
)


@dataclass(frozen=True)
class _Payload:
    name: str


class _CopyProbe:
    def __init__(self) -> None:
        self.event_loop: asyncio.AbstractEventLoop | None = None
        self.block_names: set[str] = set()
        self.release = threading.Event()
        self.started: dict[str, asyncio.Event] = {}
        self.active = 0
        self.maximum_active = 0
        self.total_starts = 0
        self.thread_ids: list[int] = []
        self._lock = threading.Lock()

    def bind(self, *names: str) -> None:
        self.event_loop = asyncio.get_running_loop()
        self.started = {name: asyncio.Event() for name in names}

    def copier(self, value: Any) -> Any:
        if not isinstance(value, _Payload):
            return deepcopy(value)

        with self._lock:
            self.thread_ids.append(threading.get_ident())
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            self.total_starts += 1

        try:
            loop = self.event_loop
            started = self.started.get(value.name)
            if loop is not None and started is not None:
                loop.call_soon_threadsafe(started.set)
            if value.name in self.block_names:
                self.release.wait(timeout=2.0)
            return value
        finally:
            with self._lock:
                self.active -= 1


def run(coro):
    return asyncio.run(coro)


def _source(key: ResourceKey, value: Any) -> CallableSource:
    return CallableSource(
        name=f"source-{key.subject}",
        priority=1,
        supports=lambda candidate: candidate == key,
        fetcher=lambda _key, _context: value,
    )


class _StaticCache:
    def __init__(self, value: SnapshotValue[Any]) -> None:
        self.value = value

    async def get(
        self,
        key: ResourceKey,
        *,
        now: float,
        policy: FreshnessPolicy,
    ) -> CacheLookup:
        assert key == self.value.key
        return CacheLookup(
            value=self.value,
            fresh=True,
            usable_stale=True,
            age_seconds=max(0.0, now - self.value.observed_at),
        )

    async def set(self, value: SnapshotValue[Any]) -> None:
        self.value = value

    async def invalidate(self, key: ResourceKey) -> None:
        return None

    async def clear(self) -> None:
        return None


def test_builder_validates_boundary_copy_settings() -> None:
    key = ResourceKey("test", "copy-settings")

    with pytest.raises(TypeError, match="run_payload_copies_in_thread"):
        SnapshotBuilder(
            [_source(key, "value")],
            run_payload_copies_in_thread=1,  # type: ignore[arg-type]
        )

    with pytest.raises(TypeError, match="max_copy_concurrency"):
        SnapshotBuilder(
            [_source(key, "value")],
            max_copy_concurrency=True,  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError, match="max_copy_concurrency"):
        SnapshotBuilder([_source(key, "value")], max_copy_concurrency=0)


def test_builder_auto_offloads_builtin_copier_only() -> None:
    key = ResourceKey("test", "copy-mode")

    builtin = SnapshotBuilder([_source(key, "value")])
    custom = SnapshotBuilder([_source(key, "value")], payload_copier=deepcopy)

    assert builtin.run_payload_copies_in_thread is True
    assert custom.run_payload_copies_in_thread is False


def test_source_payload_copy_does_not_block_event_loop() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "source-offload")
        probe = _CopyProbe()
        probe.bind("source")
        probe.block_names.add("source")
        watchdog = threading.Timer(1.0, probe.release.set)
        builder = SnapshotBuilder(
            [_source(key, _Payload("source"))],
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )

        watchdog.start()
        try:
            started_at = asyncio.get_running_loop().time()
            build = asyncio.create_task(builder.build([key]))
            await probe.started["source"].wait()
            resumed_after = asyncio.get_running_loop().time() - started_at

            assert resumed_after < 0.5
            assert not build.done()
            assert probe.thread_ids
            assert all(thread_id != threading.get_ident() for thread_id in probe.thread_ids)

            probe.release.set()
            snapshot = await build
            assert snapshot.value(key) == _Payload("source")
        finally:
            probe.release.set()
            watchdog.cancel()
            await builder.aclose()

    run(scenario())


def test_publisher_payload_copy_does_not_block_event_loop() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "publisher-offload")
        probe = _CopyProbe()
        probe.bind("published")
        probe.block_names.add("published")
        watchdog = threading.Timer(1.0, probe.release.set)
        builder = SnapshotBuilder(
            [_source(key, "fallback")],
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )

        watchdog.start()
        try:
            started_at = asyncio.get_running_loop().time()
            publication = asyncio.create_task(
                builder.publisher.publish(
                    key,
                    _Payload("published"),
                    source="stream",
                )
            )
            await probe.started["published"].wait()
            resumed_after = asyncio.get_running_loop().time() - started_at

            assert resumed_after < 0.5
            assert not publication.done()

            probe.release.set()
            result = await publication
            assert result.value.value == _Payload("published")
        finally:
            probe.release.set()
            watchdog.cancel()
            await builder.aclose()

    run(scenario())


def test_custom_cache_boundary_copy_does_not_block_event_loop() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "custom-cache-offload")
        probe = _CopyProbe()
        probe.bind("cached")
        probe.block_names.add("cached")
        watchdog = threading.Timer(1.0, probe.release.set)
        cached_value = SnapshotValue(
            key=key,
            value=_Payload("cached"),
            source="cache",
            observed_at=100.0,
            fetched_at=100.0,
            age_seconds=0.0,
            stale=False,
            from_cache=True,
            latency_ms=0.0,
        )
        builder = SnapshotBuilder(
            [_source(key, "fallback")],
            cache=_StaticCache(cached_value),
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
            clock=type(
                "StaticClock",
                (),
                {"now": staticmethod(lambda: 100.0), "monotonic": staticmethod(lambda: 100.0)},
            )(),
        )

        watchdog.start()
        try:
            started_at = asyncio.get_running_loop().time()
            build = asyncio.create_task(builder.build([key]))
            await probe.started["cached"].wait()
            resumed_after = asyncio.get_running_loop().time() - started_at

            assert resumed_after < 0.5
            assert not build.done()

            probe.release.set()
            snapshot = await build
            assert snapshot.value(key) == _Payload("cached")
        finally:
            probe.release.set()
            watchdog.cancel()
            await builder.aclose()

    run(scenario())


def test_snapshot_async_copy_does_not_block_event_loop() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "snapshot-offload")
        probe = _CopyProbe()
        probe.bind("snapshot")
        builder = SnapshotBuilder(
            [_source(key, _Payload("snapshot"))],
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )
        session = builder.session()
        await session.resolve([key])

        probe.block_names.add("snapshot")
        probe.release.clear()
        probe.started["snapshot"].clear()
        watchdog = threading.Timer(1.0, probe.release.set)
        watchdog.start()
        try:
            started_at = asyncio.get_running_loop().time()
            delivery = asyncio.create_task(session.snapshot_async())
            await probe.started["snapshot"].wait()
            resumed_after = asyncio.get_running_loop().time() - started_at

            assert resumed_after < 0.5
            assert not delivery.done()

            probe.release.set()
            snapshot = await delivery
            assert snapshot.value(key) == _Payload("snapshot")
        finally:
            probe.release.set()
            watchdog.cancel()
            await session.close()
            await builder.aclose()

    run(scenario())


def test_derived_dependency_copy_does_not_block_event_loop() -> None:
    async def scenario() -> None:
        raw_key = ResourceKey("test", "derived-copy", "raw")
        derived_key = ResourceKey("test", "derived-copy", "result")
        probe = _CopyProbe()
        probe.bind("dependency")
        builder = SnapshotBuilder(
            [
                _source(raw_key, _Payload("dependency")),
                CallableDerivedSource(
                    name="derived",
                    priority=2,
                    supports=lambda key: key == derived_key,
                    dependencies=lambda _key: (raw_key,),
                    deriver=lambda _key, snapshot, _context: snapshot.value(raw_key).name,
                ),
            ],
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )
        session = builder.session()
        await session.resolve([raw_key])

        probe.block_names.add("dependency")
        probe.release.clear()
        probe.started["dependency"].clear()
        watchdog = threading.Timer(1.0, probe.release.set)
        watchdog.start()
        try:
            started_at = asyncio.get_running_loop().time()
            resolution = asyncio.create_task(session.resolve([derived_key]))
            await probe.started["dependency"].wait()
            resumed_after = asyncio.get_running_loop().time() - started_at

            assert resumed_after < 0.5
            assert not resolution.done()

            probe.release.set()
            snapshot = await resolution
            assert snapshot.value(derived_key) == "dependency"
        finally:
            probe.release.set()
            watchdog.cancel()
            await session.close()
            await builder.aclose()

    run(scenario())


def test_batch_source_payload_copies_respect_shared_concurrency_limit() -> None:
    async def scenario() -> None:
        keys = tuple(ResourceKey("test", "batch-copy", str(index)) for index in range(4))
        probe = _CopyProbe()
        names = tuple(f"payload-{index}" for index in range(4))
        probe.bind(*names)
        probe.block_names.update(names)

        source = CallableBatchSource(
            name="batch",
            priority=1,
            supports=lambda key: key in keys,
            fetcher=lambda requested, _context: {
                key: _Payload(f"payload-{key.subject}") for key in requested
            },
        )
        builder = SnapshotBuilder(
            [source],
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=2,
        )
        build = asyncio.create_task(builder.build(keys))

        try:
            for _ in range(10_000):
                with probe._lock:
                    if probe.total_starts >= 2:
                        break
                await asyncio.sleep(0)
            with probe._lock:
                assert probe.total_starts == 2
                assert probe.maximum_active == 2
            await asyncio.sleep(0.05)
            with probe._lock:
                assert probe.total_starts == 2
                assert probe.maximum_active == 2
        finally:
            probe.release.set()

        snapshot = await build
        assert len(snapshot) == 4
        assert probe.maximum_active == 2
        await builder.aclose()

    run(scenario())


def test_cancelled_source_copy_keeps_shared_slot_until_thread_finishes() -> None:
    async def scenario() -> None:
        source_key = ResourceKey("test", "cancel-copy", "source")
        publish_key = ResourceKey("test", "cancel-copy", "publisher")
        probe = _CopyProbe()
        probe.bind("first", "second")
        probe.block_names.add("first")
        builder = SnapshotBuilder(
            [_source(source_key, _Payload("first")), _source(publish_key, "fallback")],
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )

        first = asyncio.create_task(builder.build([source_key]))
        await probe.started["first"].wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        second = asyncio.create_task(
            builder.publisher.publish(
                publish_key,
                _Payload("second"),
                source="stream",
            )
        )
        await asyncio.sleep(0.05)
        assert not probe.started["second"].is_set()

        probe.release.set()
        await asyncio.wait_for(probe.started["second"].wait(), timeout=1.0)
        await second
        await builder.aclose()

    run(scenario())
