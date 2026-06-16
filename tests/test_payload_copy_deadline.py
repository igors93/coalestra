from __future__ import annotations

import asyncio
import threading
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import pytest

from coalestra import (
    CacheLookup,
    CallableSource,
    FreshnessPolicy,
    ResourceKey,
    ResourceResolutionError,
    SnapshotBuilder,
    SnapshotBuildError,
    SnapshotDeadlineExceededError,
    SnapshotValue,
)


@dataclass(frozen=True)
class _Payload:
    value: str


class _BlockingCopier:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.started = asyncio.Event()
        self.release = threading.Event()
        self.block_value: str | None = None
        self.block_on_occurrence: int | None = None
        self._occurrences: dict[str, int] = {}
        self._lock = threading.Lock()

    def bind(self) -> None:
        self.loop = asyncio.get_running_loop()

    def copier(self, value: Any) -> Any:
        if not isinstance(value, _Payload):
            return deepcopy(value)

        with self._lock:
            occurrence = self._occurrences.get(value.value, 0) + 1
            self._occurrences[value.value] = occurrence

        should_block = value.value == self.block_value and (
            self.block_on_occurrence is None or occurrence == self.block_on_occurrence
        )
        if should_block:
            loop = self.loop
            if loop is not None:
                loop.call_soon_threadsafe(self.started.set)
            self.release.wait(timeout=2.0)
        return deepcopy(value)


class _PassthroughCache:
    def __init__(self, value: SnapshotValue[Any] | None = None) -> None:
        self.value = value

    async def get(
        self,
        key: ResourceKey,
        *,
        now: float,
        policy: FreshnessPolicy,
    ) -> CacheLookup:
        value = self.value if self.value is not None and self.value.key == key else None
        return CacheLookup(
            value=value,
            fresh=value is not None,
            usable_stale=value is not None,
            age_seconds=None if value is None else max(0.0, now - value.observed_at),
        )

    async def set(self, value: SnapshotValue[Any]) -> None:
        self.value = value

    async def invalidate(self, key: ResourceKey) -> None:
        if self.value is not None and self.value.key == key:
            self.value = None

    async def clear(self) -> None:
        self.value = None


def _source(key: ResourceKey, payload: Any) -> CallableSource:
    return CallableSource(
        name="source",
        priority=1,
        supports=lambda candidate: candidate == key,
        fetcher=lambda _key, _context: payload,
    )


def _assert_deadline_build_error(error: SnapshotBuildError, key: ResourceKey) -> None:
    assert key in error.errors
    resource_error = error.errors[key]
    if isinstance(resource_error, SnapshotDeadlineExceededError):
        return
    assert isinstance(resource_error, ResourceResolutionError)
    assert any(
        failure.error_type == SnapshotDeadlineExceededError.__name__
        for failure in resource_error.failures
    )


def test_source_payload_copy_respects_snapshot_deadline() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "copy-deadline", "source")
        probe = _BlockingCopier()
        probe.bind()
        probe.block_value = "source"
        builder = SnapshotBuilder(
            [_source(key, _Payload("source"))],
            cache=_PassthroughCache(),
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )

        try:
            with pytest.raises(SnapshotBuildError) as captured:
                await builder.build([key], deadline_seconds=0.05)
            _assert_deadline_build_error(captured.value, key)
            assert probe.started.is_set()
        finally:
            probe.release.set()
            await builder.aclose()

    asyncio.run(scenario())


def test_waiting_for_copy_capacity_respects_snapshot_deadline() -> None:
    async def scenario() -> None:
        publish_key = ResourceKey("test", "copy-deadline", "publisher")
        build_key = ResourceKey("test", "copy-deadline", "build")
        probe = _BlockingCopier()
        probe.bind()
        probe.block_value = "occupy"
        builder = SnapshotBuilder(
            [_source(build_key, _Payload("build"))],
            cache=_PassthroughCache(),
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )

        publication = asyncio.create_task(
            builder.publisher.publish(
                publish_key,
                _Payload("occupy"),
                source="stream",
            )
        )
        await probe.started.wait()

        try:
            with pytest.raises(SnapshotBuildError) as captured:
                await builder.build([build_key], deadline_seconds=0.05)
            _assert_deadline_build_error(captured.value, build_key)
        finally:
            probe.release.set()
            await publication
            await builder.aclose()

    asyncio.run(scenario())


def test_custom_cache_copy_respects_snapshot_deadline() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "copy-deadline", "cache")
        probe = _BlockingCopier()
        probe.bind()
        probe.block_value = "cached"
        cached = SnapshotValue(
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
            cache=_PassthroughCache(cached),
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )

        try:
            with pytest.raises(SnapshotBuildError) as captured:
                await builder.build([key], deadline_seconds=0.05)
            _assert_deadline_build_error(captured.value, key)
            assert captured.value.snapshot is None
        finally:
            probe.release.set()
            await builder.aclose()

    asyncio.run(scenario())


def test_snapshot_delivery_copy_respects_snapshot_deadline() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "copy-deadline", "delivery")
        probe = _BlockingCopier()
        probe.bind()
        probe.block_value = "delivery"
        probe.block_on_occurrence = 3
        builder = SnapshotBuilder(
            [_source(key, _Payload("delivery"))],
            cache=_PassthroughCache(),
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )

        try:
            with pytest.raises(SnapshotBuildError) as captured:
                await builder.build([key], deadline_seconds=0.05)
            _assert_deadline_build_error(captured.value, key)
            assert captured.value.snapshot is None
            health = await builder.health_snapshot()
            assert health.deadline_exceeded_count >= 1
        finally:
            probe.release.set()
            await builder.aclose()

    asyncio.run(scenario())


def test_revalidation_delivery_deadline_retains_previous_state() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "copy-deadline", "revalidation")
        current = {"payload": _Payload("old")}
        probe = _BlockingCopier()
        probe.bind()
        source = CallableSource(
            name="source",
            priority=1,
            supports=lambda candidate: candidate == key,
            fetcher=lambda _key, _context: current["payload"],
        )
        builder = SnapshotBuilder(
            [source],
            cache=_PassthroughCache(),
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )
        session = builder.session(deadline_seconds=0.15)
        initial = await session.resolve([key])
        assert initial.value(key) == _Payload("old")

        current["payload"] = _Payload("new")
        probe.block_value = "new"
        probe.block_on_occurrence = 3
        probe.started.clear()

        try:
            with pytest.raises(SnapshotBuildError) as captured:
                await session.revalidate([key], force_refresh=True)
            _assert_deadline_build_error(captured.value, key)
            retained = session.snapshot()
            assert retained.value(key) == _Payload("old")
        finally:
            probe.release.set()
            await session.close()
            await builder.aclose()

    asyncio.run(scenario())


def test_timed_out_copy_keeps_shared_capacity_until_worker_finishes() -> None:
    async def scenario() -> None:
        source_key = ResourceKey("test", "copy-deadline", "timed-out")
        publish_key = ResourceKey("test", "copy-deadline", "after-timeout")
        probe = _BlockingCopier()
        probe.bind()
        probe.block_value = "first"
        builder = SnapshotBuilder(
            [_source(source_key, _Payload("first"))],
            cache=_PassthroughCache(),
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
        )

        with pytest.raises(SnapshotBuildError):
            await builder.build([source_key], deadline_seconds=0.05)

        probe.started.clear()
        probe.block_value = "second"
        publication = asyncio.create_task(
            builder.publisher.publish(
                publish_key,
                _Payload("second"),
                source="stream",
            )
        )
        await asyncio.sleep(0.03)
        assert not probe.started.is_set()

        probe.release.set()
        await asyncio.wait_for(probe.started.wait(), timeout=1.0)
        await publication
        await builder.aclose()

    asyncio.run(scenario())


def test_default_memory_cache_copy_respects_snapshot_deadline() -> None:
    async def scenario() -> None:
        key = ResourceKey("test", "copy-deadline", "memory-cache")
        probe = _BlockingCopier()
        probe.bind()
        probe.block_value = "internal"
        probe.block_on_occurrence = 3
        builder = SnapshotBuilder(
            [_source(key, _Payload("internal"))],
            payload_copier=probe.copier,
            run_payload_copies_in_thread=True,
            max_copy_concurrency=1,
            cache_run_payload_copies_in_thread=True,
            cache_max_copy_concurrency=1,
        )

        try:
            with pytest.raises(SnapshotBuildError) as captured:
                await builder.build([key], deadline_seconds=0.05)
            _assert_deadline_build_error(captured.value, key)
            assert captured.value.snapshot is None
        finally:
            probe.release.set()
            await builder.aclose()

    asyncio.run(scenario())


def test_project_version_includes_snapshot_acceptance_release() -> None:
    import coalestra

    assert coalestra.__version__ == "0.6.0"
