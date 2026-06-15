from __future__ import annotations

import asyncio
from typing import Any

from coalestra import (
    AsyncMemoryCache,
    CacheLookup,
    CallableDerivedSource,
    CallableSource,
    FreshnessPolicy,
    PayloadIsolationError,
    ResourceKey,
    ResourceResolutionError,
    SnapshotBuilder,
    SnapshotValue,
    SourcePayload,
)

KEY = ResourceKey("test", "payload")
DERIVED = ResourceKey("test", "derived")


def run(coro):
    return asyncio.run(coro)


def mutable_value() -> dict[str, Any]:
    return {
        "items": [{"id": 1}],
        "settings": {"enabled": True},
    }


def test_source_snapshot_and_cache_do_not_share_mutable_payloads() -> None:
    async def scenario() -> None:
        source_value = mutable_value()
        source_metadata = {"nested": {"labels": ["source"]}}
        calls = 0

        async def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return SourcePayload(
                value=source_value,
                metadata=source_metadata,
            )

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda key: key == KEY,
                    fetcher=fetch,
                )
            ],
            default_policy=FreshnessPolicy(60.0, 60.0),
        )

        first = await builder.build([KEY])
        source_value["items"].append({"id": 2})
        source_metadata["nested"]["labels"].append("mutated")

        assert first.value(KEY)["items"] == [{"id": 1}]
        assert first[KEY].metadata["nested"]["labels"] == ["source"]

        first.value(KEY)["items"].append({"id": 3})
        first[KEY].metadata["nested"]["labels"].append("snapshot")

        second = await builder.build([KEY])
        assert second.value(KEY)["items"] == [{"id": 1}]
        assert second[KEY].metadata["nested"]["labels"] == ["source"]
        assert calls == 1

        await builder.aclose()

    run(scenario())


def test_each_session_snapshot_is_detached_from_session_state() -> None:
    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda key: key == KEY,
                    fetcher=lambda *_: mutable_value(),
                )
            ]
        )

        session = builder.session()
        first = await session.resolve([KEY])
        first.value(KEY)["settings"]["enabled"] = False

        second = session.snapshot()
        assert second.value(KEY)["settings"]["enabled"] is True

        await session.close()
        await builder.aclose()

    run(scenario())


def test_singleflight_callers_receive_independent_payloads() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def fetch(_key, _context):
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return mutable_value()

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda key: key == KEY,
                    fetcher=fetch,
                )
            ]
        )

        first_task = asyncio.create_task(builder.build([KEY]))
        await started.wait()
        second_task = asyncio.create_task(builder.build([KEY]))
        await asyncio.sleep(0)
        release.set()

        first, second = await asyncio.gather(first_task, second_task)
        first.value(KEY)["items"].append({"id": 2})

        assert second.value(KEY)["items"] == [{"id": 1}]
        assert calls == 1

        await builder.aclose()

    run(scenario())


def test_memory_cache_isolates_writes_reads_and_write_results() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        policy = FreshnessPolicy(60.0, 60.0)
        payload = mutable_value()
        metadata = {"nested": {"labels": ["initial"]}}
        candidate = SnapshotValue(
            key=KEY,
            value=payload,
            source="test",
            observed_at=100.0,
            fetched_at=100.0,
            age_seconds=0.0,
            stale=False,
            from_cache=False,
            latency_ms=0.0,
            metadata=metadata,
        )

        result = await cache.set_if_newer(candidate)
        payload["items"].append({"id": 2})
        metadata["nested"]["labels"].append("input")
        result.value.value["items"].append({"id": 3})
        result.value.metadata["nested"]["labels"].append("result")

        first = await cache.get(KEY, now=100.0, policy=policy)
        assert first.value is not None
        assert first.value.value["items"] == [{"id": 1}]
        assert first.value.metadata["nested"]["labels"] == ["initial"]

        first.value.value["items"].append({"id": 4})
        second = await cache.get(KEY, now=100.0, policy=policy)
        assert second.value is not None
        assert second.value.value["items"] == [{"id": 1}]

    run(scenario())


def test_publisher_input_and_result_do_not_mutate_cached_value() -> None:
    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="fallback",
                    priority=1,
                    supports=lambda key: key == KEY,
                    fetcher=lambda *_: {"unexpected": True},
                )
            ],
            default_policy=FreshnessPolicy(60.0, 60.0),
        )
        published = mutable_value()
        result = await builder.publisher.publish(
            KEY,
            published,
            source="stream",
        )

        published["items"].append({"id": 2})
        result.value.value["items"].append({"id": 3})

        snapshot = await builder.build([KEY])
        assert snapshot.value(KEY)["items"] == [{"id": 1}]

        await builder.aclose()

    run(scenario())


def test_derived_source_cannot_mutate_pinned_dependency() -> None:
    async def derive(_key, dependencies, _context):
        dependency = dependencies.value(KEY, dict)
        dependency["items"].append({"id": 2})
        return len(dependency["items"])

    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="raw",
                    priority=10,
                    supports=lambda key: key == KEY,
                    fetcher=lambda *_: mutable_value(),
                ),
                CallableDerivedSource(
                    name="derived",
                    priority=5,
                    supports=lambda key: key == DERIVED,
                    dependencies=lambda _key: (KEY,),
                    deriver=derive,
                ),
            ]
        )

        snapshot = await builder.build([KEY, DERIVED])
        assert snapshot.value(DERIVED, int) == 2
        assert snapshot.value(KEY)["items"] == [{"id": 1}]

        await builder.aclose()

    run(scenario())


def test_non_copyable_payload_becomes_a_structured_source_failure() -> None:
    class NonCopyable:
        def __deepcopy__(self, _memo):
            raise TypeError("copy disabled")

    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda key: key == KEY,
                    fetcher=lambda *_: NonCopyable(),
                    run_sync_in_thread=False,
                )
            ]
        )

        snapshot = await builder.build([KEY], strict=False)
        error = snapshot.errors[KEY]
        assert isinstance(error, ResourceResolutionError)
        assert error.failures[0].error_type == PayloadIsolationError.__name__
        assert "NonCopyable" in error.failures[0].message

        await builder.aclose()

    run(scenario())


class ReferenceCache:
    """Minimal cache that intentionally stores and returns object references."""

    def __init__(self) -> None:
        self.value: SnapshotValue[Any] | None = None

    async def get(
        self,
        key: ResourceKey,
        *,
        now: float,
        policy: FreshnessPolicy,
    ) -> CacheLookup:
        del key, now, policy
        return CacheLookup(
            value=self.value,
            fresh=self.value is not None,
            usable_stale=self.value is not None,
            age_seconds=0.0 if self.value is not None else None,
        )

    async def set(self, value: SnapshotValue[Any]) -> None:
        self.value = value

    async def invalidate(self, key: ResourceKey) -> None:
        del key
        self.value = None

    async def clear(self) -> None:
        self.value = None


def test_builder_isolates_payloads_even_with_reference_cache() -> None:
    async def scenario() -> None:
        cache = ReferenceCache()
        original = mutable_value()
        calls = 0

        async def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return original

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="source",
                    priority=1,
                    supports=lambda key: key == KEY,
                    fetcher=fetch,
                )
            ],
            cache=cache,
            default_policy=FreshnessPolicy(60.0, 60.0),
        )

        first = await builder.build([KEY])
        first.value(KEY)["items"].append({"id": 2})
        original["items"].append({"id": 3})

        second = await builder.build([KEY])
        assert second.value(KEY)["items"] == [{"id": 1}]
        assert calls == 1

        await builder.aclose()

    run(scenario())
