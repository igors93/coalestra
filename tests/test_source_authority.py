from __future__ import annotations

import asyncio
import time

import pytest

from coalestra import (
    AsyncMemoryCache,
    AuthorityPolicyResolver,
    CacheLookup,
    CacheWriteStatus,
    CallableSource,
    FreshnessPolicy,
    PublishStatus,
    ResourceKey,
    ResourceUpdate,
    SnapshotBuilder,
    SnapshotValue,
    SourceAuthorityPolicy,
)

POSITION = ResourceKey("account", "position", "BTCUSDT")
RISK = ResourceKey("risk", "exposure", "BTCUSDT")
OTHER = ResourceKey("account", "summary")
ALL_VALUES = FreshnessPolicy(float("inf"), float("inf"))


def run(coro):
    return asyncio.run(coro)


def snapshot_value(
    key: ResourceKey,
    value: object,
    *,
    source: str,
    observed_at: float,
    authority_rank: int,
    dependency_versions: dict[ResourceKey, str] | None = None,
) -> SnapshotValue[object]:
    return SnapshotValue(
        key=key,
        value=value,
        source=source,
        observed_at=observed_at,
        fetched_at=observed_at,
        age_seconds=0.0,
        stale=False,
        from_cache=False,
        latency_ms=0.0,
        authority_rank=authority_rank,
        dependency_versions=dependency_versions or {},
    )


def authority_policy() -> SourceAuthorityPolicy:
    return SourceAuthorityPolicy(
        source_ranks={
            "reconciled-local": 300,
            "user-data-stream": 200,
            "rest": 100,
        }
    )


def test_memory_cache_prefers_authority_before_observation_time() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        rest = snapshot_value(
            POSITION,
            "rest-newer",
            source="rest",
            observed_at=200.0,
            authority_rank=100,
        )
        local = snapshot_value(
            POSITION,
            "local-older",
            source="reconciled-local",
            observed_at=100.0,
            authority_rank=300,
        )
        later_rest = snapshot_value(
            POSITION,
            "rest-latest",
            source="rest",
            observed_at=300.0,
            authority_rank=100,
        )

        assert (await cache.set_if_newer(rest)).status is CacheWriteStatus.STORED
        assert (await cache.set_if_newer(local)).status is CacheWriteStatus.STORED

        ignored = await cache.set_if_newer(later_rest)
        assert ignored.status is CacheWriteStatus.IGNORED_LOWER_AUTHORITY
        assert ignored.value.value == "local-older"

        lookup = await cache.get(POSITION, now=300.0, policy=ALL_VALUES)
        assert lookup.value is not None
        assert lookup.value.value == "local-older"
        assert lookup.value.authority_rank == 300

    run(scenario())


def test_force_write_can_override_higher_authority() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        local = snapshot_value(
            POSITION,
            "local",
            source="reconciled-local",
            observed_at=100.0,
            authority_rank=300,
        )
        rest = snapshot_value(
            POSITION,
            "forced-rest",
            source="rest",
            observed_at=200.0,
            authority_rank=100,
        )
        await cache.set(local)

        result = await cache.set_if_newer(rest, force=True)
        assert result.status is CacheWriteStatus.STORED
        assert result.value.value == "forced-rest"
        assert result.value.authority_rank == 100

    run(scenario())


def test_batch_cache_selects_highest_authority_candidate_in_any_order() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        local = snapshot_value(
            POSITION,
            "local",
            source="reconciled-local",
            observed_at=100.0,
            authority_rank=300,
        )
        rest = snapshot_value(
            POSITION,
            "rest",
            source="rest",
            observed_at=200.0,
            authority_rank=100,
        )

        await cache.set_many_if_newer((rest, local))
        first = await cache.get(POSITION, now=200.0, policy=ALL_VALUES)
        assert first.value is not None
        assert first.value.value == "local"

        await cache.clear()
        await cache.set_many_if_newer((local, rest))
        second = await cache.get(POSITION, now=200.0, policy=ALL_VALUES)
        assert second.value is not None
        assert second.value.value == "local"

    run(scenario())


def test_publisher_ignores_newer_lower_authority_update() -> None:
    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="rest",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: "rest",
                )
            ],
            default_policy=ALL_VALUES,
            authority_policy=authority_policy(),
        )
        now = time.time()
        local = await builder.publisher.publish(
            POSITION,
            "local",
            source="reconciled-local",
            observed_at=now - 10.0,
        )
        stream = await builder.publisher.publish(
            POSITION,
            "stream",
            source="user-data-stream",
            observed_at=now,
        )

        assert local.status is PublishStatus.PUBLISHED
        assert local.value.authority_rank == 300
        assert stream.status is PublishStatus.IGNORED_LOWER_AUTHORITY
        assert stream.value.value == "local"
        assert stream.value.authority_rank == 300

        snapshot = await builder.build([POSITION])
        assert snapshot.value(POSITION) == "local"
        assert snapshot[POSITION].source == "reconciled-local"

    run(scenario())


def test_publish_many_selects_authority_before_timestamp() -> None:
    async def scenario() -> None:
        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="rest",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: "unused",
                )
            ],
            default_policy=ALL_VALUES,
            authority_policy=authority_policy(),
        )
        now = time.time()
        results = await builder.publisher.publish_many(
            (
                ResourceUpdate(
                    POSITION,
                    "rest",
                    source="rest",
                    observed_at=now,
                ),
                ResourceUpdate(
                    POSITION,
                    "local",
                    source="reconciled-local",
                    observed_at=now - 5.0,
                ),
            )
        )

        result = results[POSITION]
        assert result.status is PublishStatus.PUBLISHED
        assert result.value.value == "local"
        assert result.value.authority_rank == 300

    run(scenario())


def test_builder_assigns_authority_to_source_values() -> None:
    builder = SnapshotBuilder(
        [
            CallableSource(
                name="rest",
                priority=1,
                supports=lambda _key: True,
                fetcher=lambda _key, _context: "rest",
            )
        ],
        authority_policy=authority_policy(),
    )

    snapshot = run(builder.build([POSITION]))

    assert snapshot[POSITION].authority_rank == 100


def test_resource_specific_authority_override() -> None:
    default = authority_policy()
    override = SourceAuthorityPolicy(
        source_ranks={
            "reconciled-local": 100,
            "rest": 400,
        }
    )
    resolver = AuthorityPolicyResolver(default, overrides={OTHER: override})

    assert resolver.rank_for(POSITION, "reconciled-local") == 300
    assert resolver.rank_for(POSITION, "rest") == 100
    assert resolver.rank_for(OTHER, "reconciled-local") == 100
    assert resolver.rank_for(OTHER, "rest") == 400


def test_higher_authority_dependency_revision_invalidates_derived_cache() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        rest = snapshot_value(
            POSITION,
            2,
            source="rest",
            observed_at=200.0,
            authority_rank=100,
        )
        derived = snapshot_value(
            RISK,
            4,
            source="derived",
            observed_at=200.0,
            authority_rank=0,
            dependency_versions={POSITION: rest.version},
        )
        await cache.set_many((rest, derived))

        local = snapshot_value(
            POSITION,
            3,
            source="reconciled-local",
            observed_at=100.0,
            authority_rank=300,
        )
        await cache.set(local)

        lookup = await cache.get(RISK, now=200.0, policy=ALL_VALUES)
        assert lookup.value is None

    run(scenario())


def test_authority_policy_requires_authority_aware_custom_cache() -> None:
    class LegacyCache:
        async def get(self, key, *, now, policy):
            return CacheLookup(None, False, False)

        async def set(self, value):
            return None

        async def invalidate(self, key):
            return None

        async def clear(self):
            return None

    with pytest.raises(ValueError, match="validates_source_authority"):
        SnapshotBuilder(
            [
                CallableSource(
                    name="rest",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: "rest",
                )
            ],
            cache=LegacyCache(),
            authority_policy=authority_policy(),
        )


def test_default_policy_preserves_timestamp_only_behavior() -> None:
    async def scenario() -> None:
        cache = AsyncMemoryCache()
        older = snapshot_value(
            POSITION,
            "older",
            source="source-a",
            observed_at=100.0,
            authority_rank=0,
        )
        newer = snapshot_value(
            POSITION,
            "newer",
            source="source-b",
            observed_at=200.0,
            authority_rank=0,
        )
        await cache.set(older)
        result = await cache.set_if_newer(newer)

        assert result.status is CacheWriteStatus.STORED
        assert result.value.value == "newer"

    run(scenario())


def test_builder_rejects_ambiguous_authority_configuration() -> None:
    policy = authority_policy()
    resolver = AuthorityPolicyResolver(policy)

    with pytest.raises(ValueError, match="cannot be provided together"):
        SnapshotBuilder(
            [
                CallableSource(
                    name="rest",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=lambda _key, _context: "rest",
                )
            ],
            authority_policy=policy,
            authority_resolver=resolver,
        )


def test_concurrent_authoritative_publication_supersedes_inflight_rest_result() -> None:
    async def scenario() -> None:
        source_started = asyncio.Event()
        release_source = asyncio.Event()

        async def fetch_rest(_key, _context):
            source_started.set()
            await release_source.wait()
            return "rest"

        builder = SnapshotBuilder(
            [
                CallableSource(
                    name="rest",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch_rest,
                )
            ],
            default_policy=ALL_VALUES,
            authority_policy=authority_policy(),
        )

        pending = asyncio.create_task(builder.build([POSITION]))
        await source_started.wait()
        await builder.publisher.publish(
            POSITION,
            "local",
            source="reconciled-local",
        )
        release_source.set()

        snapshot = await pending
        assert snapshot.value(POSITION) == "local"
        assert snapshot[POSITION].source == "reconciled-local"
        assert snapshot[POSITION].authority_rank == 300
        assert snapshot[POSITION].metadata["superseded_source"] == "rest"

    run(scenario())
