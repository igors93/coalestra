from __future__ import annotations

import asyncio

from coalestra import (
    CallableDerivedSource,
    CallableSource,
    DependencyCycleError,
    DependencyResolutionError,
    FreshnessPolicy,
    ResourceKey,
    SnapshotBuilder,
    SourceUnavailableError,
)
from coalestra.resilience import RetryPolicy

RAW = ResourceKey("raw", "document")
VALUE_A = ResourceKey("derived", "value", "A")
VALUE_B = ResourceKey("derived", "value", "B")
CHAIN = ResourceKey("derived", "chain")


def test_derived_source_resolves_shared_dependency_once() -> None:
    raw_calls = 0

    async def fetch_raw(_key, _context):
        nonlocal raw_calls
        raw_calls += 1
        return {"A": 10, "B": 20}

    async def derive(key, dependencies, _context):
        document = dependencies.value(RAW, dict)
        return document[key.subject]

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="derived",
                    priority=100,
                    supports=lambda key: key in {VALUE_A, VALUE_B},
                    dependencies=lambda _key: (RAW,),
                    deriver=derive,
                ),
                CallableSource(
                    name="raw",
                    priority=10,
                    supports=lambda key: key == RAW,
                    fetcher=fetch_raw,
                ),
            ]
        )
        return await builder.build([VALUE_A, VALUE_B])

    snapshot = asyncio.run(scenario())

    assert snapshot.value(VALUE_A, int) == 10
    assert snapshot.value(VALUE_B, int) == 20
    assert raw_calls == 1
    assert len(snapshot) == 2


def test_derived_resources_can_form_a_chain() -> None:
    async def derive_value(_key, dependencies, _context):
        return dependencies.value(RAW, int) * 2

    async def derive_chain(_key, dependencies, _context):
        return dependencies.value(VALUE_A, int) + 1

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="chain",
                    priority=200,
                    supports=lambda key: key == CHAIN,
                    dependencies=lambda _key: (VALUE_A,),
                    deriver=derive_chain,
                ),
                CallableDerivedSource(
                    name="value",
                    priority=100,
                    supports=lambda key: key == VALUE_A,
                    dependencies=lambda _key: (RAW,),
                    deriver=derive_value,
                ),
                CallableSource(
                    name="raw",
                    priority=1,
                    supports=lambda key: key == RAW,
                    fetcher=lambda _key, _context: 5,
                ),
            ]
        )
        return await builder.build([CHAIN])

    snapshot = asyncio.run(scenario())
    assert snapshot.value(CHAIN, int) == 11


def test_failed_derivation_falls_back_to_lower_priority_direct_source() -> None:
    async def missing_dependency(_key, _context):
        raise SourceUnavailableError("missing")

    async def derive(_key, dependencies, _context):
        return dependencies.value(RAW)

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="derived",
                    priority=100,
                    supports=lambda key: key == VALUE_A,
                    dependencies=lambda _key: (RAW,),
                    deriver=derive,
                ),
                CallableSource(
                    name="raw",
                    priority=90,
                    supports=lambda key: key == RAW,
                    fetcher=missing_dependency,
                ),
                CallableSource(
                    name="fallback",
                    priority=1,
                    supports=lambda key: key == VALUE_A,
                    fetcher=lambda _key, _context: 77,
                ),
            ],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        return await builder.build([VALUE_A])

    snapshot = asyncio.run(scenario())
    assert snapshot.value(VALUE_A, int) == 77
    assert snapshot[VALUE_A].source == "fallback"


def test_dependency_cycle_is_reported_without_deadlock() -> None:
    async def derive(_key, _dependencies, _context):
        return 1

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="cycle",
                    priority=1,
                    supports=lambda key: key in {VALUE_A, VALUE_B},
                    dependencies=lambda key: (VALUE_B,) if key == VALUE_A else (VALUE_A,),
                    deriver=derive,
                )
            ],
            retry_policy=RetryPolicy(max_attempts=1),
        )
        return await builder.build([VALUE_A], strict=False)

    snapshot = asyncio.run(scenario())
    resolution = snapshot.errors[VALUE_A]
    dependency_failure = resolution.failures[0]  # type: ignore[attr-defined]
    assert dependency_failure.error_type == DependencyResolutionError.__name__
    assert DependencyCycleError.__name__ in dependency_failure.message


def test_derived_value_is_cached_across_builds() -> None:
    derivations = 0

    async def derive(_key, dependencies, _context):
        nonlocal derivations
        derivations += 1
        return dependencies.value(RAW, int) * 2

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="derived",
                    priority=100,
                    supports=lambda key: key == VALUE_A,
                    dependencies=lambda _key: (RAW,),
                    deriver=derive,
                ),
                CallableSource(
                    name="raw",
                    priority=1,
                    supports=lambda key: key == RAW,
                    fetcher=lambda _key, _context: 4,
                ),
            ],
            default_policy=FreshnessPolicy(60.0, 60.0),
        )
        first = await builder.build([VALUE_A])
        second = await builder.build([VALUE_A])
        return first, second

    first, second = asyncio.run(scenario())
    assert first.value(VALUE_A, int) == 8
    assert second.value(VALUE_A, int) == 8
    assert second[VALUE_A].from_cache is True
    assert derivations == 1


def test_requested_resource_can_also_be_a_dependency_without_deadlock() -> None:
    raw_calls = 0

    async def fetch_raw(_key, _context):
        nonlocal raw_calls
        raw_calls += 1
        return 3

    async def derive_value(_key, dependencies, _context):
        return dependencies.value(RAW, int) * 2

    async def derive_chain(_key, dependencies, _context):
        return dependencies.value(VALUE_A, int) + 1

    async def scenario():
        builder = SnapshotBuilder(
            [
                CallableDerivedSource(
                    name="chain",
                    priority=200,
                    supports=lambda key: key == CHAIN,
                    dependencies=lambda _key: (VALUE_A,),
                    deriver=derive_chain,
                ),
                CallableDerivedSource(
                    name="value",
                    priority=100,
                    supports=lambda key: key == VALUE_A,
                    dependencies=lambda _key: (RAW,),
                    deriver=derive_value,
                ),
                CallableSource(
                    name="raw",
                    priority=1,
                    supports=lambda key: key == RAW,
                    fetcher=fetch_raw,
                ),
            ]
        )
        return await asyncio.wait_for(builder.build([CHAIN, VALUE_A, RAW]), timeout=1.0)

    snapshot = asyncio.run(scenario())
    assert snapshot.value(RAW, int) == 3
    assert snapshot.value(VALUE_A, int) == 6
    assert snapshot.value(CHAIN, int) == 7
    assert raw_calls == 1
