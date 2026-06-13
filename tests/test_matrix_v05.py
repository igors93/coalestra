from __future__ import annotations

import asyncio
from itertools import product

import pytest

from coalestra import (
    CASE_INSENSITIVE_KEY_NORMALIZER,
    LEGACY_KEY_NORMALIZER,
    PRESERVE_KEY_NORMALIZER,
    AsyncMemoryCache,
    CallableBatchSource,
    FreshnessPolicy,
    RefreshMode,
    ResourceKey,
    SnapshotBuilder,
    SnapshotValue,
)
from coalestra.resilience import CircuitBreaker, CircuitBreakerPolicy, CircuitScope, RetryPolicy


def run(coro):
    return asyncio.run(coro)


# 120 identity cases: 40 inputs x 3 normalization policies.
_KEY_INPUTS = [
    (
        f" Namespace-{index % 7} ",
        f" Name-{index % 11} ",
        f"Subject-{index}",
        {" Interval ": f" {index % 5}m ", "Limit": index + 1},
    )
    for index in range(40)
]
_NORMALIZERS = [
    ("preserve", PRESERVE_KEY_NORMALIZER),
    ("legacy", LEGACY_KEY_NORMALIZER),
    ("casefold", CASE_INSENSITIVE_KEY_NORMALIZER),
]


@pytest.mark.parametrize(
    ("namespace", "name", "subject", "qualifiers", "normalizer_name", "normalizer"),
    [
        (*case, normalizer_name, normalizer)
        for case in _KEY_INPUTS
        for normalizer_name, normalizer in _NORMALIZERS
    ],
    ids=lambda value: str(value),
)
def test_resource_key_identity_matrix(
    namespace: str,
    name: str,
    subject: str,
    qualifiers: dict[str, object],
    normalizer_name: str,
    normalizer,
) -> None:
    key = ResourceKey(namespace, name, subject, qualifiers, normalizer=normalizer)
    rebuilt = ResourceKey(
        key.namespace,
        key.name,
        key.subject,
        reversed(key.qualifiers),
        normalizer=normalizer,
    )
    assert key == rebuilt
    assert hash(key) == hash(rebuilt)
    assert len(key.qualifiers) == 2
    assert "?" in str(key)
    if normalizer_name == "legacy":
        assert key.namespace == namespace.strip().lower()
        assert key.name == name.strip().lower()
        assert key.subject == subject.strip().upper()
        assert key.qualifier("INTERVAL") == str(qualifiers[" Interval "]).strip()
    elif normalizer_name == "casefold":
        assert key.namespace == namespace.strip().casefold()
        assert key.subject == subject.strip().casefold()
    else:
        assert key.namespace == namespace.strip()
        assert key.subject == subject.strip()


# 90 freshness boundary cases: 5 TTLs x 6 ages x 3 modes.
_FRESHNESS_CASES = list(
    product(
        (0.0, 0.1, 1.0, 5.0, 30.0),
        (0.0, 0.05, 0.1, 0.5, 5.0, 31.0),
        tuple(RefreshMode),
    )
)


@pytest.mark.parametrize(("ttl", "age", "mode"), _FRESHNESS_CASES)
def test_freshness_policy_boundary_matrix(ttl: float, age: float, mode: RefreshMode) -> None:
    refresh_ahead = ttl / 2 if mode is RefreshMode.REFRESH_AHEAD else 0.0
    policy = FreshnessPolicy(
        ttl_seconds=ttl,
        max_stale_seconds=max(ttl, 60.0),
        refresh_mode=mode,
        refresh_ahead_seconds=refresh_ahead,
    )
    assert (age <= policy.ttl_seconds) is (age <= ttl)
    expected_refresh = (
        mode is RefreshMode.REFRESH_AHEAD
        and age >= max(0.0, ttl - refresh_ahead)
        and ttl != float("inf")
    )
    assert policy.should_refresh_ahead(age) is expected_refresh


# 100 cache classification cases: 20 ages x 5 TTL/stale windows.
_CACHE_POLICIES = [
    FreshnessPolicy(0.0, 0.0),
    FreshnessPolicy(0.0, 5.0),
    FreshnessPolicy(1.0, 1.0),
    FreshnessPolicy(1.0, 10.0),
    FreshnessPolicy(5.0, 20.0),
]
_CACHE_AGES = [index * 0.75 for index in range(20)]


@pytest.mark.parametrize(("age", "policy"), list(product(_CACHE_AGES, _CACHE_POLICIES)))
def test_memory_cache_freshness_matrix(age: float, policy: FreshnessPolicy) -> None:
    async def scenario() -> None:
        key = ResourceKey(
            "cache", "matrix", f"{age}-{policy.ttl_seconds}-{policy.max_stale_seconds}"
        )
        cache = AsyncMemoryCache(max_entries=10)
        now = 100.0
        value = SnapshotValue(
            key=key,
            value=age,
            source="test",
            observed_at=now - age,
            fetched_at=now - age,
            age_seconds=0.0,
            stale=False,
            from_cache=False,
            latency_ms=0.0,
        )
        await cache.set(value)
        lookup = await cache.get(key, now=now, policy=policy)
        if age > policy.max_stale_seconds:
            assert lookup.value is None
            assert lookup.fresh is False
            assert lookup.usable_stale is False
        else:
            assert lookup.value is value
            assert lookup.fresh is (age <= policy.ttl_seconds)
            assert lookup.usable_stale is True

    run(scenario())


# 80 circuit identity cases: 20 resources x 4 scopes.
_CIRCUIT_KEYS = [
    ResourceKey(
        f"ns-{index % 4}",
        f"name-{index % 3}",
        f"subject-{index % 5}",
        {"variant": index},
    )
    for index in range(20)
]


@pytest.mark.parametrize(("key", "scope"), list(product(_CIRCUIT_KEYS, tuple(CircuitScope))))
def test_circuit_identity_scope_matrix(key: ResourceKey, scope: CircuitScope) -> None:
    breaker = CircuitBreaker(default_policy=CircuitBreakerPolicy(scope=scope))
    identity = breaker.identity_for("source", key=key, scope=scope)
    assert identity.source == "source"
    assert identity.scope is scope
    if scope is CircuitScope.SOURCE:
        assert identity.discriminator == ""
    elif scope is CircuitScope.NAMESPACE:
        assert identity.discriminator == key.namespace
    elif scope is CircuitScope.SUBJECT:
        assert identity.discriminator == key.subject
    else:
        assert identity.discriminator == str(key)


# 60 batch chunking cases: 1..20 keys x chunk sizes 1, 3, 7.
_BATCH_CASES = list(product(range(1, 21), (1, 3, 7)))


@pytest.mark.parametrize(("key_count", "chunk_size"), _BATCH_CASES)
def test_batch_chunking_matrix(key_count: int, chunk_size: int) -> None:
    async def scenario() -> None:
        keys = tuple(ResourceKey("batch", "item", str(index)) for index in range(key_count))
        calls: list[int] = []

        async def fetch_many(requested, _context):
            calls.append(len(requested))
            return {key: key.subject for key in requested}

        builder = SnapshotBuilder(
            [
                CallableBatchSource(
                    name="batch",
                    priority=1,
                    supports=lambda _key: True,
                    fetcher=fetch_many,
                    max_batch_size=chunk_size,
                )
            ],
            max_concurrency=4,
        )
        snapshot = await builder.build(keys)
        assert len(snapshot) == key_count
        assert all(size <= chunk_size for size in calls)
        assert sum(calls) == key_count
        expected_calls = (key_count + chunk_size - 1) // chunk_size
        assert len(calls) == expected_calls
        assert snapshot.diagnostics.batch_chunks == expected_calls

    run(scenario())


# 80 retry-delay cases: 20 attempts x 4 backoff configurations.
_RETRY_CONFIGS = [
    (0.0, 0.0),
    (0.01, 0.1),
    (0.1, 0.5),
    (1.0, 2.0),
]
_RETRY_CASES = list(product(range(1, 21), _RETRY_CONFIGS))


@pytest.mark.parametrize(("attempt", "config"), _RETRY_CASES)
def test_retry_delay_matrix(attempt: int, config: tuple[float, float]) -> None:
    base, maximum = config
    policy = RetryPolicy(
        max_attempts=25,
        base_delay_seconds=base,
        max_delay_seconds=maximum,
        jitter_ratio=0,
    )
    delay = policy.delay_for_attempt(attempt)
    expected = min(maximum, base * (2 ** max(0, attempt - 1)))
    assert delay == expected
    assert 0 <= delay <= maximum
