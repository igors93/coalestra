# Coalestra

**Coalestra** is a dependency-free Python library for building consistent operational snapshots from prioritized read-only sources.

It coalesces duplicate requests, batches compatible resources, derives values from existing resources, bounds read concurrency, isolates failing source partitions, and accepts event-driven updates directly into its cache. The core is domain-agnostic and contains no knowledge of HTTP, SQL, Redis, Binance, trading, or Alphora.

## Capabilities

- Immutable snapshots with provenance and freshness metadata.
- Single-stage builds and incremental multi-stage `SnapshotSession` workflows.
- Per-key single-flight coalescing across overlapping requests.
- Single-resource, batch, and derived source contracts in one priority chain.
- Recursive dependencies, dependency sharing, and cycle detection.
- Builder-wide concurrency limits shared by all builds and sessions.
- Optional per-source concurrency limits.
- Source-specific retry and circuit-breaker policies.
- Circuit isolation by source, namespace, subject, or full resource.
- Case-preserving resource identity with configurable normalization and qualifiers.
- Batch cache reads/writes, bounded LRU storage, pruning, invalidation, and cache statistics.
- Per-resource TTL, stale windows, stale-on-error fallback, and background refresh modes.
- Direct asynchronous and synchronous event publication into the cache.
- Monotonic publication that rejects older or duplicate events by default.
- Consolidated immutable diagnostics on every snapshot.
- Buffered event and metrics sinks that keep downstream I/O outside the acquisition path.
- Replaceable cache, clock, event, and metrics interfaces.
- Async API plus persistent synchronous facades.
- Strict static typing and no runtime dependencies.

## Installation

```bash
python -m pip install -e ".[dev]"
```

Python 3.10 or newer is supported.

## Generic resource identity

`ResourceKey` preserves case by default, trims surrounding whitespace and supports immutable qualifiers:

```python
from coalestra import ResourceKey

candles = ResourceKey(
    "market",
    "candles",
    "BTCUSDT",
    {"interval": "1m", "limit": 500},
)

assert candles.qualifier("interval") == "1m"
```

Qualifiers are normalized into a sorted tuple, so mapping insertion order does not affect equality or hashing. Systems with case-insensitive identity can opt in to a normalizer:

```python
from coalestra import CASE_INSENSITIVE_KEY_NORMALIZER, ResourceKey

key = ResourceKey(
    "Tenant-A",
    "DocumentId",
    "/Path/File",
    normalizer=CASE_INSENSITIVE_KEY_NORMALIZER,
)
```

`LEGACY_KEY_NORMALIZER` and `ResourceKey.legacy(...)` reproduce Coalestra 0.1-0.3 behavior.

## Batch cache operations and refresh policies

`AsyncMemoryCache` performs multi-key reads and writes under one lock, defaults to a bounded 10,000-entry LRU, removes fully expired entries on access, and exposes statistics and namespace invalidation. Custom caches may implement `BatchAsyncCache`; older single-key caches remain supported.

Freshness policies support three refresh modes:

```python
from coalestra import FreshnessPolicy, RefreshMode

policy = FreshnessPolicy(
    ttl_seconds=5.0,
    max_stale_seconds=30.0,
    refresh_mode=RefreshMode.STALE_WHILE_REVALIDATE,
)
```

- `BLOCKING`: wait for a fresh source when the cache is outside TTL.
- `STALE_WHILE_REVALIDATE`: return an acceptable stale value and refresh it in the background.
- `REFRESH_AHEAD`: return a fresh value and refresh it before TTL expiry.

Long-lived asynchronous applications can call `await builder.wait_for_refreshes()`. `SyncSnapshotBuilder.close()` waits for pending refreshes before stopping its event loop.

## Snapshot diagnostics

Every snapshot contains immutable acquisition diagnostics:

```python
snapshot = await builder.build(keys)
print(snapshot.diagnostics.cache_hits)
print(snapshot.diagnostics.source_calls_by_source)
print(snapshot.diagnostics.observation_skew_ms)
```

Diagnostics include requested, resolved and failed resource counts; cache hits/misses and batch operations; stale values and coalesced requests; source, batch and derived calls; refresh outcomes; per-source latency totals; total duration; and observation-time skew.

## Buffered observability

Wrap a potentially slow sink so logging or metrics export does not run on the acquisition path:

```python
from coalestra import BufferedEventSink, BufferedMetricsSink

events = BufferedEventSink(file_event_sink, max_pending=10_000)
metrics = BufferedMetricsSink(prometheus_adapter, max_pending=10_000)

builder = SnapshotBuilder(sources, events=events, metrics=metrics)

# During shutdown
events.close()
metrics.close()
```

The default overflow policy drops the oldest queued record. `DROP_NEWEST` and `RAISE` are also available. Delivery failures are counted and never injected into resource resolution.

## Minimal build

```python
import asyncio

from coalestra import CallableSource, ResourceKey, SnapshotBuilder

PRICE = ResourceKey("market", "price", "BTCUSDT")

builder = SnapshotBuilder(
    [
        CallableSource(
            name="rest",
            priority=10,
            supports=lambda key: key == PRICE,
            fetcher=lambda _key, _context: {"price": "65000.00"},
        )
    ]
)

snapshot = asyncio.run(builder.build([PRICE]))
print(snapshot.value(PRICE, dict))
```

## Batch sources

A batch source receives every unresolved compatible key available at its priority level. It may return a partial mapping; omitted resources continue through lower-priority sources.

```python
from coalestra import CallableBatchSource

async def fetch_prices(keys, _context):
    symbols = [key.subject for key in keys]
    response = await remote_api.fetch_prices(symbols)
    return {key: response[key.subject] for key in keys if key.subject in response}

price_source = CallableBatchSource(
    name="price-api",
    priority=100,
    supports=lambda key: key.namespace == "market" and key.name == "price",
    fetcher=fetch_prices,
)
```

## Incremental sessions

A session keeps one identity, creation time, deadline, and pinned-value memo across multiple stages. Read capacity is controlled by the long-lived builder and is therefore shared with every other active build and session.

```python
async with builder.session(
    snapshot_id="cycle-42",
    deadline_seconds=3.0,
    metadata={"tenant": "example"},
) as session:
    baseline = await session.resolve(baseline_keys, strict=False)
    selected = choose_resources_from(baseline)
    final = await session.resolve(selected, strict=False)
```

Successful values remain pinned inside the session. Existing errors can be retried explicitly:

```python
await session.resolve([KEY], retry_errors=True)
```

## Derived resources

Derived sources declare dependencies and calculate a resource from an immutable dependency snapshot.

```python
from coalestra import CallableDerivedSource, ResourceKey

EXCHANGE_INFO = ResourceKey("exchange", "info")

rules_source = CallableDerivedSource(
    name="symbol-rules",
    priority=100,
    supports=lambda key: key.namespace == "exchange" and key.name == "rules",
    dependencies=lambda _key: (EXCHANGE_INFO,),
    deriver=lambda key, snapshot, _context: extract_rules(
        snapshot.value(EXCHANGE_INFO, dict),
        key.subject,
    ),
)
```

Dependencies may themselves be cached, batched, fetched, or derived. Direct and indirect cycles are rejected.

## Global and per-source capacity

`max_concurrency` is a builder-wide limit. Concurrent calls to `build()` and multiple active sessions share the same capacity.

```python
builder = SnapshotBuilder(
    sources,
    max_concurrency=12,
    source_concurrency={
        "remote-rest": 4,
        "database": 6,
    },
)
```

Callable adapters can also declare their own limit:

```python
rest_source = CallableSource(
    name="remote-rest",
    priority=10,
    supports=supports_rest,
    fetcher=fetch_rest,
    max_concurrency=4,
)
```

An explicit `source_concurrency` entry overrides the limit declared by the source. Batch calls consume one slot regardless of batch size. Derivation consumes a slot only while the derivation function itself runs; dependency acquisition uses its own source slots.

## Source-specific resilience and circuit scopes

```python
from coalestra import (
    CircuitBreakerPolicy,
    CircuitScope,
    RetryPolicy,
    SourceResiliencePolicy,
)

stream_policy = SourceResiliencePolicy(
    retry=RetryPolicy(max_attempts=1),
    circuit=CircuitBreakerPolicy(
        scope=CircuitScope.SUBJECT,
        failure_threshold=2,
        recovery_timeout_seconds=5.0,
    ),
)

stream_source = CallableSource(
    name="market-stream",
    priority=100,
    supports=supports_market,
    fetcher=read_stream_state,
    resilience_policy=stream_policy,
)
```

Available scopes:

- `SOURCE`: one circuit for the complete source;
- `NAMESPACE`: one circuit per source and resource namespace;
- `SUBJECT`: one circuit per source and subject;
- `RESOURCE`: one circuit per complete `ResourceKey`.

A stale payload is treated as an unsuccessful circuit outcome for its configured scope. With `SUBJECT`, stale data for one symbol does not disable the source for other symbols.

Policies can also be supplied centrally through `source_resilience` or a `ResiliencePolicyResolver`.

## Direct event publication

A long-lived builder exposes a `ResourcePublisher` backed by the same cache used by snapshot acquisition.

```python
await builder.publisher.publish(
    PRICE,
    {"price": "65001.25"},
    source="market-stream",
    observed_at=event_timestamp,
    metadata={"sequence": sequence},
)
```

Publication is monotonic by default:

- an older `observed_at` is ignored;
- an equal timestamp is treated as a duplicate;
- `force=True` permits explicit reconciliation or repair;
- `replace_equal=True` permits replacement at the same timestamp.

Several updates can be published together:

```python
from coalestra import ResourceUpdate

await builder.publisher.publish_many(
    [
        ResourceUpdate(PRICE_BTC, btc, source="stream", observed_at=btc_time),
        ResourceUpdate(PRICE_ETH, eth, source="stream", observed_at=eth_time),
    ]
)
```

Uncertain state can be invalidated:

```python
await builder.publisher.invalidate(POSITION_BTC, reason="stream-gap")
```

A session intentionally keeps values already pinned before a publication. New builds and new sessions observe the published value.

## Synchronous applications

`SyncSnapshotBuilder` owns one persistent event-loop thread. Keep it alive for the application lifetime.

```python
from coalestra import SyncSnapshotBuilder

with SyncSnapshotBuilder(builder) as sync_builder:
    sync_builder.publisher.publish(
        PRICE,
        {"price": "65001.25"},
        source="market-stream",
    )

    # Suitable for callbacks that should not block their producer thread.
    future = sync_builder.publisher.submit_publish(
        POSITION,
        position,
        source="user-stream",
        observed_at=event_timestamp,
    )

    snapshot = sync_builder.build([PRICE, POSITION])
    future.result()
```

Synchronous fetchers and derivation functions run in worker threads. Transport-level timeouts are still necessary because an already-running Python thread cannot be forcibly terminated.

## Architectural boundary

Coalestra owns read acquisition, cache publication, freshness, coalescing, fallback, derivation, and read concurrency. The consuming application owns business decisions, authorization, risk, writes, transactions, and domain validation.

## Project layout

```text
src/coalestra/
├── adapters/         # Callable single, batch, and derived sources
├── cache/            # Cache implementations and event publisher
├── concurrency/      # Builder-wide and per-source capacity control
├── core/             # Models, protocols, and errors
├── observability/    # Event and metrics sinks
├── orchestration/    # Builder, session, policy, and single-flight
├── resilience/       # Retry, circuit policies, and circuit breaker
└── sync.py           # Persistent synchronous facades
```

## Quality pipeline

```bash
make quality
```

This runs formatting, linting, strict mypy, tests, and package build.

## Documentation

- [Architecture](docs/architecture.md)
- [Public API](docs/public-api.md)
- [Migration from 0.3](docs/migration-0.4.md)
- [Implementação 7, 8 e 9](docs/implementation-7-8-9.pt-BR.md)
- [Integração com o Alphora](docs/alphora-integration.pt-BR.md)
- [Changelog](CHANGELOG.md)

## License

MIT
