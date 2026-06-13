# Coalestra

**Coalestra** is a dependency-free Python library for building consistent operational snapshots from multiple read-only data sources.

It reduces duplicate calls, hides independent I/O latency through bounded concurrency, combines batch and single-resource sources, and can derive application resources from already acquired data. The core is domain-agnostic: it does not know about trading, Binance, Alphora, HTTP, SQL, Redis, or any application model.

## Capabilities

- Immutable snapshots with provenance and freshness metadata.
- Single-stage builds and incremental multi-stage `SnapshotSession` workflows.
- Concurrent acquisition of independent resources.
- Per-key single-flight coalescing across overlapping requests.
- Batch sources that resolve many resources with one operation.
- Derived resources with dependency chains and cycle detection.
- Priority-based fallback across single, batch, and derived sources.
- Per-resource TTL and maximum-staleness policies.
- Fresh cache reuse and optional stale-on-error behavior.
- Bounded retries, source timeouts, snapshot deadlines, and circuit breakers.
- Replaceable cache, clock, event, and metrics interfaces.
- Async API plus persistent synchronous facades.
- Strict static typing and no runtime dependencies.

## Installation

```bash
python -m pip install -e ".[dev]"
```

## Single-resource source

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

## Batch acquisition

A batch source receives every unresolved compatible key available at its priority level. It may return a partial mapping; omitted resources automatically continue through lower-priority sources.

```python
from coalestra import CallableBatchSource, ResourceKey, SnapshotBuilder


def price(symbol: str) -> ResourceKey:
    return ResourceKey("market", "price", symbol)


async def fetch_prices(keys, _context):
    symbols = [key.subject for key in keys]
    response = await remote_api.fetch_prices(symbols)
    return {key: response[key.subject] for key in keys if key.subject in response}


builder = SnapshotBuilder(
    [
        CallableBatchSource(
            name="price-api",
            priority=100,
            supports=lambda key: key.namespace == "market" and key.name == "price",
            fetcher=fetch_prices,
        )
    ]
)
```

Custom integrations may implement the `BatchSnapshotSource` protocol directly.

## Incremental snapshot sessions

A session keeps one identity, creation time, deadline, concurrency budget, and internal acquisition memo across multiple stages. Values resolved in an earlier stage are pinned for the rest of the session.

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

A failed key is retained by the session. It can be attempted again explicitly:

```python
await session.resolve([KEY], retry_errors=True)
```

## Derived resources

Derived sources declare dependencies and compute a resource from an immutable dependency snapshot. Dependencies may themselves be cached, batched, fetched, or derived.

```python
from coalestra import CallableDerivedSource, ResourceKey

EXCHANGE_INFO = ResourceKey("exchange", "info")


def rules(symbol: str) -> ResourceKey:
    return ResourceKey("exchange", "rules", symbol)


def derive_rules(key, dependencies, _context):
    exchange_info = dependencies.value(EXCHANGE_INFO, dict)
    return extract_rules(exchange_info, key.subject)


rules_source = CallableDerivedSource(
    name="symbol-rules",
    priority=100,
    supports=lambda key: key.namespace == "exchange" and key.name == "rules",
    dependencies=lambda _key: (EXCHANGE_INFO,),
    deriver=derive_rules,
)
```

Coalestra detects direct and indirect dependency cycles and allows lower-priority sources to act as fallbacks when a derivation cannot be completed.

## Synchronous applications

`SyncSnapshotBuilder` owns a dedicated event-loop thread. Keep it alive for the lifetime of the application so cache, circuits, and single-flight state survive across calls.

```python
from coalestra import SyncSnapshotBuilder

with SyncSnapshotBuilder(builder) as sync_builder:
    snapshot = sync_builder.build([PRICE])

    with sync_builder.session(snapshot_id="cycle-42") as session:
        session.resolve(baseline_keys, strict=False)
        final = session.resolve(selected_keys, strict=False)
```

Synchronous source, batch, and derivation callables are executed in worker threads. Transport-level timeouts are still necessary because Python cannot forcibly terminate an already-running thread.

## Source priority and fallback

All source types share one descending-priority chain:

1. Derived source, batch source, or single source at the highest priority.
2. Remaining unresolved resources proceed to the next compatible source.
3. The newest acceptable stale value is used only when policy permits and no fresh source succeeds.

If a class exposes more than one source capability, Coalestra selects derived first, then batch, then single-resource acquisition.

## Freshness

Every resource uses a `FreshnessPolicy`:

- `ttl_seconds`: maximum age considered fresh;
- `max_stale_seconds`: maximum age accepted as an emergency fallback;
- `allow_stale_on_error`: whether stale data may be returned when fresh resolution fails.

Every resolved value contains its source, observation time, fetch time, age, stale flag, cache flag, latency, attempts, and metadata.

## Architectural boundary

Coalestra owns read acquisition, caching, freshness, coalescing, fallback, derivation, and read concurrency. The consuming application owns business decisions, authorization, risk, writes, transactions, and domain validation.

## Project layout

```text
src/coalestra/
├── adapters/         # Callable single, batch, and derived sources
├── cache/            # Cache implementations
├── core/             # Models, protocols, and errors
├── observability/    # Event and metrics sinks
├── orchestration/    # Builder, session, policy, and single-flight
├── resilience/       # Retry and circuit breaker
└── sync.py           # Persistent synchronous builder and session
```

## Quality pipeline

```bash
make quality
```

This runs formatting, linting, strict mypy, tests, and package build.

## Documentation

- [Architecture](docs/architecture.md)
- [Public API](docs/public-api.md)
- [Integração com o Alphora](docs/alphora-integration.pt-BR.md)
- [Changelog](CHANGELOG.md)

## License

MIT
