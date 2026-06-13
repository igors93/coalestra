# Coalestra

**Coalestra** is a dependency-free Python library for building consistent operational snapshots from multiple read-only data sources.

It helps applications that repeatedly request the same information from event streams, local caches, databases and remote APIs. Coalestra reduces duplicate calls, hides independent I/O latency through bounded concurrency and returns one immutable view of the data used by a unit of work.

The library is domain-agnostic. It does not know about trading, Binance, Alphora, HTTP, SQL or any application model.

> Em português: a Coalestra reúne leituras de várias fontes em um snapshot único, consistente e rastreável. Ela foi criada pensando no Alphora, mas o núcleo não depende dele e pode ser usado por qualquer sistema.

## What it provides

- Immutable snapshots with provenance and freshness metadata.
- Concurrent acquisition of independent resources.
- Single-flight request coalescing for identical resources.
- Priority-based source selection and safe fallback.
- Per-resource TTL and maximum-staleness policies.
- Fresh cache reuse and optional stale-on-error behavior.
- Bounded retries, source timeouts and per-source circuit breakers.
- Replaceable cache, clock, event and metrics interfaces.
- Async API plus a persistent synchronous facade.
- Strict static typing and no runtime dependencies.

## Non-goals

Coalestra deliberately does **not** provide:

- business decisions;
- write or mutation orchestration;
- distributed transactions;
- domain validation;
- authorization or risk decisions.

A trading application may use Coalestra to read prices, positions, orders, balances and exchange rules. It must keep trading decisions and order submission outside the library.

## Requirements

- Python 3.10+

## Install for development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

## Minimal async example

```python
import asyncio

from coalestra import (
    CallableSource,
    FreshnessPolicy,
    ResourceKey,
    SnapshotBuilder,
    SourcePayload,
)

PRICE = ResourceKey("market", "price", "BTCUSDT")


async def stream_fetch(key, context):
    return SourcePayload(
        value={"price": "65000.00"},
        observed_at=context.requested_at,
        metadata={"transport": "websocket"},
    )


async def rest_fetch(key, context):
    return {"price": "65001.00"}


async def main() -> None:
    builder = SnapshotBuilder(
        sources=[
            CallableSource(
                name="stream",
                priority=100,
                supports=lambda key: key.namespace == "market",
                fetcher=stream_fetch,
                timeout_seconds=0.2,
            ),
            CallableSource(
                name="rest",
                priority=10,
                supports=lambda key: key.namespace == "market",
                fetcher=rest_fetch,
                timeout_seconds=2.0,
            ),
        ],
        default_policy=FreshnessPolicy(
            ttl_seconds=1.0,
            max_stale_seconds=5.0,
        ),
        max_concurrency=8,
    )

    snapshot = await builder.build([PRICE], deadline_seconds=2.0)
    price = snapshot[PRICE]

    print(price.value)
    print(price.source)
    print(price.age_seconds)
    print(price.from_cache)


asyncio.run(main())
```

## Synchronous applications

`SyncSnapshotBuilder` owns one dedicated event-loop thread. Keeping that facade alive preserves cache, circuit-breaker and single-flight state across application cycles.

```python
from coalestra import SyncSnapshotBuilder

with SyncSnapshotBuilder(builder) as sync_builder:
    snapshot = sync_builder.build([PRICE], deadline_seconds=2.0)
```

Synchronous source functions are executed in worker threads by `CallableSource`, so they do not block Coalestra's event loop. A timed-out Python thread cannot be forcibly stopped; source implementations should still configure transport-level timeouts.

## Resource identity

Applications define their own vocabulary using stable keys:

```python
ResourceKey(namespace="account", name="balance")
ResourceKey(namespace="market", name="price", subject="BTCUSDT")
ResourceKey(namespace="orders", name="open", subject="ETHUSDT")
```

Keys are normalized and hashable, making them safe for caching and coalescing.

## Source priority and fallback

Higher numeric priority is attempted first. A typical order is:

1. In-process event-stream state.
2. Distributed or local cache.
3. Authoritative remote API.

A source exposes a stable name, a priority, a `supports()` predicate and a `fetch()` operation. `CallableSource` adapts normal functions and coroutines; advanced integrations can implement the `SnapshotSource` protocol directly.

## Freshness semantics

Each resource receives a `FreshnessPolicy`:

- `ttl_seconds`: maximum age considered fresh;
- `max_stale_seconds`: maximum age accepted as emergency fallback;
- `allow_stale_on_error`: whether acceptable stale data may be returned after fresh sources fail.

Each resolved value includes:

```python
SnapshotValue(
    key=...,
    value=...,
    source="user-stream",
    observed_at=...,
    fetched_at=...,
    age_seconds=0.4,
    stale=False,
    from_cache=False,
    latency_ms=2.7,
    attempts=1,
    metadata={...},
)
```

## Project layout

```text
coalestra/
├── src/coalestra/
│   ├── adapters/         # Integration helpers
│   ├── cache/            # Cache implementations
│   ├── core/             # Models, protocols and errors
│   ├── observability/    # Event and metrics sinks
│   ├── orchestration/    # Builder, policies and single-flight
│   ├── resilience/       # Retry and circuit breaker
│   └── sync.py           # Persistent synchronous facade
├── tests/                # Contract and concurrency tests
├── examples/             # Generic and Alphora examples
├── benchmarks/           # Synthetic regression harness
└── docs/                 # Architecture and integration guides
```

## Quality pipeline

```bash
make quality
```

This runs formatting, linting, strict mypy, tests and package build. The same checks run in GitHub Actions for Python 3.10, 3.11 and 3.12.

## Benchmark harness

```bash
PYTHONPATH=src python benchmarks/benchmark_snapshot.py
```

The benchmark compares serial acquisition with bounded concurrent snapshot construction. It is a regression harness, not a production performance claim.

## Documentation

- [Architecture](docs/architecture.md)
- [Public API](docs/public-api.md)
- [Integração com o Alphora](docs/alphora-integration.pt-BR.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

## License

MIT
