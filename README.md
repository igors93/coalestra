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
- Optional observation-skew limits for temporally coherent request groups and revalidation.
- Buffered event and metrics sinks that keep downstream I/O outside the acquisition path.
- Replaceable cache, clock, event, and metrics interfaces.
- Async API plus persistent synchronous facades.
- Required/optional resource requests for integration-safe partial snapshots.
- Deadline-aware retries, bounded batch chunking, and future-timestamp validation.
- Health snapshots for cache, circuits, capacity, refreshes, and in-flight work.
- Fast inline execution for explicitly non-blocking local synchronous sources.
- Strict static typing and no runtime dependencies.

## Installation

```bash
python -m pip install -e ".[dev]"
```

Python 3.10 or newer is supported.

## Integration-ready requests

Use `SnapshotRequest` to separate resources that must exist from resources that may fail without aborting the unit of work:

```python
from coalestra import SnapshotRequest

request = SnapshotRequest(
    required=[ACCOUNT, ALL_POSITIONS],
    optional=[MARKET_HEALTH, LEARNING_CONTEXT],
)

snapshot = await builder.build_request(request, deadline_seconds=3.0)
```

Only required failures raise `SnapshotBuildError`. The exception exposes a partial `snapshot`, so already resolved values and diagnostics are not lost. Sessions and synchronous facades expose the same request API.

### Temporal consistency

A request can reject a group whose resolved values were observed too far apart in time:

```python
from coalestra import SnapshotConsistencyPolicy, SnapshotRequest

request = SnapshotRequest(
    required=[ACCOUNT, POSITION, OPEN_ORDERS],
    optional=[MARKET_HEALTH],
    consistency_policy=SnapshotConsistencyPolicy(
        max_observation_skew_seconds=2.0,
    ),
)
```

The default scope contains required resources only. Set `include_optional_resources=True` when every resolved optional value should participate. A violation raises `SnapshotConsistencyError`, which remains a `SnapshotBuildError`, exposes the complete partial snapshot, and identifies the oldest and newest resources. The feature is opt-in, so existing requests keep their current behavior.

Selective revalidation can enforce the same invariant before committing refreshed values:

```python
updated = await session.revalidate(
    [POSITION, OPEN_ORDERS, ACCOUNT],
    consistency_policy=SnapshotConsistencyPolicy(2.0),
)
```

A revalidation consistency failure always retains the previous session state and raises `SnapshotConsistencyError`, including when `strict=False`. Freshness and observation skew remain separate checks: freshness limits how old one value may be, while skew limits how far apart a group of values may be.

### Versioned error diagnostics

`SourceFailure.to_dict()`, `ResourceResolutionError.to_dict()`, and `SnapshotBuildError.to_dict()` return a stable JSON-safe schema identified by `ERROR_DIAGNOSTICS_SCHEMA` and `ERROR_DIAGNOSTICS_SCHEMA_VERSION`. Consumers should branch on `schema_version` and ignore unknown fields so future additive changes remain compatible.

```python
from coalestra import ERROR_DIAGNOSTICS_SCHEMA_VERSION, SnapshotBuildError

try:
    snapshot = await builder.build_request(request)
except SnapshotBuildError as error:
    diagnostic = error.to_dict()
    assert diagnostic["schema_version"] == ERROR_DIAGNOSTICS_SCHEMA_VERSION
    send_to_observability(diagnostic)
```

`partial_snapshot_available` is the canonical field. Schema version 1 also includes the legacy `has_partial_snapshot` alias for consumers created against Coalestra 0.5.1-0.5.4. Serialized diagnostics contain only strings, integers, booleans, lists, and dictionaries; Python exception objects are never included.

## Fast local sources and bounded batches

Synchronous adapters run in worker threads by default. Lock-protected, non-blocking in-memory reads can opt into inline execution:

```python
local_source = CallableBatchSource(
    name="market-state",
    priority=100,
    supports=supports_market,
    fetcher=read_local_market_state,
    run_sync_in_thread=False,
    max_batch_size=100,
)
```

Do not use inline execution for network, filesystem, database, or any potentially blocking operation. Large batches are split into capacity-aware waves, avoiding unbounded task creation.

## Timestamp precision

`ObservationPolicy` rejects observations too far in the future, preventing clock errors from making values artificially fresh. Small accepted clock differences are recorded as `clock_skew_seconds` metadata.

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

## Payload isolation

Coalestra deep-copies payload values and nested metadata when data crosses ownership boundaries. Source results, cache entries, publisher results, derived dependency snapshots, single-flight callers, and session snapshots therefore do not share mutable payload objects by default. Mutating one returned snapshot cannot modify the cache or another snapshot.

Payloads must support `copy.deepcopy`. A payload that cannot be copied is reported as a structured `PayloadIsolationError` instead of being stored by reference. Integrations that use proven immutable values or specialized model-copying APIs may provide a custom copier:

```python
from coalestra import SnapshotBuilder

builder = SnapshotBuilder(
    sources,
    payload_copier=lambda value: value.model_copy(deep=True),
)
```

`AsyncMemoryCache` and standalone `ResourcePublisher` instances accept the same `payload_copier` option. Returning the original object from a custom copier is safe only when the payload is deeply immutable.

## Source authority

Source priority controls acquisition order. Source authority independently controls which revision
may remain in the shared cache when local state, real-time events, and remote reads disagree.
Higher ranks win even when their observation timestamp is older. Sources with the same rank retain
the existing timestamp-monotonic behavior.

```python
from coalestra import SnapshotBuilder, SourceAuthorityPolicy

authority = SourceAuthorityPolicy(
    source_ranks={
        "reconciled-local": 300,
        "user-data-stream": 200,
        "binance-rest": 100,
    }
)

builder = SnapshotBuilder(
    sources,
    authority_policy=authority,
)
```

A common alternative is to assign local reconciled state and real-time events the same rank so the
newest of those two wins, while keeping REST at a lower rank. Resource-specific rules use
`AuthorityPolicyResolver` with exact-key overrides or a dynamic resolver.

`SnapshotValue.authority_rank` records the rank used for the cached revision. `force=True` remains an
explicit administrative override and can replace a higher-authority value. Freshness is still
independent: authority decides write precedence, while `FreshnessPolicy` decides whether a stored
value is usable by a reader.

Custom caches used with configured authority rules must implement the same atomic comparison and
declare `validates_source_authority = True`. Clear persistent caches after changing authority ranks,
because existing entries retain the rank assigned when they were written.

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

### Low-cardinality metric labels

Default metrics describe resource types with `resource_namespace` and `resource_name`. They never include `ResourceKey.subject`, qualifier values, symbols, account identifiers, or the rendered full key. Detailed resource identity remains available in structured events.

```text
metric labels: resource_namespace="market", resource_name="price"
event payload: resource="market:price:BTCUSDT?venue=spot"
```

This keeps metric series bounded when an application observes many symbols or accounts. Source labels should also use stable configured source names rather than per-request identifiers. Existing dashboards that query the former `resource` metric label must migrate to the two resource-type labels.

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

Critical resources can be revalidated without replacing unrelated pinned values:

```python
updated = await session.revalidate([POSITION, OPEN_ORDERS])
```

Revalidation reads the newest shared-cache or published revisions by default. Use
`force_refresh=True` when the selected resources must bypass the shared cache and be acquired from
the source chain again:

```python
updated = await session.revalidate(
    [POSITION, OPEN_ORDERS],
    force_refresh=True,
)
```

Pinned derived values that depend on a selected resource are refreshed transitively. The operation
is transactional: all affected visible values are committed together, or the previous session state
is retained. With `strict=False`, a failed attempt returns the retained values plus transient errors
for that call; those errors are not stored in the session.

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

Every `SnapshotValue` carries an opaque resource version. Derived values record the versions used for each dependency. A cached derived value is accepted only while every recorded dependency version is still current. Publishing or caching a newer dependency therefore invalidates affected derived values lazily on their next read, including transitive derived chains.

## Global and per-source capacity

`max_concurrency` is a builder-wide limit. Concurrent calls to `build()` and multiple active sessions share the same capacity. `max_pending_tasks` bounds the fixed worker pool used to dispatch individual and derived resources, preventing one large request from creating one asyncio task per key. It defaults to `max_concurrency`.

```python
builder = SnapshotBuilder(
    sources,
    max_concurrency=12,
    max_pending_tasks=12,
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

An explicit `source_concurrency` entry overrides the limit declared by the source. Batch calls consume one slot regardless of batch size. Derivation consumes a slot only while the derivation function itself runs; dependency acquisition uses its own source slots. Individual and derived dispatch preserve input ordering while using at most `max_pending_tasks` workers per source attempt. Values above `max_concurrency` permit a bounded number of workers to wait during retries or capacity contention; lower values deliberately reduce dispatch parallelism.

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

`SyncSnapshotBuilder` owns one persistent event-loop thread. Keep it alive for the application lifetime. Non-blocking publisher submissions use a bounded backlog so event-producing threads cannot create unbounded work.

```python
from coalestra import ResourceUpdate, SyncSnapshotBuilder

with SyncSnapshotBuilder(
    builder,
    max_pending_submissions=256,
) as sync_builder:
    # Blocking publication remains available for callers that need the result immediately.
    sync_builder.publisher.publish(
        PRICE,
        {"price": "65001.25"},
        source="market-stream",
    )

    # Non-blocking operations return concurrent.futures.Future objects.
    publication = sync_builder.publisher.submit_publish_many(
        (
            ResourceUpdate(POSITION, position, source="user-stream"),
            ResourceUpdate(OPEN_ORDERS, orders, source="user-stream"),
        )
    )
    invalidation = sync_builder.publisher.submit_invalidate(
        ACCOUNT,
        reason="stream-gap",
    )

    # Wait for operations accepted before this call when a synchronization point is required.
    sync_builder.publisher.flush(timeout_seconds=1.0)
    publication.result()
    invalidation.result()
```

The available non-blocking methods are `submit_publish()`, `submit_publish_update()`, `submit_publish_many()`, `submit_invalidate()`, and `submit_invalidate_many()`. Publication values, nested metadata, update collections, and invalidation-key collections are captured before the submission call returns, so later producer-side mutation cannot change accepted work. When the backlog is full, submission fails immediately with `SubmissionBacklogFullError`; the producer thread is never silently blocked. `pending_submissions` exposes the current backlog size. Closing the facade stops accepting new submissions, drains accepted work up to `shutdown_timeout_seconds`, then cancels any remaining submissions before stopping the event loop. Exceptions raised by asynchronous operations remain available from their returned futures.

Synchronous fetchers and derivation functions run in worker threads by default. `run_sync_in_thread=False` is available only for guaranteed non-blocking local reads. Transport-level timeouts remain necessary because an already-running Python thread cannot be forcibly terminated. Closing the synchronous facade closes its underlying builder by default.

## Operational health

`await builder.health_snapshot()` returns an immutable, aggregated view without calling any source. In addition to cache, circuit, refresh, capacity, and single-flight state, it reports current dispatch workers and capacity waiters plus cumulative timeout and session-revalidation counters.

```python
health = await builder.health_snapshot()

print(health.active_dispatch_workers)
print(health.waiting_for_capacity)
print(health.queue_timeout_count)
print(health.source_timeout_count)
print(health.deadline_exceeded_count)
print(health.revalidation_failure_count)
```

Payload-copy health is exposed both as builder-wide aggregates and as bounded component snapshots. The `builder` component covers source, publisher, derived, custom-cache boundary, and asynchronous session-delivery copies. The `cache` component is present when the default `AsyncMemoryCache` exposes its independent copy limiter.

```python
health = await builder.health_snapshot()

print(health.active_payload_copies)
print(health.waiting_for_copy_capacity)
print(health.payload_copy_timeout_count)

builder_copies = health.payload_copy_components["builder"]
print(builder_copies.max_concurrency)
print(builder_copies.peak_active_copies)
print(builder_copies.average_wait_ms)
print(builder_copies.max_duration_ms)
```

`PayloadCopyHealth` separates current activity from cumulative outcomes. A timed-out caller may later be followed by a completed or failed worker because Python cannot safely terminate a thread that has already started; timeout and completion counters are therefore intentionally not mutually exclusive. Component names are fixed and low-cardinality.

Payload-copy shutdown is controlled by `SnapshotBuilder.copy_shutdown_timeout_seconds` (default `5.0`). Closing a builder stops new copy submissions, interrupts operations waiting for copy capacity, and waits for already-started workers. If they do not drain in time, `PayloadCopyShutdownTimeoutError` reports the active component counts while health retains `shutdown_incomplete`, `shutdown_timeout_count`, and `active_at_last_shutdown_timeout`. A later worker completion automatically transitions that component to `shutdown_complete`.

```python
builder = SnapshotBuilder(
    sources,
    copy_shutdown_timeout_seconds=3.0,
)

await builder.aclose()
```

Standalone `AsyncMemoryCache` and `ResourcePublisher` instances close their owned copy runners through `aclose()`. Builder-created publishers share the builder runner and do not close it independently. The synchronous facade defers stopping its event loop after a shutdown timeout until late copy workers have actually finished.

`sync_builder.health_snapshot()` adds `pending_submissions` and `max_pending_submissions` from the synchronous non-blocking publication backlog. Counters are process-local and cumulative since builder creation. The snapshot intentionally exposes aggregates only; it does not include symbols, subjects, qualifiers, or business decisions.


### Versioned health serialization and severity

`BuilderHealth.to_dict()` returns an official JSON-safe schema identified by `BUILDER_HEALTH_SCHEMA` and `BUILDER_HEALTH_SCHEMA_VERSION`. Nested dataclasses, enums, immutable mappings, circuit identities, cache statistics, copy components, and observability buffers are normalized automatically, so consumers do not need to maintain a manual list of fields whenever Coalestra adds health signals.

```python
previous = None
health = await builder.health_snapshot()
payload = health.to_dict(previous=previous)

print(payload["schema"])
print(payload["assessment"]["severity"])
previous = health
```

`BuilderHealth.assess()` returns `BuilderHealthAssessment` with one of three stable severities: `healthy`, `degraded`, or `critical`. Current-state checks cover closed builders, capacity waiters, submission and observability backlog ratios, open or half-open circuits, stopped observability workers, and incomplete shutdowns. Cumulative counters are assessed only when a previous snapshot is supplied; this reports new failures without making one historical timeout permanently degrade every later health sample.

```python
assessment = health.assess(previous=previous)

for finding in assessment.findings:
    print(finding.reason.value, finding.severity.value, finding.observed)
```

Thresholds are configurable through `BuilderHealthAssessmentPolicy`. The default policy is conservative and generic; applications remain responsible for deciding which business actions follow a degraded or critical result.

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

## Quality and release pipeline

```bash
make quality
```

The quality gate checks formatting without modifying files, runs lint and strict mypy, verifies the 700-test minimum, executes deterministic concurrency regressions and the complete suite, validates version consistency, builds both distributions, installs the wheel in a clean virtual environment, and checks the exact public API manifest.

```bash
make release-check
```

The release gate uses the same checks locally. GitHub Actions repeats the supported-Python test matrix on Python 3.10-3.13. Distribution artifacts are built only after that matrix succeeds. Tag builds additionally require `vX.Y.Z` to match `pyproject.toml`, `coalestra.__version__`, and the dated changelog section. Verified wheel and source-distribution artifacts are uploaded by the workflow.

## Documentation

- [Architecture](docs/architecture.md)
- [Public API](docs/public-api.md)
- [Migration from 0.4](docs/migration-0.5.md)
- [Revisão final para integração](docs/integration-readiness.pt-BR.md)
- [Integração com o Alphora](docs/alphora-integration.pt-BR.md)
- [Changelog](CHANGELOG.md)

## License

MIT
