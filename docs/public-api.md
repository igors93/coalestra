# Public API

The supported public surface is exported from `coalestra`.

## Builders

### `SnapshotBuilder`

Create one long-lived builder per acquisition domain.

```python
builder = SnapshotBuilder(
    sources=[...],
    default_policy=FreshnessPolicy(1.0, 10.0),
    max_concurrency=12,
    source_concurrency={"rest": 4},
    source_resilience={"rest": rest_policy},
)

snapshot = await builder.build(
    keys,
    strict=True,
    deadline_seconds=2.0,
    metadata={"request_id": "..."},
)
```

Important attributes:

- `cache`
- `publisher`
- `capacity`
- `circuit_breaker`
- `policy_resolver`
- `resilience_resolver`
- `metrics`
- `events`

Constructor compatibility is preserved for `retry_policy`, `circuit_breaker`, and `max_concurrency`. `max_concurrency` is now builder-wide rather than per build.

### `SnapshotSession`

Created with `builder.session(...)`. Supports incremental `resolve()` calls, one deadline, and pinned values.

```python
async with builder.session(deadline_seconds=3.0) as session:
    await session.resolve(first_stage, strict=False)
    final = await session.resolve(second_stage, strict=False)
```

Methods and properties:

- `resolve(keys, strict=True, retry_errors=False) -> Snapshot`
- `snapshot() -> Snapshot`
- `close()`
- `snapshot_id`
- `created_at`
- `context`
- `closed`

### `SyncSnapshotBuilder` and `SyncSnapshotSession`

Persistent synchronous facades with equivalent build and session operations. `SyncSnapshotBuilder.publisher` exposes a `SyncResourcePublisher` using the same event loop and cache.

## Source protocols

### `SnapshotSource`

```python
async def fetch(key, context) -> SourcePayload
```

### `BatchSnapshotSource`

```python
async def fetch_many(keys, context) -> Mapping[ResourceKey, SourcePayload]
```

A partial mapping is valid. Extra unrequested keys are a protocol error.

### `DerivedSource`

```python
def dependencies(key) -> Collection[ResourceKey]
async def derive(key, dependencies: Snapshot, context) -> SourcePayload
```

### Optional source capabilities

A source may expose:

```python
max_concurrency: int | None
resilience_policy: SourceResiliencePolicy | None
```

Callable adapters accept both as constructor arguments.

## Callable adapters

- `CallableSource`
- `CallableBatchSource`
- `CallableDerivedSource`

Each accepts synchronous or asynchronous callables. Synchronous functions run in worker threads.

## Capacity

### `CapacityLimiter`

- `acquire()`
- `release()`
- `slot()` async context manager
- `snapshot() -> CapacitySnapshot`

### `CapacityController`

- `limit_for(source)`
- `slot(source)` async context manager
- `snapshot() -> dict[str, CapacitySnapshot]`

The special key `"__global__"` identifies global capacity in snapshots.

## Direct publication

### `ResourcePublisher`

- `publish(key, value, *, source, observed_at=None, metadata=None, force=False, replace_equal=False)`
- `publish_update(update, *, force=False, replace_equal=False)`
- `publish_many(updates, *, force=False, replace_equal=False)`
- `invalidate(key, *, reason="")`
- `invalidate_many(keys, *, reason="")`

### `SyncResourcePublisher`

Provides blocking equivalents plus:

```python
submit_publish(...) -> concurrent.futures.Future[PublishResult]
```

### Publication models

- `ResourceUpdate[T]`
- `PublishResult`
- `PublishStatus.PUBLISHED`
- `PublishStatus.IGNORED_OLDER`
- `PublishStatus.IGNORED_DUPLICATE`

## Resource models

- `ResourceKey`
- `FreshnessPolicy`
- `FetchContext`
- `SourcePayload[T]`
- `SnapshotValue[T]`
- `Snapshot`
- `CacheLookup`

`FetchContext.snapshot_id` identifies the enclosing build or session.

`SnapshotValue.version` is an opaque identity for one resolved resource revision. `SnapshotValue.dependency_versions` records the exact dependency revisions used to produce a derived value. Cache implementations use these fields to reject derived entries whose dependencies have changed.

## Cache

- `AsyncMemoryCache`
- `AsyncCache`

## Resilience

### Retry

- `RetryPolicy`

### Circuit breaker

- `CircuitBreaker`
- `CircuitBreakerPolicy`
- `CircuitScope`
- `CircuitIdentity`
- `CircuitSnapshot`
- `CircuitState`

`CircuitBreaker` retains its source-only methods for compatibility. Resource-aware calls accept `key=` and `policy=`.

### Per-source policies

- `SourceResiliencePolicy`
- `ResiliencePolicyResolver`

Precedence:

1. explicit resolver override by source name;
2. dynamic resolver;
3. source-declared policy;
4. default resolver policy.

## Observability

- `NullEventSink`
- `LoggingEventSink`
- `NullMetrics`
- `InMemoryMetrics`

Additional metric names emitted by version 0.3 include:

- `source_capacity_wait_ms`
- `resource_publish_total`
- `resource_invalidation_total`

Additional event types include:

- `source_circuit_open`
- `resource_published`
- `resource_publish_ignored`
- `resource_invalidated`

## Errors

- `CoalestraError`
- `SourceUnavailableError`
- `SourceTimeoutError`
- `SourceProtocolError`
- `CircuitOpenError`
- `DependencyCycleError`
- `DependencyResolutionError`
- `ResourceResolutionError`
- `SnapshotBuildError`
- `SessionClosedError`

## Resource identity in 0.4

### `ResourceKey`

```python
ResourceKey(
    namespace,
    name,
    subject="",
    qualifiers=None,
    *,
    normalizer=None,
)
```

The default normalizer preserves case and strips surrounding whitespace. Public normalizers:

- `PRESERVE_KEY_NORMALIZER`
- `LEGACY_KEY_NORMALIZER`
- `CASE_INSENSITIVE_KEY_NORMALIZER`
- `KeyNormalizer`

Helpers:

- `ResourceKey.legacy(...)`
- `normalized(normalizer)`
- `qualifier(name, default=None)`
- `with_qualifiers(...)`
- `without_qualifiers(...)`

## Cache additions in 0.4

### `BatchAsyncCache`

Optional capability detected by `SnapshotBuilder`:

- `get_many(keys, *, now, policies)`
- `set_many(values)`
- `invalidate_many(keys)`

### `AsyncMemoryCache`

Additional methods:

- `get_many(...)`
- `set_many(...)`
- `invalidate_many(...)`
- `invalidate_matching(predicate)`
- `invalidate_namespace(namespace, *, name=None, subject=None)`
- `prune(*, now, policy_resolver)`
- `stats() -> CacheStats`

The default maximum size is 10,000 entries. `None` keeps it unbounded.

### Refresh

- `RefreshMode.BLOCKING`
- `RefreshMode.STALE_WHILE_REVALIDATE`
- `RefreshMode.REFRESH_AHEAD`

`FreshnessPolicy` additionally accepts `refresh_mode` and `refresh_ahead_seconds`.

`SnapshotBuilder` additions:

- `wait_for_refreshes()`
- `aclose(cancel_refreshes=False)`
- asynchronous context-manager support

`SyncSnapshotBuilder` additionally exposes `wait_for_refreshes()` and waits for pending refreshes on close.

## Snapshot diagnostics

`Snapshot.diagnostics` is a `SnapshotDiagnostics` instance containing:

- duration and observation skew;
- request/result counts;
- cache hit/miss and batch-operation counts;
- stale and coalesced counts;
- source, batch, derived and refresh counts;
- per-source call and latency mappings.

## Buffered observability

### `BufferedEventSink`

- `emit(...)`
- `flush(timeout=None) -> bool`
- `close(timeout=5.0, drain=True) -> bool`
- `stats() -> BufferedSinkStats`

### `BufferedMetricsSink`

Implements `MetricsSink` and exposes the same lifecycle methods.

### Buffer policies

- `BufferOverflowPolicy.DROP_OLDEST`
- `BufferOverflowPolicy.DROP_NEWEST`
- `BufferOverflowPolicy.RAISE`

## Version 0.5 integration APIs

### `SnapshotRequest`

Declares required and optional keys. Use with `SnapshotBuilder.build_request`, `SnapshotSession.resolve_request`, `SyncSnapshotBuilder.build_request`, or `SyncSnapshotSession.resolve_request`.

### Payload isolation

`SnapshotBuilder`, `AsyncMemoryCache`, and `ResourcePublisher` accept an optional `payload_copier`. The default uses `copy.deepcopy` and isolates values plus nested metadata at source, cache, publication, derived-dependency, single-flight, session, and snapshot-delivery boundaries. Copy failures raise `PayloadIsolationError`. Custom copiers should return a deeply independent value unless the payload is already deeply immutable.

### `ObservationPolicy`

Controls tolerance and rejection of source or published timestamps that are ahead of the local clock.

### `BuilderHealth`

Returned by `await builder.health_snapshot()` and `sync_builder.health_snapshot()`. It includes builder state, refresh count, single-flight count, support-cache size, capacity state, cache statistics, and circuit snapshots.

### Adapter options

`CallableSource`, `CallableBatchSource`, and `CallableDerivedSource` accept:

- `cache_supports`: memoize stable `supports(key)` results;
- `run_sync_in_thread`: keep blocking callables out of the event loop, or explicitly run guaranteed non-blocking local reads inline.

`CallableBatchSource` additionally accepts `max_batch_size`.
