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
