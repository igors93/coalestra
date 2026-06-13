# Public API

The public surface is exported from `coalestra`.

## Builders

### `SnapshotBuilder`

Create one long-lived builder per acquisition domain.

```python
builder = SnapshotBuilder(
    sources=[...],
    default_policy=FreshnessPolicy(1.0, 10.0),
    max_concurrency=8,
)

snapshot = await builder.build(
    keys,
    strict=True,
    deadline_seconds=2.0,
    metadata={"request_id": "..."},
)
```

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

Persistent synchronous facades with equivalent build and session operations.

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

## Callable adapters

- `CallableSource`
- `CallableBatchSource`
- `CallableDerivedSource`

Each accepts synchronous or asynchronous callables. Synchronous functions run in worker threads.

## Models

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

- `RetryPolicy`
- `CircuitBreaker`
- `CircuitState`

## Observability

- `NullEventSink`
- `LoggingEventSink`
- `NullMetrics`
- `InMemoryMetrics`

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
