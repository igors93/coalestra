# Public API

The stable first-release surface is exported from `coalestra`.

## Construction

### `SnapshotBuilder`

Asynchronous orchestrator. Create one long-lived instance per acquisition domain so its cache, circuit breakers and single-flight registry can be reused.

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

### `SyncSnapshotBuilder`

Persistent synchronous facade. Close it during application shutdown or use it as a context manager.

## Models

### `ResourceKey`

Hashable resource identity: `namespace`, `name`, optional `subject`.

### `FreshnessPolicy`

Fresh TTL, maximum stale window and stale-on-error permission.

### `SourcePayload[T]`

Source-returned value with optional observation time and metadata.

### `SnapshotValue[T]`

Resolved value plus provenance and timing information.

### `Snapshot`

Immutable mapping from `ResourceKey` to `SnapshotValue`. In non-strict mode, unresolved keys are available in `snapshot.errors`.

## Sources

### `CallableSource`

Adapter for synchronous or asynchronous functions.

### `SnapshotSource`

Protocol for custom source classes.

## Cache

### `AsyncMemoryCache`

Concurrency-safe in-memory cache with optional LRU bound.

### `AsyncCache`

Protocol for external cache implementations.

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
- `CircuitOpenError`
- `ResourceResolutionError`
- `SnapshotBuildError`
