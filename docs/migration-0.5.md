# Migration to Coalestra 0.5

Version 0.5 is backward compatible with the 0.4 build, session, source, cache, and publisher APIs. The new APIs are additive, except for one lifecycle correction: closing a `SyncSnapshotBuilder` now closes its underlying builder by default.

## Required and optional resources

Use `SnapshotRequest` when a unit of work can continue without every resource:

```python
request = SnapshotRequest(
    required=[ACCOUNT, ALL_POSITIONS],
    optional=[MARKET_HEALTH, LEARNING_CONTEXT],
)
snapshot = provider.build_request(request, deadline_seconds=3.0)
```

Only failures in `required` raise `SnapshotBuildError`. The error contains `error.snapshot`, including every value that was resolved successfully.

## Synchronous lifecycle

`SyncSnapshotBuilder(builder)` now owns and closes `builder` by default. To preserve the old behavior for a deliberately shared builder:

```python
SyncSnapshotBuilder(builder, close_builder=False)
```

Set `manage_lifecycle=True` on `SnapshotBuilder` when the builder should also close its sources, cache, event sink, and metrics sink.

## Local synchronous sources

Callable adapters still run synchronous callables in the thread pool by default. For lock-protected, non-blocking in-memory reads, opt into inline execution:

```python
CallableSource(..., run_sync_in_thread=False)
CallableBatchSource(..., run_sync_in_thread=False)
```

Never disable thread execution for network, filesystem, database, or other potentially blocking operations.

## Batch limits

Batch adapters accept `max_batch_size`. Coalestra splits large key sets into capacity-aware waves:

```python
CallableBatchSource(..., max_batch_size=100)
```

## Timestamp validation

The default `ObservationPolicy` accepts up to one second of future clock skew and rejects larger future timestamps. Configure it on the builder when systems have a known clock tolerance.

## Source support caching

`source.supports(key)` results are cached by default in a bounded LRU. Set `cache_supports=False` on a dynamic callable source or call `builder.clear_source_support_cache()` after reconfiguration.
