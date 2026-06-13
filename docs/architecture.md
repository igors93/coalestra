# Architecture

## Design goal

Coalestra creates one immutable read model for one unit of work. It centralizes acquisition concerns without taking ownership of application decisions or mutations.

## Dependency direction

```text
consumer application
        |
        v
application adapters / SnapshotSource implementations
        |
        v
SnapshotBuilder
  |          |            |
  v          v            v
AsyncCache  SingleFlight  resilience
        \       |       /
         v      v      v
        core models and protocols
```

The core imports no transport, framework or application integration.

## Main components

### `ResourceKey`

Stable identity used by caching, policy resolution and request coalescing. The consuming application owns the key vocabulary.

### `SnapshotSource`

Read-only source protocol. Sources declare support for a key and return a `SourcePayload` containing the value, its observation timestamp and optional metadata.

### `SnapshotBuilder`

Coordinates cache lookup, source fallback, bounded concurrency, deadlines, retries, circuit breakers and snapshot assembly.

### `AsyncCache`

Replaceable asynchronous cache contract. The included `AsyncMemoryCache` is a concurrency-safe optional LRU implementation. Redis or another shared cache can be implemented externally without changing builder semantics.

### `SingleFlight`

Maintains at most one in-flight acquisition per resource key in a builder instance. Additional callers join the same future. Cancelling one waiter does not cancel the shared acquisition.

### `PolicyResolver`

Resolves freshness policy through exact overrides, a dynamic resolver or a default policy.

### `SyncSnapshotBuilder`

Long-lived bridge for synchronous systems. It owns a dedicated event loop so cache, circuits and in-flight coordination survive across calls.

## Snapshot construction

For each resource key, the builder:

1. Resolves the freshness policy.
2. Returns a fresh cache entry when available.
3. Joins an existing in-flight acquisition for the same key when present.
4. Finds compatible sources and orders them by descending priority.
5. Checks the source circuit breaker.
6. Applies the smaller of source timeout and remaining snapshot deadline.
7. Runs bounded retries for transient failures.
8. Rejects values older than the fresh TTL and keeps the newest acceptable stale candidate.
9. Returns the first fresh value and caches it.
10. Uses acceptable stale data only when allowed and fresh resolution did not succeed.
11. Produces an immutable snapshot or an aggregate strict-mode error.

## Consistency model

Coalestra provides **acquisition consistency**, not a distributed transaction. Every resource records its own `observed_at`, source and age. Consumers can enforce stronger domain rules after snapshot construction.

A snapshot is immutable, but resources may have slightly different observation times because independent reads happen concurrently.

## Concurrency model

Different resource keys resolve concurrently up to `max_concurrency`. Identical keys are coalesced.

Only reads are parallelized. Coalestra intentionally provides no write orchestration API.

Synchronous callables run through `asyncio.to_thread`. Transport-level timeouts remain necessary because Python cannot forcibly terminate an already-running worker thread.

## Deadline model

The public context exposes a wall-clock deadline for adapters. Internally, timeout calculations use a monotonic clock so wall-clock adjustments do not extend or shorten a build unexpectedly.

## Resilience model

- Retry is bounded and only applied to transient exception classes.
- Circuit breakers are isolated per source name.
- Stale fallback is controlled per resource.
- Strict mode raises one `SnapshotBuildError` containing all unresolved keys.
- Non-strict mode returns partial resources plus an immutable error mapping.

## Observability

Event and metrics sinks are replaceable protocols. The default sinks are no-ops. Custom sinks should be fast and non-blocking because they execute in the acquisition path.

Coalestra does not emit resource values by default. Applications remain responsible for sanitizing custom metadata and exception messages.

## Extension points

- `SnapshotSource`
- `AsyncCache`
- `Clock`
- `EventSink`
- `MetricsSink`
- `PolicyResolver`

New transports and application adapters should remain outside the core package.
