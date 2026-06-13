# Architecture

## Goal

Coalestra creates one immutable read model for one unit of work while keeping transport and domain concerns outside the core.

## Source model

Every source exposes a name, priority, optional timeout, and `supports(ResourceKey)` predicate. Three acquisition contracts can coexist in one builder:

### `SnapshotSource`

Resolves one key through `fetch(key, context)`.

### `BatchSnapshotSource`

Resolves a collection through `fetch_many(keys, context)`. Partial results are valid. Missing keys continue through lower-priority sources.

### `DerivedSource`

Declares `dependencies(key)` and computes the requested value through `derive(key, dependency_snapshot, context)`.

Capability selection is deterministic: derived, then batch, then single. Normal applications should expose one capability per source object.

## Resolution pipeline

For a set of requested keys, the builder:

1. Reuses values pinned in the current session runtime.
2. Resolves freshness policy and checks the cache.
3. Reserves unresolved keys independently in `SingleFlight`.
4. Gives the first caller ownership of newly reserved keys.
5. Walks compatible sources in descending priority.
6. Groups all unresolved compatible keys for a batch source.
7. Resolves declared dependencies recursively for a derived source.
8. Applies timeout, retry, and circuit-breaker rules.
9. Accepts fresh values and caches them.
10. Keeps the newest acceptable stale candidate while trying lower sources.
11. Returns stale only when policy allows and fresh resolution failed.
12. Returns immutable values and errors to the caller.

## Per-key single-flight with batches

Batching and coalescing operate together. `SingleFlight.run_many()` reserves each key independently:

```text
request 1: A B
request 2:   B C

owner batch 1: A B
owner batch 2:     C
joined key:        B
```

This prevents duplicate work for overlapping requests without requiring identical key sets.

A cancelled waiter does not cancel shared producer work.

## Snapshot sessions

`SnapshotSession` supports workflows where later resource requirements depend on earlier results.

A session owns:

- one `snapshot_id`;
- one wall-clock creation timestamp;
- one monotonic deadline;
- one concurrency semaphore;
- an internal memo of values acquired directly or as dependencies;
- explicit resources and errors requested by the consumer.

Successful values are pinned for the session, even if their cache TTL expires between stages. Dependency resources remain internal until explicitly requested, preserving the public snapshot contract.

`retry_errors=True` allows explicitly requested failed keys to be attempted again without changing session identity or deadline.

## Derived resource graph

A derived source receives an immutable snapshot containing its dependencies. Dependencies can be resolved by any source type and can form multi-level chains.

Coalestra tracks the ancestry path of each derivation. A direct or indirect cycle produces `DependencyCycleError`; the failed derived source is then treated like any other failed source, allowing lower-priority fallback.

Example:

```text
symbol rules(BTC)
        |
        v
exchange info ---- remote API
```

Several derived keys that depend on the same base key share one acquisition through memoization and single-flight.

## Consistency model

Coalestra provides acquisition consistency, not a distributed transaction. Resources may have different `observed_at` timestamps because independent reads occur concurrently. Consumers may inspect age and provenance and apply stronger domain rules.

## Concurrency and deadlines

Each build creates one session internally. A session has one semaphore shared by all stages and recursive dependency resolution. Batch operations consume one concurrency slot, independent single-resource calls consume one slot each, and derivation execution consumes one slot after dependencies are available.

The deadline is created once per build or session. Timeout calculations use a monotonic clock.

## Cache model

The cache stores both acquired and derived `SnapshotValue` instances. A session additionally pins successful values in an internal memo to avoid re-reading or recomputing them in later stages.

## Extension points

- `SnapshotSource`
- `BatchSnapshotSource`
- `DerivedSource`
- `AsyncCache`
- `Clock`
- `EventSink`
- `MetricsSink`
- `PolicyResolver`

Transport and application adapters should remain outside the core package.
