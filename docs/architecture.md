# Architecture

## Goal

Coalestra creates an immutable read model for one unit of work while keeping transport and domain decisions outside the core.

## Source model

Every source exposes a name, priority, optional timeout, and `supports(ResourceKey)` predicate. Three acquisition contracts can coexist:

- `SnapshotSource`: resolves one key through `fetch(key, context)`.
- `BatchSnapshotSource`: resolves a collection through `fetch_many(keys, context)`; partial mappings are valid.
- `DerivedSource`: declares `dependencies(key)` and computes a value through `derive(key, snapshot, context)`.

Capability selection is deterministic: derived, then batch, then single. Normal integrations should expose one capability per source object.

A source may additionally declare:

- `max_concurrency`: its independent capacity ceiling;
- `resilience_policy`: its retry and circuit behavior.

Both declarations are optional and do not alter the base source protocols.

## Resolution pipeline

For requested keys, the builder:

1. Reuses values pinned in the current session runtime.
2. Resolves freshness policy and checks the cache.
3. Reserves unresolved keys independently in `SingleFlight`.
4. Gives the first caller ownership of newly reserved keys.
5. Walks sources in descending priority.
6. Groups unresolved compatible keys for batch sources.
7. Resolves dependencies recursively for derived sources.
8. Applies circuit admission, deadline, timeout, capacity, and retry rules.
9. Records a circuit outcome using the configured failure scope.
10. Accepts fresh values and writes them to the cache.
11. Keeps the newest acceptable stale candidate while trying lower sources.
12. Returns stale only when policy permits and no fresh source succeeds.
13. Returns immutable values and errors.

## Single-flight and cancellation

`SingleFlight.run_many()` reserves each key independently, allowing overlapping batches to share work:

```text
request 1: A B
request 2:   B C

owner batch 1: A B
owner batch 2:     C
joined key:        B
```

A cancelled waiter does not cancel shared producer work. The producer retains its capacity slot until the actual source operation completes or times out. This prevents one consumer from interrupting work still required by another consumer.

## Snapshot sessions

A `SnapshotSession` supports workflows where later requirements depend on earlier results. It owns:

- one `snapshot_id`;
- one wall-clock creation timestamp;
- one monotonic deadline;
- an internal memo of successful values;
- explicit resources and errors requested by the consumer.

Capacity is not session-local. Every session and direct build shares the builder's capacity controller. This prevents simultaneous sessions from multiplying the configured concurrency ceiling.

Successful values are pinned even if their normal cache TTL expires between stages. Dependency resources remain internal until explicitly requested. `retry_errors=True` retries selected failed keys without changing session identity or deadline.

## Capacity model

The builder owns one global `CapacityLimiter` and zero or more source limiters.

Source capacity is acquired before global capacity. A heavily queued source therefore does not consume every global slot while it waits behind its own smaller limit.

```text
source limiter
      |
      v
global limiter
      |
      v
source operation
```

Capacity applies to the actual fetch, batch, or derivation operation. Retry delays do not hold slots. Batch size does not change slot cost.

`CapacityController.snapshot()` exposes current limits, in-use slots, and waiters.

## Circuit identity model

A circuit is identified by the source plus a configurable discriminator:

- `SOURCE`: no resource discriminator;
- `NAMESPACE`: `ResourceKey.namespace`;
- `SUBJECT`: `ResourceKey.subject`;
- `RESOURCE`: the complete key string.

For batch sources, requested keys are grouped by circuit identity before the call. Open identities are removed from the batch while other identities continue. A group succeeds when it returns at least one fresh value; a group containing only omitted or stale values records a failure.

A half-open circuit allows one probe. If that probe is abandoned by cancellation, the circuit returns to open state instead of remaining permanently locked in half-open.

## Source-specific resilience

`SourceResiliencePolicy` combines:

- `RetryPolicy`;
- `CircuitBreakerPolicy`.

Policies may be declared by a source, configured by source name in the builder, or resolved dynamically. Builder-level overrides have precedence over source declarations.

Retries represent one logical circuit attempt. A source failure is recorded only after its configured retries are exhausted.

## Derived resource graph

A derived source receives an immutable snapshot containing its dependencies. Dependencies can be resolved by any source type and can form multi-level graphs.

```text
symbol rules(BTC)
        |
        v
exchange info ---- remote API
```

Several derived keys sharing a base key share one acquisition through memoization and single-flight. Direct and indirect cycles raise `DependencyCycleError`.

## Direct publication

`ResourcePublisher` writes event-driven state into the same cache read by the builder.

Each publication creates a normal `SnapshotValue` with provenance, observation time, current fetch time, freshness, and metadata. Per-key striped locks serialize conflicting publications without retaining an unbounded lock registry.

The default monotonic rule is:

```text
newer timestamp  -> publish
equal timestamp  -> ignore duplicate
older timestamp  -> ignore older update
force=True       -> publish regardless
```

Publication does not mutate existing session memos. This preserves session consistency. A new build or session reads the updated cache.

Invalidation removes a resource from the shared cache, causing normal source resolution on the next request.

## Cache model

The cache stores acquired, derived, and published `SnapshotValue` instances. A session additionally pins values in its private memo.

The publisher requests cached values with an unbounded freshness window only to compare observation timestamps. Normal builder reads continue to use each resource's configured `FreshnessPolicy`.

## Consistency model

Coalestra provides acquisition consistency, not a distributed transaction. Independent resources may have different `observed_at` timestamps. Consumers can inspect age and provenance and apply stronger domain constraints.

## Deadlines and timeouts

A deadline is created once per build or session using a monotonic clock. Source timeout covers waiting for capacity and executing the source call. This ensures queued work cannot outlive the consumer's acquisition deadline.

## Extension points

- `SnapshotSource`
- `BatchSnapshotSource`
- `DerivedSource`
- `AsyncCache`
- `Clock`
- `EventSink`
- `MetricsSink`
- `PolicyResolver`
- `ResiliencePolicyResolver`

Transport and application adapters remain outside the core package.

## Generic resource identity

Resource identity is exact and case-preserving unless the caller supplies a `KeyNormalizer`. This prevents the core from assuming that identifiers behave like market symbols. Qualifiers form an immutable sorted tuple and are part of complete identity.

```text
namespace + name + subject + qualifiers
                    |
                    v
cache / single-flight / circuits / snapshots
```

The legacy lower/lower/upper normalization remains available as an explicit compatibility policy.

## Batch cache path

The cache path mirrors source batching:

1. The builder removes values pinned in the session.
2. It resolves policies for the remaining keys.
3. If the cache implements `BatchAsyncCache`, it performs one `get_many` call.
4. Fresh and refresh-eligible values are classified locally.
5. Fresh source results are grouped into `set_many` operations.
6. Older-than-`max_stale_seconds` memory entries are removed during lookup.

Single-key custom caches remain supported through concurrent fallback calls.

## Refresh state machine

```text
fresh, outside refresh window
        -> return cache

fresh, inside refresh-ahead window
        -> return cache + schedule refresh

stale but inside max-stale, SWR
        -> return stale + schedule refresh

stale in blocking mode
        -> resolve synchronously + stale-on-error fallback

expired
        -> resolve synchronously
```

Background refreshes use the same capacity, resilience and single-flight controls as foreground acquisition. One key has at most one refresh task registered per builder. A refresh must produce a fresh result to be considered successful, and stale refresh results are not written back over the current cache state.

## Diagnostics model

A `DiagnosticsCollector` belongs to one build/session runtime. It records explicit requested keys separately from internal dependencies. `SnapshotSession.snapshot()` freezes the current collector state into `SnapshotDiagnostics`.

Observation skew is the difference between the newest and oldest `observed_at` among resolved resources. It describes temporal consistency but does not enforce a domain threshold.

## Buffered observability model

Buffered sinks preserve the synchronous `EventSink` and `MetricsSink` protocols while moving downstream delivery to a dedicated thread.

```text
acquisition task
      |
      | non-blocking enqueue
      v
bounded queue -> worker thread -> downstream sink
```

The queue is bounded to prevent observability from becoming an unbounded memory leak. Overflow behavior is explicit and measurable. Downstream exceptions are counted by the buffer and never re-enter acquisition control flow.
