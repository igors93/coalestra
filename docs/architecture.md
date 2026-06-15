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

Selective revalidation creates a staging runtime from the current session memo, removes the selected
keys plus every memoized derived dependent, and resolves the affected visible set again under the
original identity and deadline. Normal revalidation can observe newer cache or publisher revisions;
source-forced revalidation bypasses the shared cache. The staged memo is committed atomically only
when all affected visible resources succeed. Failures retain the entire previous session state.

Nested single-flight ownership is inherited while resolving revalidation dependencies. This prevents
a derived resource from waiting on a dependency already owned by the same resolution tree.

## Capacity model

Individual and derived source attempts are scheduled through a fixed worker pool bounded by `max_pending_tasks`. This prevents large key collections from creating one pending asyncio task per resource while preserving input order and cancellation propagation.

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
Publisher reads, writes, atomic writes, and invalidations use fixed-worker fallbacks when a custom cache exposes only single-key methods. Builder-created publishers inherit the builder `max_pending_tasks` limit.

## Cache model

The cache stores acquired, derived, and published `SnapshotValue` instances. A session additionally pins values in its private memo.

The publisher requests cached values with an unbounded freshness window to compare source authority and observation timestamps. Normal builder reads continue to use each resource's configured `FreshnessPolicy`.

## Source-authority model

Acquisition priority and cache authority are separate concerns. Priority determines which source is attempted first when a resource must be resolved. Authority determines whether a newly acquired or published revision may replace the revision already stored for the same key.

Each `SnapshotValue` carries the resolved `authority_rank`. Atomic cache writes compare revisions in this order:

1. an explicit forced write wins;
2. a higher authority rank wins;
3. a lower authority rank is rejected;
4. equal ranks compare `observed_at`;
5. equal rank and timestamp follow `replace_equal`.

This permits policies such as reconciled local state above event streams above REST, or equal local and stream authority with REST below both. Freshness remains orthogonal: a high-authority value can still expire under its `FreshnessPolicy`. Persistent caches must be cleared when authority ranks change because ranks are stored with revisions.

## Consistency model

Coalestra provides acquisition consistency, not a distributed transaction. Independent resources may have different `observed_at` timestamps. Consumers can inspect age and provenance and apply stronger domain constraints.

`SnapshotRequest.consistency_policy` can enforce a maximum observation skew across required resources, or across all resolved request resources when optional inclusion is enabled. The check runs only after required-resource resolution succeeds, preserving the existing required/optional failure precedence. A violation raises `SnapshotConsistencyError` with the complete partial snapshot and the oldest/newest observations.

Transactional session revalidation accepts the same policy for its explicitly selected keys. The candidate values are checked before the staging runtime is committed. A violation therefore leaves the previous session memo and visible resources unchanged. This rule remains orthogonal to freshness: a group can be temporally aligned but old, or individually fresh but too far apart from one another.

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
- `AuthorityPolicyResolver`
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

Single-key custom caches remain supported through ordered fixed-worker fallbacks bounded by `max_pending_tasks`. Reads, writes, atomic writes, and invalidations therefore avoid creating one task per key. Caches implementing `BatchAsyncCache` or `BatchAtomicAsyncCache` continue to use their native bulk operations.

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

## Operational health aggregation

One builder owns a low-cardinality operational tracker shared by source dispatch, custom-cache fallbacks, the event publisher, and session revalidation. `health_snapshot()` reads this tracker, the capacity controller, single-flight state, refresh state, cache statistics, and circuit snapshots without performing source I/O.

Current-state fields include active bounded-dispatch workers and the aggregate number of tasks waiting for capacity. Cumulative fields count queue timeouts, source-call timeouts, exhausted snapshot deadlines, revalidation attempts, and revalidation failures since builder creation. The synchronous facade overlays its current pending-submission count and configured backlog limit.

```text
bounded dispatch workers ----+
capacity limiters -----------+
source timeout outcomes -----+--> BuilderHealth
session revalidation --------+
sync submission backlog -----+
```

The health model remains operational rather than prescriptive. Coalestra reports saturation and failures; the consuming application decides whether to alert, degrade, pause work, or continue. No resource subjects or qualifier values are added to health fields.

## Synchronous submission backlog

The synchronous facade schedules event publications and invalidations on one persistent event-loop thread. Non-blocking `submit_*` calls reserve a slot in a bounded, thread-safe backlog before scheduling work. A full backlog is rejected immediately instead of blocking the producer or accumulating unbounded futures.

```text
producer thread
      |
      | submit_*
      v
bounded submission backlog -> Coalestra event loop -> publisher/cache
```

Publication payloads and nested metadata are isolated on the producer thread before a backlog slot is scheduled. Bulk update and invalidation collections are also materialized at submission time. Accepted work therefore represents the state supplied by the producer at the call boundary, even when the original objects are mutated afterward.

Completed futures release their backlog slots regardless of success, failure, or cancellation. `flush_submissions()` waits for the operations that were pending when the call began. During shutdown the facade atomically stops accepting submissions, drains accepted work up to the shutdown timeout, cancels any remainder, and only then closes the builder and event loop. Application errors are retained on the futures returned to producers and do not disappear during backlog accounting.

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

## Metric cardinality boundary

Default metrics expose resource type, not resource instance identity. The stable labels are `resource_namespace` and `resource_name`. Subjects and qualifier values are excluded because they commonly contain symbols, accounts, tenants, or other unbounded identifiers.

```text
ResourceKey("market", "price", "BTCUSDT", {"venue": "spot"})
        |
        +-- metric labels: market / price
        +-- event resource: market:price:BTCUSDT?venue=spot
```

Structured events retain the full rendered resource key for investigation. This separates aggregated monitoring from detailed diagnostics without removing context. Configured source names remain valid metric labels because the builder owns a bounded source catalog; applications should not create source names from per-resource identifiers.
