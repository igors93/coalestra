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

A deadline is created once per build or session using a monotonic clock. The same absolute budget covers source-capacity waits, source execution, cache-boundary operations, bounded payload-copy capacity, payload-copy execution, derived dependency isolation, and asynchronous snapshot delivery. This prevents work after acquisition from silently extending the consumer's total wait.

Worker threads that have already started cannot be terminated safely. When a copy reaches the snapshot deadline, the caller stops waiting and receives `SnapshotDeadlineExceededError`, while the worker keeps its copy-capacity slot until it really finishes. This preserves the configured concurrency bound during timeout or cancellation storms.

A partial diagnostic snapshot is a compatibility exception: when acquisition has already failed because the deadline was exhausted, asynchronous delivery may copy the retained state without reapplying the exhausted budget. The operation remains failed and the diagnostic copy cannot commit new values. Successful revalidation copies its candidate delivery snapshot before committing the staged memo and visible resources, so a delivery timeout leaves the previous session state intact.

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

`AsyncMemoryCache` keeps structural work inside its lock: lookup classification, dependency validation, LRU updates, authority comparison, entry replacement, and eviction. Payload and metadata isolation copies run outside the lock. Writes prepare only candidates that currently qualify for storage, then reacquire the lock and recheck the full batch before committing. This prevents slow copies from extending the critical section without weakening monotonic authority or timestamp ordering.

The built-in `copy.deepcopy` path runs through bounded `asyncio.to_thread` workers by default. A per-cache semaphore limits active copies, and fixed-worker batch dispatch prevents one large cache operation from creating one task per resource. If a caller is cancelled after a worker thread starts, the thread continues because Python cannot terminate it safely; its semaphore slot remains reserved until completion. Custom payload copiers remain inline unless threaded execution is explicitly enabled, preserving compatibility with thread-affine implementations.

## Cross-boundary payload copy model

The builder owns a second bounded copy runner for ownership boundaries outside the cache. Source payloads are copied after source capacity is released. Custom-cache values are copied before entering builder state and before cache writes. Publisher candidates, cache handoff values, and returned publication results use the same runner. Derived sources receive dependency snapshots copied through that runner, and asynchronous session delivery uses it before exposing values to callers.

The runner uses a fixed worker set and one shared semaphore per builder, so a large batch does not create one task or one thread per resource. Cancellation cannot terminate an already-running Python thread; the corresponding capacity slot remains reserved until the copy finishes. This prevents cancellation storms from bypassing the configured limit.

The default copier is offloaded automatically. Custom copiers remain inline unless explicitly marked safe for worker threads. `SnapshotSession.snapshot_async()` uses the bounded runner, while the compatibility `snapshot()` method remains synchronous because it cannot await worker completion.

Every builder-managed asynchronous copy accepts the session's absolute monotonic deadline. The limit includes both time queued behind the shared copy semaphore and time waiting for the worker result. Cache calls are wrapped by the same deadline so copies performed by the default `AsyncMemoryCache` also remain inside the snapshot budget. The synchronous compatibility method `SnapshotSession.snapshot()` cannot await or interrupt work and therefore does not enforce asynchronous copy deadlines.

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

Current-state fields include active bounded-dispatch workers, source-capacity waiters, active payload copies, and payload-copy capacity waiters. Cumulative fields count queue timeouts, source-call timeouts, exhausted snapshot deadlines, revalidation attempts, copy starts, copy completions, copy failures, and copy timeouts since builder creation. The synchronous facade overlays its current pending-submission count and configured backlog limit.

```text
bounded dispatch workers ----+
capacity limiters -----------+
source timeout outcomes -----+
payload copy runners --------+--> BuilderHealth
session revalidation --------+
sync submission backlog -----+
```

`payload_copy_components` contains fixed low-cardinality snapshots. The `builder` component represents the shared isolator used outside the cache, while the `cache` component represents the default memory cache's independent limiter. Each `PayloadCopyHealth` reports current and peak activity, waiting, cumulative successes and failures, caller timeouts, capacity timeouts, and average/maximum wait and execution durations. Aggregate `BuilderHealth` counters are sums across the available components.

A caller timeout does not imply that the underlying worker stopped. The health model therefore permits one operation to increment `timeout_count` and later increment either `completed_count` or `failure_count`. This distinction makes leaked capacity and slow late completions visible without weakening the concurrency limit.

Copy runners also have an explicit lifecycle. Shutdown atomically rejects new copies, cancels semaphore waiters before they can start, and waits for tracked worker tasks within one configured budget. A timeout raises `PayloadCopyShutdownTimeoutError` with component-level active counts; the worker remains tracked until the underlying thread finishes because Python cannot terminate it safely. Health exposes both the historical timeout and the current completion state.

The builder closes its shared runner and its owned default-cache runner concurrently so the configured timeout is a total budget rather than a separate full timeout per component. Standalone caches and publishers close only runners they own. After a synchronous shutdown timeout, the facade keeps its private event loop alive until late copy workers drain, then stops the loop automatically.

The health model remains operational rather than business-prescriptive. Coalestra now supplies a generic severity assessment for infrastructure conditions, while the consuming application still decides whether to alert, degrade, pause work, or continue. `BuilderHealth.to_dict()` serializes the complete public state through a versioned JSON-safe schema, and `BuilderHealth.assess()` evaluates current saturation, lifecycle failures, circuit state, and optional cumulative-counter deltas against an immutable policy. No resource subjects or qualifier values are added to assessment findings.

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

Buffered sinks preserve the synchronous `EventSink` and `MetricsSink` protocols while moving downstream delivery to dedicated worker threads. `SnapshotBuilder` applies this protection automatically to external sinks that do not declare `coalestra_non_blocking = True`. Built-in null and in-memory sinks remain inline, and an already-buffered sink is never wrapped twice.

```text
acquisition task
      |
      | bounded non-blocking enqueue
      v
metrics queue -> metrics worker -> downstream metrics sink
events queue  -> events worker  -> downstream event sink
```

Each queue is bounded to prevent observability from becoming an unbounded memory leak. Overflow behavior is explicit and measurable. The default `DROP_OLDEST` policy keeps acquisition moving under sustained sink congestion. Downstream exceptions are counted by the buffer and never re-enter acquisition control flow.

Builder-owned buffers are lifecycle components even when `manage_lifecycle=False`: the builder always stops the worker threads it created. With managed lifecycle enabled, buffers drain before the underlying sinks are closed. A timeout leaves the downstream open while the active delivery finishes and raises `ObservabilityShutdownTimeoutError`; the synchronous facade keeps its private event loop alive long enough to complete the deferred shutdown sequence.

## Metric cardinality boundary

Default metrics expose resource type, not resource instance identity. The stable labels are `resource_namespace` and `resource_name`. Subjects and qualifier values are excluded because they commonly contain symbols, accounts, tenants, or other unbounded identifiers.

```text
ResourceKey("market", "price", "BTCUSDT", {"venue": "spot"})
        |
        +-- metric labels: market / price
        +-- event resource: market:price:BTCUSDT?venue=spot
```

Structured events retain the full rendered resource key for investigation. This separates aggregated monitoring from detailed diagnostics without removing context. Configured source names remain valid metric labels because the builder owns a bounded source catalog; applications should not create source names from per-resource identifiers.
