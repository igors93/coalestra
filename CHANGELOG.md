# Changelog

## Unreleased

## 0.5.8 - 2026-06-15

- Moved `AsyncMemoryCache` payload and metadata copies outside its internal lock for reads, stored writes, and write results.
- Added a two-phase atomic write path that rechecks authority and timestamp ordering after preparing storage copies.
- Preserved rejection behavior for non-copyable candidates that lose authority or timestamp comparison and therefore never need storage.
- Added deterministic lock-scope and concurrent-write regression tests.

## 0.5.7 - 2026-06-15

- Added opt-in observation-skew limits to `SnapshotRequest` through `SnapshotConsistencyPolicy`.
- Added `SnapshotConsistencyError` with oldest/newest resource details and compatible partial-snapshot diagnostics.
- Added transactional observation-skew validation to asynchronous and synchronous session revalidation.
- Preserved existing behavior for requests and revalidations that do not configure a consistency policy.
- Corrected build outcome metrics so consistency failures are recorded as failed builds even when every resource resolved successfully.

## 0.5.6 - 2026-06-15

- Replaced full resource-key metric labels with bounded `resource_namespace` and `resource_name` labels while preserving full resource identity in structured events.
- Expanded `BuilderHealth` with aggregated dispatch, capacity-wait, timeout, revalidation, and synchronous submission-backlog state.
- Made wheel and source-distribution builds mandatory after the supported-Python test matrix succeeds.
- Added clean-wheel installation, exact public-API manifest verification, and project/package/tag version consistency checks.
- Added deterministic release concurrency regressions for authority races, capacity cancellation, session revalidation, and synchronous shutdown draining.

## 0.5.5 - 2026-06-15

- Added bounded non-blocking publication and invalidation submissions to the synchronous facade.
- Added bulk submission, backlog inspection, explicit flush, deterministic shutdown draining, and call-time payload capture.
- Added `SubmissionBacklogFullError` when a producer exceeds the configured backlog limit.
- Bounded single-key custom-cache read, write, atomic-write, and invalidation fallbacks with the existing `max_pending_tasks` worker limit.
- Added a stable, versioned, JSON-safe error-diagnostics schema for source, resource, and snapshot build failures.
- Added canonical `partial_snapshot_available` while retaining `has_partial_snapshot` as a schema-version-1 compatibility alias.

## 0.5.4 - 2026-06-15

- Added transactional selective revalidation for pinned session resources.
- Refreshed already-pinned derived dependents transitively when a dependency is revalidated.
- Added optional source-forced revalidation while preserving newer cache and publication reads by default.
- Preserved the previous session state when any affected resource fails revalidation.
- Added explicit source-authority policies for cache and publication conflicts.
- Added authority-aware cache outcomes while preserving timestamp ordering for equal-ranked sources.
- Bounded individual and derived source dispatch with a configurable fixed worker pool.

## 0.5.3 - 2026-06-15

- Added deep payload isolation across source, cache, publisher, single-flight, derived, session, and snapshot boundaries.
- Added configurable `payload_copier` hooks for trusted immutable or specialized payload types.
- Added structured `PayloadIsolationError` failures for payloads that cannot be copied safely.
- Updated cache tests to require value equality without object-identity sharing.
- Added version-aware derived cache invalidation, including transitive dependency chains and custom-cache fallback validation.

## 0.5.2 - 2026-06-14

- Added atomic and monotonic cache writes.
- Rejected non-finite observation timestamps.
- Split SnapshotBuilder into focused internal components.
- Expanded regression and component-level test coverage.

## 0.5.1 - 2026-06-14

- Added bounded nested source-failure details to `SnapshotBuildError`.
- Added JSON-safe `to_dict()` diagnostics to snapshot and resource errors.
- Added source names, error types, messages, and retry attempts to structured diagnostics.
- Added the effective timeout budget to `SourceTimeoutError` messages.
- Preserved concise default error rendering while keeping every failure available programmatically.

## 0.5.0 - 2026-06-13

- Added `SnapshotRequest` with required and optional resource semantics.
- Added partial snapshots to `SnapshotBuildError` for immediate fallback and diagnostics.
- Added `SnapshotBuilder.build_request()` and synchronous/session equivalents.
- Added `BuilderHealth` and health snapshots covering cache, capacity, circuits, refreshes, and single-flight work.
- Added configurable, bounded LRU caching for `source.supports()` decisions.
- Added `max_batch_size` with capacity-aware chunk dispatch for batch sources.
- Added deadline-aware retry backoff and exact retry-attempt diagnostics.
- Added `ObservationPolicy` for future-timestamp precision in sources and event publication.
- Fixed bulk publication so the newest duplicate update wins regardless of input order.
- Preserved each `ResourceKey` normalizer across qualifier transformations.
- Added optional inline execution for guaranteed non-blocking synchronous source adapters.
- Added managed component lifecycle and corrected synchronous builder shutdown.
- Expanded snapshot diagnostics with support-cache, retry, chunk, and timestamp-rejection counters.
- Added an enforced 500-test minimum; the release suite contains 618 collected tests.

## 0.4.0 - 2026-06-13

- Made `ResourceKey` case-preserving by default and added configurable `KeyNormalizer` policies.
- Added immutable, order-independent resource qualifiers for parameterized resources.
- Added `LEGACY_KEY_NORMALIZER` and migration helpers for 0.1-0.3 behavior.
- Added `BatchAsyncCache`, cache `get_many`/`set_many`/`invalidate_many`, namespace invalidation, pruning, and cache statistics.
- Added a bounded default memory-cache size with LRU eviction and automatic removal of fully expired entries.
- Added `RefreshMode.BLOCKING`, `STALE_WHILE_REVALIDATE`, and `REFRESH_AHEAD`.
- Added cancellation-safe background refresh scheduling, deduplication, lifecycle methods, and synchronous waiting.
- Added immutable `SnapshotDiagnostics` with per-session cache, source, batch, refresh, coalescing, latency, and observation-skew data.
- Added `BufferedEventSink` and `BufferedMetricsSink` with bounded queues and explicit overflow policies.
- Updated bulk publication to use cache batch operations under stable striped locking.
- Prevented background refreshes from replacing cache state with stale results.
- Expanded the test suite to 72 scenarios.

## 0.2.0 - 2026-06-13

- Added `BatchSnapshotSource` and `CallableBatchSource`.
- Added partial batch results with automatic fallback for omitted resources.
- Added per-key single-flight reservations for overlapping batch requests.
- Added asynchronous `SnapshotSession` for incremental multi-stage acquisition.
- Added `SyncSnapshotSession` and `SyncSnapshotBuilder.session()`.
- Added one identity, deadline, concurrency budget, and pinned-value memo per session.
- Added explicit retry of session errors through `retry_errors=True`.
- Added `DerivedSource` and `CallableDerivedSource`.
- Added recursive dependency resolution, dependency sharing, derived-value caching, and source fallback.
- Added direct and indirect dependency-cycle detection.
- Added source protocol validation for invalid batch responses and dependencies.
- Expanded the test suite to 37 scenarios.

## 0.1.0 - 2026-06-13

- Initial architecture.
- Concurrent snapshot construction.
- Request coalescing through single-flight execution.
- Per-resource freshness policies.
- Source priority and fallback.
- Retry and circuit-breaker policies.
- Stale-on-error support.
- In-memory metrics and structured events.
- Generic callable adapters and an Alphora integration example.
