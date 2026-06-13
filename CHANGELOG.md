# Changelog

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
