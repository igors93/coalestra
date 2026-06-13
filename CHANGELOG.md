# Changelog

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
