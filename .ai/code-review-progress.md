# Coalestra – incremental review progress

## Cycle 1 — 2026-06-19

### Scope
- Module: `src/coalestra/operational.py` (newly added, untracked)
- Secondary: `src/coalestra/__init__.py` (import integration)

### Reason for selection
`operational.py` was the only untracked source file and had no dedicated test coverage. It introduced
a new public API surface (`try_resolve_request`, `try_build_request`, `try_build_requests`,
`RequestDegradationPolicy`, `SnapshotResult`) with 9 verified ruff/mypy violations and zero tests.

### Files modified
- `src/coalestra/operational.py` — 9 fixes applied (see problems below)
- `src/coalestra/__init__.py` — import order corrected
- `tests/test_operational.py` — 35 new tests (created)

### Problems fixed

1. **F401 – unused import `Sequence`** (`operational.py:4`): removed from `collections.abc` import.
2. **I001 – import order** (`__init__.py`): moved `from coalestra.operational import ...` before
   `from coalestra.orchestration import ...` (alphabetical: operational < orchestration).
3. **arg-type × 3 – `allowed_actions` type mismatch** (`operational.py` lines 290, 548, 566):
   `policy.*_actions` typed as `Iterable[str]` but `SnapshotResult` expects `tuple[str, ...]`.
   Fixed by wrapping with `tuple()` at the 3 call sites.
4. **no-any-return × 3 – sync wrappers** (`operational.py` lines 458, 478, 502):
   `_submit()` returns `Any`; fixed with `cast(SnapshotResult, ...)` and
   `cast(tuple[SnapshotResult, ...], ...)`.
5. **union-attr – `builder.clock.now()`** (`operational.py:600`): `builder` could be `None` after
   `getattr(..., None)` — added explicit `if builder is None: return ()` guard before accessing it.
6. **E501 – docstring line too long** (`operational.py:341`): split at 100 chars.
7. **E501 – `asyncio.gather` line too long** (`operational.py:371`): extracted generator to
   `tasks` variable; line now within limit.
8. **B010 × 6 – `setattr` with constant attribute names** (`operational.py:382-392`):
   replaced with direct class attribute assignment + `# type: ignore[attr-defined]`.
9. **Missing dedicated tests for `operational.py`**: created `tests/test_operational.py` with 35
   tests covering `RequestDegradationPolicy` construction/validation, `SnapshotResult` properties
   and helpers, `try_resolve_request` (accepted / degraded / rejected), `try_build_request`,
   `try_build_requests` (order, concurrency, fanout isolation), `install_operational_methods`
   idempotency, and sync wrappers.

### Tests added
- `tests/test_operational.py` — 35 tests, all passing.

### Validations executed

| Command | Result |
|---|---|
| `python -m ruff check src/coalestra/` | All checks passed |
| `python -m mypy src/coalestra/` | Success: no issues in 52 source files |
| `python -m pytest tests/test_operational.py` | 35 passed |
| `python -m pytest tests/` | 867 passed |

Pre-existing failures: none detected.

### Known risks / residual
- `_record_try_result` silently swallows all exceptions with bare `except Exception: return`.
  If observability sinks raise unexpectedly, failures are invisible. Not fixed in this cycle because
  the suppression is intentional for resilience. Deferred.
- Sync wrappers use `self: Any` throughout; mypy coverage of those paths is shallow by design.
  The `cast` calls are assertions, not verified types.

### Deferred problems
- Silent exception swallowing in `_record_try_result` — intentional but limits observability.

### Areas recommended for next cycle
- `src/coalestra/orchestration/builder.py` — large file, orchestrates deadline logic,
  singleflight, source executor, capacity; high regression risk if changed.
- `src/coalestra/orchestration/source_executor.py` — deadline dispatch and grace logic added in
  v0.6.2; relatively recent with potential edge cases.
- `src/coalestra/resilience/circuit_breaker.py` — circuit state machine; security and
  reliability dimensions worth checking.
