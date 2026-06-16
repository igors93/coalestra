# Coalestra 0.6 integration readiness

Coalestra 0.6 is classified as beta and provides a versioned runtime contract for
integration consumers.

## Required production checks

1. Pin the package to `>=0.6.0,<0.7.0`.
2. Call `require_capabilities()` during application startup.
3. Declare every custom source as non-blocking or transport-timeout protected.
4. Keep `allow_unsafe_blocking_sources=False`.
5. Use snapshot acceptance policies for decision-critical reads.
6. Persist `BuilderHealth.to_dict()` and assess counter deltas against the previous sample.
7. Close the builder or synchronous facade during application shutdown.

## Stable contracts

The 0.6 compatibility surface includes:

- versioned error diagnostics;
- versioned builder-health serialization;
- versioned capability discovery;
- immutable snapshots and diagnostics;
- transactional session revalidation;
- snapshot consistency and acceptance policies;
- bounded payload-copy and observability workers;
- explicit blocking-source timeout guarantees.

## Remaining operational responsibility

Coalestra validates declared source budgets but cannot prove that a third-party HTTP,
database, or SDK client actually applies its configured timeout. Runtime transport-timeout
violations are reported through health, metrics, and structured events. The consuming
application must alert on those violations and repair the client configuration.
