# Migration to Coalestra 0.6

Coalestra 0.6 establishes the first versioned integration contract. The snapshot, session,
cache, publisher, and synchronous APIs remain source-compatible with 0.5 unless an
integration uses a custom source without timeout-safety declarations.

## Update the dependency

```toml
coalestra = ">=0.6.0,<0.7.0"
```

## Declare custom source timeout safety

`SnapshotBuilder` now enables `require_source_timeout_declarations=True` by default.
Custom sources must expose these attributes:

```python
blocking_io = False
blocking_io_offloaded = False
transport_timeout_seconds = None
```

Use that declaration only for asynchronous or guaranteed non-blocking local work.
Blocking network, database, filesystem, or SDK calls must be offloaded and bounded:

```python
blocking_io = True
blocking_io_offloaded = True
transport_timeout_seconds = 1.5
timeout_seconds = 2.0
```

Callable adapters expose the same constructor options. The transport timeout must be
strictly smaller than the Coalestra source timeout.

A temporary compatibility escape hatch remains available:

```python
SnapshotBuilder(
    sources,
    require_source_timeout_declarations=False,
)
```

Do not use this setting for production integrations. Unsafe blocking declarations remain
rejected unless `allow_unsafe_blocking_sources=True` is explicitly selected.

## Validate capabilities at startup

```python
from coalestra import require_capabilities

require_capabilities(
    features=(
        "snapshot_acceptance",
        "transactional_revalidation",
        "builder_health_serialization",
        "blocking_source_timeout_guarantees",
    ),
    schemas={
        "builder_health": 1,
        "error_diagnostics": 1,
    },
)
```

This turns an incompatible package installation into a clear startup failure instead of a
runtime integration failure.

## Monitor declared transport timeouts

`BuilderHealth` now includes:

- `source_transport_timeout_violation_count`;
- `source_transport_timeout_violations`.

A violation means the observed source call exceeded its declared transport timeout plus
the configured scheduler grace. This usually indicates that the downstream client did not
apply the timeout that the source declared.

## Use the official health schema

Replace manual health dictionaries with:

```python
health = await builder.health_snapshot()
payload = health.to_dict(previous=previous_health)
```

Consumers should branch on `schema_version` and ignore unknown additive fields.

## Release contract

The 0.6 wheel is verified against a capability manifest, public API manifest, versioned
health and error schemas, strict typing, deterministic concurrency regressions, the full
test suite, and a clean-environment installation check.
