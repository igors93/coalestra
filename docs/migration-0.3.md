# Migrating from 0.2 to 0.3

Version 0.3 preserves the 0.2 build, session, batch, derived-source, cache, and synchronous APIs.

## Concurrency semantic change

In 0.2, `max_concurrency` created one semaphore per session. Two simultaneous sessions configured with `max_concurrency=8` could execute up to sixteen source operations.

In 0.3, the same option creates one builder-wide limit shared by every build and session:

```python
builder = SnapshotBuilder(sources, max_concurrency=8)
```

No call-site change is required. Applications that intentionally relied on multiplied session capacity should increase the builder limit explicitly.

Optional source limits can be added centrally:

```python
builder = SnapshotBuilder(
    sources,
    max_concurrency=12,
    source_concurrency={"rest": 4},
)
```

Or declared by callable adapters with `max_concurrency=`.

## Resilience policies

The old `retry_policy=` and `circuit_breaker=` constructor arguments remain supported and define the default behavior.

New applications can configure each source independently:

```python
policy = SourceResiliencePolicy(
    retry=RetryPolicy(max_attempts=1),
    circuit=CircuitBreakerPolicy(scope=CircuitScope.SUBJECT),
)

source = CallableSource(..., resilience_policy=policy)
```

Central overrides can be supplied with `source_resilience=`. They take precedence over source declarations.

Existing direct uses of `CircuitBreaker.before_call("source")`, `record_success("source")`, `record_failure("source")`, and `state_for("source")` continue to use source-wide scope.

## Event publication

No existing cache writes need to change. Event-driven consumers may now publish through the builder:

```python
await builder.publisher.publish(
    key,
    value,
    source="event-stream",
    observed_at=event_time,
)
```

Synchronous applications use `sync_builder.publisher`.

Published values are visible to new builds and sessions. A value already pinned in an active session remains unchanged until that session ends.

## Custom source classes

The base source protocols are unchanged. Custom classes may optionally expose:

```python
max_concurrency: int | None
resilience_policy: SourceResiliencePolicy | None
```

Neither attribute is mandatory.
