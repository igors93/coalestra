from __future__ import annotations

import asyncio

from coalestra import (
    CallableSource,
    ResourceKey,
    ResourceResolutionError,
    SnapshotBuilder,
    SnapshotDeadlineExceededError,
    SourceQueueTimeoutError,
    SourceTimeoutError,
)
from coalestra.resilience import (
    CircuitBreakerPolicy,
    CircuitState,
    RetryPolicy,
    SourceResiliencePolicy,
)

FIRST = ResourceKey("test", "first")
SECOND = ResourceKey("test", "second")


def run(coro):
    return asyncio.run(coro)


def first_failure(snapshot, key):
    error = snapshot.errors[key]
    assert isinstance(error, ResourceResolutionError)
    return error.failures[0]


def test_source_timeout_starts_after_capacity_is_acquired() -> None:
    async def scenario() -> None:
        calls = 0

        async def fetch(_key, _context):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            return "ok"

        source = CallableSource(
            name="source",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            timeout_seconds=0.04,
            queue_timeout_seconds=0.20,
            max_concurrency=1,
        )

        builder = SnapshotBuilder(
            [source],
            max_concurrency=2,
        )

        lease = await builder.capacity.acquire(source.name)

        try:
            task = asyncio.create_task(builder.build([SECOND]))

            await asyncio.sleep(0.06)

            assert calls == 0

            lease.release()

            snapshot = await task

            assert snapshot.value(SECOND) == "ok"
            assert calls == 1
        finally:
            lease.release()
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


def test_queue_timeout_does_not_trip_circuit() -> None:
    async def scenario() -> None:
        calls = 0

        async def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return "unexpected"

        resilience = SourceResiliencePolicy(
            retry=RetryPolicy(max_attempts=3),
            circuit=CircuitBreakerPolicy(
                failure_threshold=1,
            ),
        )

        source = CallableSource(
            name="source",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            timeout_seconds=1.0,
            queue_timeout_seconds=0.02,
            max_concurrency=1,
            resilience_policy=resilience,
        )

        builder = SnapshotBuilder(
            [source],
            max_concurrency=2,
        )

        lease = await builder.capacity.acquire(source.name)

        try:
            snapshot = await builder.build(
                [SECOND],
                strict=False,
            )

            failure = first_failure(snapshot, SECOND)

            assert failure.error_type == (SourceQueueTimeoutError.__name__)
            assert failure.attempts == 1
            assert calls == 0

            state = await builder.circuit_breaker.state_for(
                source.name,
                key=SECOND,
                policy=resilience.circuit,
            )

            assert state is CircuitState.CLOSED
        finally:
            lease.release()
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


def test_snapshot_deadline_does_not_trip_circuit() -> None:
    async def scenario() -> None:
        calls = 0

        async def fetch(_key, _context):
            nonlocal calls
            calls += 1
            return "unexpected"

        resilience = SourceResiliencePolicy(
            retry=RetryPolicy(max_attempts=1),
            circuit=CircuitBreakerPolicy(
                failure_threshold=1,
            ),
        )

        source = CallableSource(
            name="source",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            timeout_seconds=1.0,
            queue_timeout_seconds=1.0,
            max_concurrency=1,
            resilience_policy=resilience,
        )

        builder = SnapshotBuilder(
            [source],
            max_concurrency=2,
        )

        lease = await builder.capacity.acquire(source.name)

        try:
            snapshot = await builder.build(
                [SECOND],
                strict=False,
                deadline_seconds=0.02,
            )

            failure = first_failure(snapshot, SECOND)

            assert failure.error_type == (SnapshotDeadlineExceededError.__name__)
            assert calls == 0

            state = await builder.circuit_breaker.state_for(
                source.name,
                key=SECOND,
                policy=resilience.circuit,
            )

            assert state is CircuitState.CLOSED
        finally:
            lease.release()
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


def test_real_source_timeout_still_trips_circuit() -> None:
    async def scenario() -> None:
        async def fetch(_key, _context):
            await asyncio.sleep(0.05)
            return "late"

        resilience = SourceResiliencePolicy(
            retry=RetryPolicy(max_attempts=1),
            circuit=CircuitBreakerPolicy(
                failure_threshold=1,
            ),
        )

        source = CallableSource(
            name="source",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            timeout_seconds=0.01,
            queue_timeout_seconds=0.10,
            resilience_policy=resilience,
        )

        builder = SnapshotBuilder([source])

        try:
            snapshot = await builder.build(
                [SECOND],
                strict=False,
            )

            failure = first_failure(snapshot, SECOND)

            assert failure.error_type == (SourceTimeoutError.__name__)

            state = await builder.circuit_breaker.state_for(
                source.name,
                key=SECOND,
                policy=resilience.circuit,
            )

            assert state is CircuitState.OPEN
        finally:
            await builder.aclose(cancel_refreshes=True)

    run(scenario())


def test_transport_timeout_is_not_misclassified_as_deadline() -> None:
    async def scenario() -> None:
        async def fetch(_key, _context):
            raise TimeoutError("transport timeout")

        source = CallableSource(
            name="source",
            priority=1,
            supports=lambda _key: True,
            fetcher=fetch,
            timeout_seconds=None,
            run_sync_in_thread=False,
        )

        builder = SnapshotBuilder(
            [source],
            retry_policy=RetryPolicy(max_attempts=1),
        )

        try:
            snapshot = await builder.build(
                [SECOND],
                strict=False,
                deadline_seconds=1.0,
            )

            failure = first_failure(snapshot, SECOND)

            assert failure.error_type == "TimeoutError"
            assert "transport timeout" in failure.message
        finally:
            await builder.aclose(cancel_refreshes=True)

    run(scenario())
