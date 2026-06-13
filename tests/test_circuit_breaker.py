from __future__ import annotations

import asyncio

import pytest

from coalestra import CircuitBreaker, CircuitOpenError
from coalestra.resilience import CircuitState


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def now(self) -> float:
        return self.value

    def monotonic(self) -> float:
        return self.value


def test_circuit_opens_and_recovers_through_half_open_probe() -> None:
    clock = FakeClock()

    async def scenario() -> None:
        breaker = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout_seconds=5.0,
            clock=clock,
        )
        await breaker.record_failure("rest")
        await breaker.record_failure("rest")
        assert await breaker.state_for("rest") is CircuitState.OPEN

        with pytest.raises(CircuitOpenError):
            await breaker.before_call("rest")

        clock.value = 6.0
        await breaker.before_call("rest")
        assert await breaker.state_for("rest") is CircuitState.HALF_OPEN
        await breaker.record_success("rest")
        assert await breaker.state_for("rest") is CircuitState.CLOSED

    asyncio.run(scenario())
