from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum

from coalestra.core.clock import SystemClock
from coalestra.core.errors import CircuitOpenError
from coalestra.core.protocols import Clock


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class _Circuit:
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    opened_at: float = 0.0
    half_open_probe_active: bool = False


class CircuitBreaker:
    """Per-source circuit breaker designed for asynchronous orchestration."""

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        recovery_timeout_seconds: float = 10.0,
        clock: Clock | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if recovery_timeout_seconds < 0:
            raise ValueError("recovery_timeout_seconds cannot be negative")
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.clock = clock or SystemClock()
        self._circuits: dict[str, _Circuit] = {}
        self._lock = asyncio.Lock()

    async def before_call(self, source: str) -> None:
        async with self._lock:
            circuit = self._circuits.setdefault(source, _Circuit())
            if circuit.state is CircuitState.CLOSED:
                return

            now = self.clock.monotonic()
            if circuit.state is CircuitState.OPEN:
                if now - circuit.opened_at < self.recovery_timeout_seconds:
                    raise CircuitOpenError(f"circuit is open for source {source}")
                circuit.state = CircuitState.HALF_OPEN

            if circuit.half_open_probe_active:
                raise CircuitOpenError(f"half-open probe already active for source {source}")
            circuit.half_open_probe_active = True

    async def record_success(self, source: str) -> None:
        async with self._lock:
            circuit = self._circuits.setdefault(source, _Circuit())
            circuit.state = CircuitState.CLOSED
            circuit.failures = 0
            circuit.opened_at = 0.0
            circuit.half_open_probe_active = False

    async def record_failure(self, source: str) -> None:
        async with self._lock:
            circuit = self._circuits.setdefault(source, _Circuit())
            circuit.half_open_probe_active = False
            circuit.failures += 1
            if (
                circuit.failures >= self.failure_threshold
                or circuit.state is CircuitState.HALF_OPEN
            ):
                circuit.state = CircuitState.OPEN
                circuit.opened_at = self.clock.monotonic()

    async def state_for(self, source: str) -> CircuitState:
        async with self._lock:
            return self._circuits.get(source, _Circuit()).state
