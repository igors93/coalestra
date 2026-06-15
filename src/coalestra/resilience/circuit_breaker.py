from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

from coalestra.core.clock import SystemClock
from coalestra.core.errors import CircuitOpenError
from coalestra.core.models import ResourceKey
from coalestra.core.protocols import Clock
from coalestra.resilience.policy import CircuitBreakerPolicy, CircuitScope


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class CircuitIdentity:
    """Stable identity for one independently protected failure domain."""

    source: str
    scope: CircuitScope
    discriminator: str = ""

    def __str__(self) -> str:
        base = f"{self.source}[{self.scope.value}]"
        return f"{base}:{self.discriminator}" if self.discriminator else base


@dataclass
class _Circuit:
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    opened_at: float = 0.0
    half_open_probe_active: bool = False


@dataclass(frozen=True)
class CircuitSnapshot:
    state: CircuitState
    failures: int
    opened_at: float
    half_open_probe_active: bool


class CircuitBreaker:
    """Asynchronous circuit breaker with configurable identity scope.

    The original source-only API remains valid. Callers that provide a resource key and a policy
    can isolate failures by namespace, subject, or full resource identity.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        recovery_timeout_seconds: float = 10.0,
        clock: Clock | None = None,
        default_policy: CircuitBreakerPolicy | None = None,
    ) -> None:
        self.default_policy = default_policy or CircuitBreakerPolicy(
            failure_threshold=failure_threshold,
            recovery_timeout_seconds=recovery_timeout_seconds,
        )
        self.clock = clock or SystemClock()
        self._circuits: dict[CircuitIdentity, _Circuit] = {}
        self._lock = asyncio.Lock()

    def identity_for(
        self,
        source: str,
        *,
        key: ResourceKey | None = None,
        scope: CircuitScope | None = None,
    ) -> CircuitIdentity:
        normalized_source = str(source or "").strip()
        if not normalized_source:
            raise ValueError("source name cannot be empty")

        resolved_scope = scope or self.default_policy.scope
        if resolved_scope is CircuitScope.SOURCE:
            return CircuitIdentity(normalized_source, resolved_scope)

        if key is None:
            raise ValueError(f"resource key is required for circuit scope {resolved_scope.value}")

        if resolved_scope is CircuitScope.NAMESPACE:
            discriminator = key.namespace
        elif resolved_scope is CircuitScope.SUBJECT:
            discriminator = key.subject or f"{key.namespace}:{key.name}"
        else:
            discriminator = str(key)

        return CircuitIdentity(
            normalized_source,
            resolved_scope,
            discriminator,
        )

    async def before_call(
        self,
        source: str,
        *,
        key: ResourceKey | None = None,
        policy: CircuitBreakerPolicy | None = None,
    ) -> CircuitIdentity:
        resolved_policy = policy or self.default_policy
        identity = self.identity_for(
            source,
            key=key,
            scope=resolved_policy.scope,
        )
        if not resolved_policy.enabled:
            return identity

        async with self._lock:
            circuit = self._circuits.setdefault(identity, _Circuit())
            if circuit.state is CircuitState.CLOSED:
                return identity

            now = self.clock.monotonic()
            if circuit.state is CircuitState.OPEN:
                if now - circuit.opened_at < resolved_policy.recovery_timeout_seconds:
                    raise CircuitOpenError(f"circuit is open for {identity}")
                circuit.state = CircuitState.HALF_OPEN

            if circuit.half_open_probe_active:
                raise CircuitOpenError(f"half-open probe already active for {identity}")

            circuit.half_open_probe_active = True
            return identity

    async def record_success(
        self,
        source: str,
        *,
        key: ResourceKey | None = None,
        policy: CircuitBreakerPolicy | None = None,
    ) -> None:
        resolved_policy = policy or self.default_policy
        if not resolved_policy.enabled:
            return

        identity = self.identity_for(
            source,
            key=key,
            scope=resolved_policy.scope,
        )
        async with self._lock:
            circuit = self._circuits.setdefault(identity, _Circuit())
            circuit.state = CircuitState.CLOSED
            circuit.failures = 0
            circuit.opened_at = 0.0
            circuit.half_open_probe_active = False

    async def record_failure(
        self,
        source: str,
        *,
        key: ResourceKey | None = None,
        policy: CircuitBreakerPolicy | None = None,
    ) -> None:
        resolved_policy = policy or self.default_policy
        if not resolved_policy.enabled:
            return

        identity = self.identity_for(
            source,
            key=key,
            scope=resolved_policy.scope,
        )
        async with self._lock:
            circuit = self._circuits.setdefault(identity, _Circuit())
            circuit.half_open_probe_active = False
            circuit.failures += 1
            if (
                circuit.failures >= resolved_policy.failure_threshold
                or circuit.state is CircuitState.HALF_OPEN
            ):
                circuit.state = CircuitState.OPEN
                circuit.opened_at = self.clock.monotonic()

    async def record_abandoned(
        self,
        source: str,
        *,
        key: ResourceKey | None = None,
        policy: CircuitBreakerPolicy | None = None,
    ) -> None:
        """Release a half-open probe when its task is cancelled before an outcome exists."""

        resolved_policy = policy or self.default_policy
        if not resolved_policy.enabled:
            return

        identity = self.identity_for(
            source,
            key=key,
            scope=resolved_policy.scope,
        )
        async with self._lock:
            circuit = self._circuits.get(identity)
            if circuit is None:
                return

            if circuit.state is CircuitState.HALF_OPEN and circuit.half_open_probe_active:
                circuit.state = CircuitState.OPEN
                circuit.opened_at = self.clock.monotonic()

            circuit.half_open_probe_active = False

    async def record_skipped(
        self,
        source: str,
        *,
        key: ResourceKey | None = None,
        policy: CircuitBreakerPolicy | None = None,
    ) -> None:
        """Release a probe when no source outcome was produced."""

        resolved_policy = policy or self.default_policy
        if not resolved_policy.enabled:
            return

        identity = self.identity_for(
            source,
            key=key,
            scope=resolved_policy.scope,
        )
        async with self._lock:
            circuit = self._circuits.get(identity)
            if circuit is None:
                return

            circuit.half_open_probe_active = False

    async def state_for(
        self,
        source: str,
        *,
        key: ResourceKey | None = None,
        policy: CircuitBreakerPolicy | None = None,
    ) -> CircuitState:
        resolved_policy = policy or self.default_policy
        if not resolved_policy.enabled:
            return CircuitState.CLOSED

        identity = self.identity_for(
            source,
            key=key,
            scope=resolved_policy.scope,
        )
        async with self._lock:
            return self._circuits.get(identity, _Circuit()).state

    async def snapshot(self) -> Mapping[CircuitIdentity, CircuitSnapshot]:
        async with self._lock:
            return MappingProxyType(
                {
                    identity: CircuitSnapshot(
                        state=circuit.state,
                        failures=circuit.failures,
                        opened_at=circuit.opened_at,
                        half_open_probe_active=circuit.half_open_probe_active,
                    )
                    for identity, circuit in self._circuits.items()
                }
            )

    async def reset(
        self,
        source: str | None = None,
        *,
        key: ResourceKey | None = None,
        policy: CircuitBreakerPolicy | None = None,
    ) -> None:
        async with self._lock:
            if source is None:
                self._circuits.clear()
                return

            resolved_policy = policy or self.default_policy
            identity = self.identity_for(
                source,
                key=key,
                scope=resolved_policy.scope,
            )
            self._circuits.pop(identity, None)
