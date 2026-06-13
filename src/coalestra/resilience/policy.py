from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum

from coalestra.resilience.retry import RetryPolicy


class CircuitScope(str, Enum):
    """Identity boundary used to isolate circuit-breaker failures."""

    SOURCE = "source"
    NAMESPACE = "namespace"
    SUBJECT = "subject"
    RESOURCE = "resource"


@dataclass(frozen=True)
class CircuitBreakerPolicy:
    """Circuit-breaker behavior for one source or source family."""

    scope: CircuitScope = CircuitScope.SOURCE
    failure_threshold: int = 3
    recovery_timeout_seconds: float = 10.0
    enabled: bool = True

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if self.recovery_timeout_seconds < 0:
            raise ValueError("recovery_timeout_seconds cannot be negative")


@dataclass(frozen=True)
class SourceResiliencePolicy:
    """Retry and circuit settings applied to one source."""

    retry: RetryPolicy = field(default_factory=RetryPolicy)
    circuit: CircuitBreakerPolicy = field(default_factory=CircuitBreakerPolicy)


ResilienceResolver = Callable[[str], SourceResiliencePolicy]


class ResiliencePolicyResolver:
    """Resolve source-specific resilience policies with explicit and dynamic overrides."""

    def __init__(
        self,
        default: SourceResiliencePolicy | None = None,
        overrides: Mapping[str, SourceResiliencePolicy] | None = None,
        dynamic: ResilienceResolver | None = None,
    ) -> None:
        self._default = default or SourceResiliencePolicy()
        self._overrides = {
            self._normalize_source(source): policy for source, policy in (overrides or {}).items()
        }
        self._dynamic = dynamic

    @property
    def default(self) -> SourceResiliencePolicy:
        return self._default

    def resolve(
        self,
        source: str,
        *,
        declared: SourceResiliencePolicy | None = None,
    ) -> SourceResiliencePolicy:
        normalized = self._normalize_source(source)
        if normalized in self._overrides:
            return self._overrides[normalized]
        if self._dynamic is not None:
            return self._dynamic(normalized)
        if declared is not None:
            return declared
        return self._default

    @staticmethod
    def _normalize_source(source: str) -> str:
        normalized = str(source or "").strip()
        if not normalized:
            raise ValueError("source name cannot be empty")
        return normalized
