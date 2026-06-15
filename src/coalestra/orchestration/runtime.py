from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.models import ResourceKey, SnapshotValue, SourcePayload


@dataclass(frozen=True)
class ResolutionResult:
    """Internal result for one resource resolution."""

    value: SnapshotValue[Any] | None = None
    error: Exception | None = None

    def __post_init__(self) -> None:
        if (self.value is None) == (self.error is None):
            raise ValueError("resolution result must contain exactly one of value or error")


@dataclass(frozen=True)
class SourceAttempt:
    """Internal result for one source attempt."""

    payload: SourcePayload[Any] | None = None
    error: Exception | None = None
    attempts: int = 1
    latency_ms: float = 0.0

    def __post_init__(self) -> None:
        if (self.payload is None) == (self.error is None):
            raise ValueError("source attempt must contain exactly one of payload or error")


@dataclass
class ResolutionRuntime:
    """Mutable state shared by every stage of one snapshot session."""

    diagnostics: DiagnosticsCollector
    memo: dict[ResourceKey, SnapshotValue[Any]] = field(default_factory=dict)
    cache_stale_results: bool = True
