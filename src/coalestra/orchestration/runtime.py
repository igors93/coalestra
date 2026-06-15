from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
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
    dependency_versions: Mapping[ResourceKey, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if (self.payload is None) == (self.error is None):
            raise ValueError("source attempt must contain exactly one of payload or error")
        if self.error is not None and self.dependency_versions:
            raise ValueError("failed source attempts cannot contain dependency versions")
        object.__setattr__(
            self,
            "dependency_versions",
            MappingProxyType(dict(self.dependency_versions)),
        )


@dataclass
class ResolutionRuntime:
    """Mutable state shared by every stage of one snapshot session."""

    diagnostics: DiagnosticsCollector
    memo: dict[ResourceKey, SnapshotValue[Any]] = field(default_factory=dict)
    cache_stale_results: bool = True
    force_refresh_keys: set[ResourceKey] = field(default_factory=set)

    def requires_refresh(self, key: ResourceKey) -> bool:
        """Return whether ``key`` must bypass pinned and cached values."""

        return key in self.force_refresh_keys

    def mark_refreshed(self, key: ResourceKey) -> None:
        """Allow later dependency reads to reuse a successfully refreshed value."""

        self.force_refresh_keys.discard(key)
