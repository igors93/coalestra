from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True)
class BuilderHealth:
    """Immutable operational state for integration health endpoints."""

    closed: bool
    background_refreshes: int
    singleflight_in_flight: int
    source_support_cache_entries: int
    capacity: Mapping[str, Any] = field(default_factory=dict)
    cache: Any | None = None
    circuits: Mapping[Any, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "capacity", MappingProxyType(dict(self.capacity)))
        object.__setattr__(self, "circuits", MappingProxyType(dict(self.circuits)))
