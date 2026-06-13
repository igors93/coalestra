from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")


@dataclass(frozen=True, order=True)
class ResourceKey:
    """Stable, hashable identifier for one resource in a snapshot."""

    namespace: str
    name: str
    subject: str = ""

    def __post_init__(self) -> None:
        namespace = self.namespace.strip().lower()
        name = self.name.strip().lower()
        subject = self.subject.strip().upper()
        if not namespace:
            raise ValueError("namespace cannot be empty")
        if not name:
            raise ValueError("name cannot be empty")
        object.__setattr__(self, "namespace", namespace)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "subject", subject)

    def __str__(self) -> str:
        base = f"{self.namespace}:{self.name}"
        return f"{base}:{self.subject}" if self.subject else base


@dataclass(frozen=True)
class FreshnessPolicy:
    """Controls fresh-cache reuse and stale fallback for a resource."""

    ttl_seconds: float
    max_stale_seconds: float = 0.0
    allow_stale_on_error: bool = True

    def __post_init__(self) -> None:
        if self.ttl_seconds < 0:
            raise ValueError("ttl_seconds cannot be negative")
        if self.max_stale_seconds < self.ttl_seconds:
            raise ValueError("max_stale_seconds must be greater than or equal to ttl_seconds")


@dataclass(frozen=True)
class FetchContext:
    """Context shared with sources while one snapshot is being acquired."""

    requested_at: float
    deadline_at: float | None = None
    deadline_monotonic: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    snapshot_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class SourcePayload(Generic[T]):
    """Value returned by a source before Coalestra adds acquisition metadata."""

    value: T
    observed_at: float | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class SnapshotValue(Generic[T]):
    """Resolved resource plus provenance, freshness and acquisition diagnostics."""

    key: ResourceKey
    value: T
    source: str
    observed_at: float
    fetched_at: float
    age_seconds: float
    stale: bool
    from_cache: bool
    latency_ms: float
    attempts: int = 1
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True)
class CacheLookup:
    """Freshness-aware result returned by an asynchronous cache."""

    value: SnapshotValue[Any] | None
    fresh: bool
    usable_stale: bool


@dataclass(frozen=True)
class Snapshot(Mapping[ResourceKey, SnapshotValue[Any]]):
    """Immutable read model produced for one unit of work."""

    snapshot_id: str
    created_at: float
    resources: Mapping[ResourceKey, SnapshotValue[Any]]
    errors: Mapping[ResourceKey, Exception] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "resources", MappingProxyType(dict(self.resources)))
        object.__setattr__(self, "errors", MappingProxyType(dict(self.errors)))

    def __getitem__(self, key: ResourceKey) -> SnapshotValue[Any]:
        return self.resources[key]

    def __iter__(self) -> Iterator[ResourceKey]:
        return iter(self.resources)

    def __len__(self) -> int:
        return len(self.resources)

    def value(self, key: ResourceKey, expected_type: type[T] | None = None) -> T:
        """Return the resource value, optionally enforcing a runtime type."""

        item = self.resources[key].value
        if expected_type is not None and not isinstance(item, expected_type):
            raise TypeError(
                f"Resource {key} contains {type(item).__name__}, expected {expected_type.__name__}"
            )
        return cast(T, item)

    @property
    def complete(self) -> bool:
        """Whether every requested resource was resolved."""

        return not self.errors
