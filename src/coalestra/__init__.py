"""Coalestra public API."""

from coalestra.adapters import CallableSource
from coalestra.cache import AsyncMemoryCache
from coalestra.core import (
    AsyncCache,
    CacheLookup,
    CircuitOpenError,
    CoalestraError,
    FetchContext,
    FreshnessPolicy,
    ResourceKey,
    ResourceResolutionError,
    Snapshot,
    SnapshotBuildError,
    SnapshotSource,
    SnapshotValue,
    SourceFailure,
    SourcePayload,
    SourceTimeoutError,
    SourceUnavailableError,
)
from coalestra.observability import (
    InMemoryMetrics,
    LoggingEventSink,
    NullEventSink,
    NullMetrics,
)
from coalestra.orchestration import PolicyResolver, SnapshotBuilder
from coalestra.resilience import CircuitBreaker, CircuitState, RetryPolicy
from coalestra.sync import SyncSnapshotBuilder

__version__ = "0.1.0"

__all__ = [
    "AsyncCache",
    "AsyncMemoryCache",
    "CacheLookup",
    "CallableSource",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "CoalestraError",
    "FetchContext",
    "FreshnessPolicy",
    "InMemoryMetrics",
    "LoggingEventSink",
    "NullEventSink",
    "NullMetrics",
    "PolicyResolver",
    "ResourceKey",
    "ResourceResolutionError",
    "RetryPolicy",
    "Snapshot",
    "SnapshotBuildError",
    "SnapshotBuilder",
    "SnapshotSource",
    "SnapshotValue",
    "SourceFailure",
    "SourcePayload",
    "SourceTimeoutError",
    "SourceUnavailableError",
    "SyncSnapshotBuilder",
    "__version__",
]
