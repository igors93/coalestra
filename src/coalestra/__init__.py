"""Coalestra public API."""

from coalestra.adapters import CallableBatchSource, CallableDerivedSource, CallableSource
from coalestra.cache import AsyncMemoryCache
from coalestra.core import (
    AsyncCache,
    BatchSnapshotSource,
    CacheLookup,
    CircuitOpenError,
    CoalestraError,
    DependencyCycleError,
    DependencyResolutionError,
    DerivedSource,
    FetchContext,
    FreshnessPolicy,
    ResourceKey,
    ResourceResolutionError,
    SessionClosedError,
    Snapshot,
    SnapshotBuildError,
    SnapshotSource,
    SnapshotValue,
    SourceFailure,
    SourcePayload,
    SourceProtocolError,
    SourceTimeoutError,
    SourceUnavailableError,
)
from coalestra.observability import (
    InMemoryMetrics,
    LoggingEventSink,
    NullEventSink,
    NullMetrics,
)
from coalestra.orchestration import PolicyResolver, SnapshotBuilder, SnapshotSession
from coalestra.resilience import CircuitBreaker, CircuitState, RetryPolicy
from coalestra.sync import SyncSnapshotBuilder, SyncSnapshotSession

__version__ = "0.2.0"

__all__ = [
    "AsyncCache",
    "AsyncMemoryCache",
    "BatchSnapshotSource",
    "CacheLookup",
    "CallableBatchSource",
    "CallableDerivedSource",
    "CallableSource",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
    "CoalestraError",
    "DependencyCycleError",
    "DependencyResolutionError",
    "DerivedSource",
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
    "SessionClosedError",
    "Snapshot",
    "SnapshotBuildError",
    "SnapshotBuilder",
    "SnapshotSession",
    "SnapshotSource",
    "SnapshotValue",
    "SourceFailure",
    "SourcePayload",
    "SourceProtocolError",
    "SourceTimeoutError",
    "SourceUnavailableError",
    "SyncSnapshotBuilder",
    "SyncSnapshotSession",
    "__version__",
]
