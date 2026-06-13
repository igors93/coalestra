from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from coalestra.core.models import ResourceKey


class CoalestraError(Exception):
    """Base exception for the library."""


class SourceUnavailableError(CoalestraError):
    """Raised when a source cannot serve a resource at this time."""


class SourceTimeoutError(SourceUnavailableError):
    """Raised when a source exceeds its configured timeout."""


class SourceProtocolError(CoalestraError):
    """Raised when a source violates one of Coalestra's source contracts."""


class CircuitOpenError(SourceUnavailableError):
    """Raised when a circuit breaker prevents a source call."""


class SessionClosedError(CoalestraError):
    """Raised when a closed snapshot session receives more work."""


class DependencyCycleError(CoalestraError):
    """Raised when derived resources form a dependency cycle."""

    def __init__(self, path: tuple[ResourceKey, ...]):
        self.path = path
        rendered = " -> ".join(str(item) for item in path)
        super().__init__(f"Derived resource dependency cycle detected: {rendered}")


class DependencyResolutionError(CoalestraError):
    """Raised when a derived source cannot resolve all required dependencies."""

    def __init__(
        self,
        key: ResourceKey,
        source: str,
        errors: Mapping[ResourceKey, Exception],
    ) -> None:
        self.key = key
        self.source = source
        self.errors = dict(errors)
        summary = ", ".join(
            f"{dependency}={type(error).__name__}({error})" for dependency, error in errors.items()
        )
        super().__init__(
            f"Unable to derive {key} with source {source}; dependency failures: {summary}"
        )


@dataclass(frozen=True)
class SourceFailure:
    source: str
    error_type: str
    message: str
    attempts: int = 1


class ResourceResolutionError(CoalestraError):
    def __init__(self, key: ResourceKey, failures: tuple[SourceFailure, ...]):
        self.key = key
        self.failures = failures
        details = (
            "; ".join(f"{item.source}: {item.error_type}({item.message})" for item in failures)
            or "no compatible source"
        )
        super().__init__(f"Unable to resolve {key}: {details}")


class SnapshotBuildError(CoalestraError):
    def __init__(self, errors: Mapping[ResourceKey, Exception]):
        self.errors = dict(errors)
        summary = ", ".join(f"{key}={type(error).__name__}" for key, error in errors.items())
        super().__init__(f"Snapshot build failed: {summary}")
