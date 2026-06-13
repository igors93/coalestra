from __future__ import annotations

from dataclasses import dataclass

from coalestra.core.models import ResourceKey


class CoalestraError(Exception):
    """Base exception for the library."""


class SourceUnavailableError(CoalestraError):
    """Raised when a source cannot serve a resource at this time."""


class SourceTimeoutError(SourceUnavailableError):
    """Raised when a source exceeds its configured timeout."""


class CircuitOpenError(SourceUnavailableError):
    """Raised when a circuit breaker prevents a source call."""


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
    def __init__(self, errors: dict[ResourceKey, Exception]):
        self.errors = errors
        summary = ", ".join(f"{key}={type(error).__name__}" for key, error in errors.items())
        super().__init__(f"Snapshot build failed: {summary}")
