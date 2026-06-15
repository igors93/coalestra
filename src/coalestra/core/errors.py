from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from coalestra.core.diagnostic_schema import (
    ERROR_DIAGNOSTICS_SCHEMA,
    ERROR_DIAGNOSTICS_SCHEMA_VERSION,
    SerializedResourceError,
    SerializedSnapshotBuildError,
    SerializedSourceFailure,
)
from coalestra.core.models import ResourceKey

_DEFAULT_MESSAGE_LIMIT = 300
_DEFAULT_FAILURE_LIMIT = 3
_DEFAULT_RESOURCE_LIMIT = 5


def _compact_message(
    value: object,
    *,
    max_length: int = _DEFAULT_MESSAGE_LIMIT,
) -> str:
    if max_length < 1:
        raise ValueError("max_length must be positive")

    text = " ".join(str(value).split()) or "<no message>"

    if len(text) <= max_length:
        return text

    if max_length <= 3:
        return text[:max_length]

    return f"{text[: max_length - 3]}..."


class CoalestraError(Exception):
    """Base exception for the library."""


class SourceUnavailableError(CoalestraError):
    """Raised when a source cannot serve a resource at this time."""


class SourceTimeoutError(SourceUnavailableError):
    """Raised when a source exceeds its configured timeout."""


class SourceQueueTimeoutError(SourceTimeoutError):
    """Raised when a source cannot acquire Coalestra capacity in time."""


class SnapshotDeadlineExceededError(SourceTimeoutError):
    """Raised when the overall snapshot deadline is exhausted."""


class SubmissionBacklogFullError(CoalestraError):
    """Raised when the synchronous non-blocking submission backlog is full."""

    def __init__(self, *, limit: int, pending: int) -> None:
        self.limit = int(limit)
        self.pending = int(pending)
        super().__init__(
            f"Non-blocking submission backlog is full (pending={self.pending}, limit={self.limit})"
        )


class PayloadIsolationError(CoalestraError):
    """Raised when a payload cannot be safely copied across an isolation boundary."""

    def __init__(self, *, context: str, value_type: str) -> None:
        self.context = context
        self.value_type = value_type
        super().__init__(
            f"Unable to isolate {context}; payload type {value_type} does not support copying"
        )


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
    """Raised when a derived source cannot resolve all dependencies."""

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

    def format(
        self,
        *,
        max_message_length: int = _DEFAULT_MESSAGE_LIMIT,
    ) -> str:
        message = _compact_message(
            self.message,
            max_length=max_message_length,
        )

        return f"{self.source}: {self.error_type}({message}; attempts={self.attempts})"

    def to_dict(
        self,
        *,
        max_message_length: int = _DEFAULT_MESSAGE_LIMIT,
    ) -> SerializedSourceFailure:
        return {
            "schema": ERROR_DIAGNOSTICS_SCHEMA,
            "schema_version": ERROR_DIAGNOSTICS_SCHEMA_VERSION,
            "source": self.source,
            "error_type": self.error_type,
            "message": _compact_message(
                self.message,
                max_length=max_message_length,
            ),
            "attempts": self.attempts,
        }


class ResourceResolutionError(CoalestraError):
    def __init__(
        self,
        key: ResourceKey,
        failures: tuple[SourceFailure, ...],
    ):
        self.key = key
        self.failures = failures

        super().__init__(f"Unable to resolve {key}: {self.format_failures()}")

    def format_failures(
        self,
        *,
        max_failures: int = _DEFAULT_FAILURE_LIMIT,
        max_message_length: int = _DEFAULT_MESSAGE_LIMIT,
    ) -> str:
        if max_failures < 1:
            raise ValueError("max_failures must be positive")

        visible = self.failures[:max_failures]

        details = (
            "; ".join(
                failure.format(
                    max_message_length=max_message_length,
                )
                for failure in visible
            )
            or "no compatible source"
        )

        hidden = len(self.failures) - len(visible)

        if hidden > 0:
            details = f"{details}; +{hidden} more failure(s)"

        return details

    def to_dict(
        self,
        *,
        max_message_length: int = _DEFAULT_MESSAGE_LIMIT,
    ) -> SerializedResourceError:
        return {
            "schema": ERROR_DIAGNOSTICS_SCHEMA,
            "schema_version": ERROR_DIAGNOSTICS_SCHEMA_VERSION,
            "resource": str(self.key),
            "error_type": type(self).__name__,
            "message": _compact_message(
                self,
                max_length=max_message_length,
            ),
            "failures": [
                failure.to_dict(
                    max_message_length=max_message_length,
                )
                for failure in self.failures
            ],
        }


class SnapshotBuildError(CoalestraError):
    def __init__(
        self,
        errors: Mapping[ResourceKey, Exception],
        *,
        snapshot: Any | None = None,
    ) -> None:
        self.errors = dict(errors)
        self.snapshot = snapshot

        super().__init__(f"Snapshot build failed: {self._format_summary()}")

    def _format_summary(
        self,
        *,
        max_resources: int = _DEFAULT_RESOURCE_LIMIT,
        max_failures: int = _DEFAULT_FAILURE_LIMIT,
        max_message_length: int = _DEFAULT_MESSAGE_LIMIT,
    ) -> str:
        if max_resources < 1:
            raise ValueError("max_resources must be positive")

        items = list(self.errors.items())
        visible = items[:max_resources]
        rendered: list[str] = []

        for key, error in visible:
            if isinstance(error, ResourceResolutionError):
                details = error.format_failures(
                    max_failures=max_failures,
                    max_message_length=max_message_length,
                )

                rendered.append(f"{key}=ResourceResolutionError[{details}]")
                continue

            rendered.append(
                f"{key}={type(error).__name__}("
                f"{_compact_message(error, max_length=max_message_length)}"
                f")"
            )

        hidden = len(items) - len(visible)

        if hidden > 0:
            rendered.append(f"+{hidden} more resource error(s)")

        return ", ".join(rendered) or "no resource details"

    def to_dict(
        self,
        *,
        max_message_length: int = _DEFAULT_MESSAGE_LIMIT,
    ) -> SerializedSnapshotBuildError:
        details: list[SerializedResourceError] = []

        for key, error in self.errors.items():
            if isinstance(error, ResourceResolutionError):
                details.append(
                    error.to_dict(
                        max_message_length=max_message_length,
                    )
                )
                continue

            details.append(
                {
                    "schema": ERROR_DIAGNOSTICS_SCHEMA,
                    "schema_version": ERROR_DIAGNOSTICS_SCHEMA_VERSION,
                    "resource": str(key),
                    "error_type": type(error).__name__,
                    "message": _compact_message(
                        error,
                        max_length=max_message_length,
                    ),
                    "failures": [],
                }
            )

        partial_snapshot_available = self.snapshot is not None
        return {
            "schema": ERROR_DIAGNOSTICS_SCHEMA,
            "schema_version": ERROR_DIAGNOSTICS_SCHEMA_VERSION,
            "error_type": type(self).__name__,
            "message": _compact_message(
                self,
                max_length=max_message_length,
            ),
            "partial_snapshot_available": partial_snapshot_available,
            # Kept in schema version 1 for compatibility with Coalestra 0.5.1-0.5.4.
            "has_partial_snapshot": partial_snapshot_available,
            "errors": details,
        }


class SnapshotConsistencyError(SnapshotBuildError):
    """Raised when resolved resources violate a declared consistency policy."""

    def __init__(
        self,
        *,
        keys: tuple[ResourceKey, ...],
        oldest_key: ResourceKey,
        oldest_observed_at: float,
        newest_key: ResourceKey,
        newest_observed_at: float,
        observation_skew_seconds: float,
        max_observation_skew_seconds: float,
        snapshot: Any | None = None,
    ) -> None:
        self.errors: dict[ResourceKey, Exception] = {}
        self.snapshot = snapshot
        self.keys = keys
        self.oldest_key = oldest_key
        self.oldest_observed_at = float(oldest_observed_at)
        self.newest_key = newest_key
        self.newest_observed_at = float(newest_observed_at)
        self.observation_skew_seconds = float(observation_skew_seconds)
        self.max_observation_skew_seconds = float(max_observation_skew_seconds)
        CoalestraError.__init__(
            self,
            "Snapshot observation skew "
            f"{self.observation_skew_seconds:.6f}s exceeds the allowed "
            f"{self.max_observation_skew_seconds:.6f}s across {len(self.keys)} resource(s); "
            f"oldest={self.oldest_key}@{self.oldest_observed_at:.6f}, "
            f"newest={self.newest_key}@{self.newest_observed_at:.6f}",
        )
