from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

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
    ) -> dict[str, Any]:
        return {
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
    ) -> dict[str, Any]:
        return {
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
    ) -> dict[str, Any]:
        details: list[dict[str, Any]] = []

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
                    "resource": str(key),
                    "error_type": type(error).__name__,
                    "message": _compact_message(
                        error,
                        max_length=max_message_length,
                    ),
                    "failures": [],
                }
            )

        return {
            "error_type": type(self).__name__,
            "message": _compact_message(
                self,
                max_length=max_message_length,
            ),
            "has_partial_snapshot": self.snapshot is not None,
            "errors": details,
        }
