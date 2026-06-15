from __future__ import annotations

from typing import TypedDict

ERROR_DIAGNOSTICS_SCHEMA = "coalestra.error-diagnostics"
ERROR_DIAGNOSTICS_SCHEMA_VERSION = 1


class SerializedSourceFailure(TypedDict):
    """Stable JSON-safe representation of one source failure."""

    schema: str
    schema_version: int
    source: str
    error_type: str
    message: str
    attempts: int


class SerializedResourceError(TypedDict):
    """Stable JSON-safe representation of one resource resolution error."""

    schema: str
    schema_version: int
    resource: str
    error_type: str
    message: str
    failures: list[SerializedSourceFailure]


class SerializedSnapshotBuildError(TypedDict):
    """Stable JSON-safe representation of one snapshot build failure."""

    schema: str
    schema_version: int
    error_type: str
    message: str
    partial_snapshot_available: bool
    has_partial_snapshot: bool
    errors: list[SerializedResourceError]
