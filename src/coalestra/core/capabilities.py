from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from coalestra.core.diagnostic_schema import ERROR_DIAGNOSTICS_SCHEMA_VERSION
from coalestra.core.health import (
    BUILDER_HEALTH_ASSESSMENT_SCHEMA_VERSION,
    BUILDER_HEALTH_SCHEMA_VERSION,
)

COALESTRA_CAPABILITIES_SCHEMA = "coalestra.capabilities"
COALESTRA_CAPABILITIES_SCHEMA_VERSION = 1
COALESTRA_API_STABILITY = "0.6"

_STABLE_FEATURES = (
    "automatic_observability_buffering",
    "blocking_source_timeout_guarantees",
    "builder_health_assessment",
    "builder_health_serialization",
    "controlled_payload_copy_shutdown",
    "payload_copy_health",
    "payload_copy_offload",
    "snapshot_acceptance",
    "snapshot_consistency",
    "snapshot_requests",
    "source_authority",
    "sync_submission_backlog",
    "transactional_revalidation",
    "versioned_error_diagnostics",
)

_STABLE_SCHEMAS = {
    "builder_health": BUILDER_HEALTH_SCHEMA_VERSION,
    "builder_health_assessment": BUILDER_HEALTH_ASSESSMENT_SCHEMA_VERSION,
    "capabilities": COALESTRA_CAPABILITIES_SCHEMA_VERSION,
    "error_diagnostics": ERROR_DIAGNOSTICS_SCHEMA_VERSION,
}


@dataclass(frozen=True)
class CoalestraCapabilities:
    """Versioned runtime contract for integration compatibility checks."""

    package_version: str
    api_stability: str = COALESTRA_API_STABILITY
    python_requires: str = ">=3.10"
    stability: str = "beta"
    features: tuple[str, ...] = _STABLE_FEATURES
    schemas: Mapping[str, int] = field(default_factory=lambda: _STABLE_SCHEMAS)
    defaults: Mapping[str, Any] = field(
        default_factory=lambda: {
            "allow_unsafe_blocking_sources": False,
            "buffer_observability": "auto",
            "require_source_timeout_declarations": True,
        }
    )

    def __post_init__(self) -> None:
        normalized_features = tuple(dict.fromkeys(str(item) for item in self.features))
        if normalized_features != tuple(sorted(normalized_features)):
            raise ValueError("capability features must be unique and sorted")
        if any(not feature.strip() for feature in normalized_features):
            raise ValueError("capability feature names cannot be empty")
        schema_values = dict(self.schemas)
        for name, version in schema_values.items():
            if not str(name).strip():
                raise ValueError("capability schema names cannot be empty")
            if isinstance(version, bool) or not isinstance(version, int):
                raise TypeError("capability schema versions must be integers")
            if version < 1:
                raise ValueError("capability schema versions must be at least 1")
        default_values = dict(self.defaults)
        object.__setattr__(self, "features", normalized_features)
        object.__setattr__(self, "schemas", MappingProxyType(dict(schema_values)))
        object.__setattr__(self, "defaults", MappingProxyType(dict(default_values)))

    def supports(self, feature: str) -> bool:
        """Return whether one stable feature is available."""

        return str(feature) in self.features

    def require(
        self,
        *,
        features: Iterable[str] = (),
        schemas: Mapping[str, int] | None = None,
    ) -> None:
        """Raise ``CapabilityRequirementError`` when an integration contract is unmet."""

        from coalestra.core.errors import CapabilityRequirementError

        missing = tuple(sorted({str(item) for item in features if not self.supports(str(item))}))
        schema_mismatches: dict[str, tuple[int, int | None]] = {}
        for name, minimum in (schemas or {}).items():
            if isinstance(minimum, bool) or not isinstance(minimum, int):
                raise TypeError("minimum schema versions must be integers")
            if minimum < 1:
                raise ValueError("minimum schema versions must be at least 1")
            current = self.schemas.get(str(name))
            if current is None or current < minimum:
                schema_mismatches[str(name)] = (minimum, current)
        if missing or schema_mismatches:
            raise CapabilityRequirementError(
                missing_features=missing,
                schema_mismatches=schema_mismatches,
            )

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-safe capabilities payload."""

        return {
            "schema": COALESTRA_CAPABILITIES_SCHEMA,
            "schema_version": COALESTRA_CAPABILITIES_SCHEMA_VERSION,
            "package_version": self.package_version,
            "api_stability": self.api_stability,
            "python_requires": self.python_requires,
            "stability": self.stability,
            "features": list(self.features),
            "schemas": dict(self.schemas),
            "defaults": dict(self.defaults),
        }


def build_capabilities(package_version: str) -> CoalestraCapabilities:
    """Build the immutable runtime contract for the installed package version."""

    normalized = str(package_version).strip()
    if not normalized:
        raise ValueError("package_version cannot be empty")
    return CoalestraCapabilities(package_version=normalized)
