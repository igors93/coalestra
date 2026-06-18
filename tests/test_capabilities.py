from __future__ import annotations

import json

import pytest

import coalestra
from coalestra import CapabilityRequirementError


def test_capabilities_publish_stable_v060_contract() -> None:
    installed = coalestra.capabilities()
    payload = installed.to_dict()

    assert installed.package_version == "0.6.2"
    assert installed.api_stability == "0.6"
    assert installed.stability == "beta"
    assert installed.supports("snapshot_acceptance")
    assert installed.supports("blocking_source_timeout_guarantees")
    assert installed.defaults["require_source_timeout_declarations"] is True
    assert payload["schema"] == "coalestra.capabilities"
    assert payload["schema_version"] == 1
    assert json.loads(json.dumps(payload, allow_nan=False)) == payload


def test_capability_requirements_accept_supported_contract() -> None:
    installed = coalestra.require_capabilities(
        features=(
            "builder_health_serialization",
            "snapshot_acceptance",
            "transactional_revalidation",
        ),
        schemas={
            "builder_health": 1,
            "error_diagnostics": 1,
        },
    )

    assert installed.package_version == coalestra.__version__


def test_capability_requirements_report_missing_contract() -> None:
    with pytest.raises(CapabilityRequirementError) as captured:
        coalestra.require_capabilities(
            features=("future-feature",),
            schemas={"builder_health": 999},
        )

    error = captured.value
    assert error.missing_features == ("future-feature",)
    assert error.schema_mismatches == {"builder_health": (999, 1)}
    assert "future-feature" in str(error)
