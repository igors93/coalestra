from __future__ import annotations

import pytest

from coalestra import LEGACY_KEY_NORMALIZER, FreshnessPolicy, ResourceKey


def test_resource_key_normalizes_identity() -> None:
    key = ResourceKey(" Market ", " Price ", " btcusdt ", normalizer=LEGACY_KEY_NORMALIZER)

    assert key.namespace == "market"
    assert key.name == "price"
    assert key.subject == "BTCUSDT"
    assert str(key) == "market:price:BTCUSDT"


def test_freshness_policy_rejects_invalid_windows() -> None:
    with pytest.raises(ValueError):
        FreshnessPolicy(ttl_seconds=-1.0, max_stale_seconds=1.0)
    with pytest.raises(ValueError):
        FreshnessPolicy(ttl_seconds=5.0, max_stale_seconds=4.0)


@pytest.mark.parametrize("field", ["ttl_seconds", "max_stale_seconds"])
def test_freshness_policy_rejects_nan_windows(field: str) -> None:
    values = {"ttl_seconds": 1.0, "max_stale_seconds": 2.0}
    values[field] = float("nan")
    with pytest.raises(ValueError, match="cannot be NaN"):
        FreshnessPolicy(**values)


def test_freshness_policy_keeps_intentional_infinite_windows() -> None:
    policy = FreshnessPolicy(float("inf"), float("inf"))
    assert policy.ttl_seconds == float("inf")
    assert policy.max_stale_seconds == float("inf")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_freshness_policy_rejects_non_finite_refresh_ahead(value: float) -> None:
    with pytest.raises(ValueError, match="refresh_ahead_seconds must be finite"):
        FreshnessPolicy(1.0, 2.0, refresh_ahead_seconds=value)
