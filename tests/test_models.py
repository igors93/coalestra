from __future__ import annotations

import pytest

from coalestra import FreshnessPolicy, ResourceKey


def test_resource_key_normalizes_identity() -> None:
    key = ResourceKey(" Market ", " Price ", " btcusdt ")

    assert key.namespace == "market"
    assert key.name == "price"
    assert key.subject == "BTCUSDT"
    assert str(key) == "market:price:BTCUSDT"


def test_freshness_policy_rejects_invalid_windows() -> None:
    with pytest.raises(ValueError):
        FreshnessPolicy(ttl_seconds=-1.0, max_stale_seconds=1.0)
    with pytest.raises(ValueError):
        FreshnessPolicy(ttl_seconds=5.0, max_stale_seconds=4.0)
