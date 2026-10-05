from __future__ import annotations

import pytest

from coalestra import (
    CircuitBreakerPolicy,
    FreshnessPolicy,
    PolicyResolver,
    ResourceKey,
    RetryPolicy,
)

PRICE = ResourceKey("market", "price", "BTCUSDT")
ACCOUNT = ResourceKey("account", "summary")


def test_policy_resolver_prefers_exact_override_then_dynamic_then_default() -> None:
    default = FreshnessPolicy(10.0, 30.0)
    price_override = FreshnessPolicy(1.0, 5.0)
    account_dynamic = FreshnessPolicy(3.0, 10.0)
    resolver = PolicyResolver(
        default,
        overrides={PRICE: price_override},
        dynamic=lambda key: account_dynamic if key.namespace == "account" else default,
    )

    assert resolver.resolve(PRICE) is price_override
    assert resolver.resolve(ACCOUNT) is account_dynamic
    assert resolver.resolve(ResourceKey("other", "value")) is default


@pytest.mark.parametrize("field", ["base_delay_seconds", "max_delay_seconds", "jitter_ratio"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_retry_policy_rejects_non_finite_settings(field: str, value: float) -> None:
    with pytest.raises(ValueError, match=f"{field} must be finite"):
        RetryPolicy(**{field: value})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_circuit_policy_rejects_non_finite_recovery_timeout(value: float) -> None:
    with pytest.raises(ValueError, match="recovery_timeout_seconds must be finite"):
        CircuitBreakerPolicy(recovery_timeout_seconds=value)
