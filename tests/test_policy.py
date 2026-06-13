from __future__ import annotations

from coalestra import FreshnessPolicy, PolicyResolver, ResourceKey

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
