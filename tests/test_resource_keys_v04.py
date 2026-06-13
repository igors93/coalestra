from __future__ import annotations

import pytest

from coalestra import (
    CASE_INSENSITIVE_KEY_NORMALIZER,
    LEGACY_KEY_NORMALIZER,
    KeyNormalizer,
    ResourceKey,
)


def test_resource_key_preserves_case_by_default() -> None:
    key = ResourceKey(" Tenant-A ", " DocumentId ", "/Path/File")

    assert key.namespace == "Tenant-A"
    assert key.name == "DocumentId"
    assert key.subject == "/Path/File"
    assert key != ResourceKey("tenant-a", "documentid", "/path/file")


def test_resource_key_supports_legacy_and_custom_normalization() -> None:
    legacy = ResourceKey(
        " Market ",
        " Price ",
        " btcusdt ",
        normalizer=LEGACY_KEY_NORMALIZER,
    )
    insensitive = ResourceKey(
        " Tenant-A ",
        " DocumentId ",
        "/Path/File",
        normalizer=CASE_INSENSITIVE_KEY_NORMALIZER,
    )
    custom = ResourceKey(
        " Market ",
        " Price ",
        " btcusdt ",
        normalizer=KeyNormalizer(
            namespace=lambda value: value.strip().upper(),
            name=lambda value: value.strip().lower(),
            subject=lambda value: value.strip().upper(),
        ),
    )

    assert legacy == ResourceKey.legacy("market", "price", "BTCUSDT")
    assert insensitive == ResourceKey("tenant-a", "documentid", "/path/file")
    assert custom.namespace == "MARKET"
    assert custom.name == "price"
    assert custom.subject == "BTCUSDT"


def test_qualifiers_are_order_independent_hashable_and_rendered() -> None:
    first = ResourceKey(
        "market",
        "candles",
        "BTCUSDT",
        {"limit": 500, "interval": "1m"},
    )
    second = ResourceKey(
        "market",
        "candles",
        "BTCUSDT",
        (("interval", "1m"), ("limit", "500")),
    )

    assert first == second
    assert hash(first) == hash(second)
    assert first.qualifier("interval") == "1m"
    assert first.qualifier("missing") is None
    assert str(first) == "market:candles:BTCUSDT?interval=1m&limit=500"


def test_qualifier_helpers_create_new_immutable_keys() -> None:
    base = ResourceKey("market", "candles", "BTCUSDT", {"interval": "1m"})
    changed = base.with_qualifiers({"limit": 100}, interval="5m")
    removed = changed.without_qualifiers("limit")

    assert base.qualifiers == (("interval", "1m"),)
    assert changed.qualifiers == (("interval", "5m"), ("limit", "100"))
    assert removed.qualifiers == (("interval", "5m"),)


def test_duplicate_qualifiers_after_normalization_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate qualifier"):
        ResourceKey(
            "market",
            "candles",
            qualifiers=(("Interval", "1m"), ("interval", "5m")),
            normalizer=LEGACY_KEY_NORMALIZER,
        )
