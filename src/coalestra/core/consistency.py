from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from math import isfinite
from typing import Any

from coalestra.core.keys import ResourceKey
from coalestra.core.models import SnapshotValue


@dataclass(frozen=True)
class SnapshotConsistencyPolicy:
    """Controls temporal consistency checks for a group of snapshot resources."""

    max_observation_skew_seconds: float
    include_optional_resources: bool = False

    def __post_init__(self) -> None:
        raw_limit = self.max_observation_skew_seconds
        if isinstance(raw_limit, bool) or not isinstance(raw_limit, (int, float)):
            raise TypeError("max_observation_skew_seconds must be a number")
        value = float(raw_limit)
        if not isfinite(value):
            raise ValueError("max_observation_skew_seconds must be finite")
        if value < 0:
            raise ValueError("max_observation_skew_seconds cannot be negative")
        if not isinstance(self.include_optional_resources, bool):
            raise TypeError("include_optional_resources must be a boolean")
        object.__setattr__(self, "max_observation_skew_seconds", value)


@dataclass(frozen=True)
class ObservationSkewViolation:
    """Details for one group whose observation timestamps exceed its allowed skew."""

    keys: tuple[ResourceKey, ...]
    oldest_key: ResourceKey
    oldest_observed_at: float
    newest_key: ResourceKey
    newest_observed_at: float
    observation_skew_seconds: float
    max_observation_skew_seconds: float


def find_observation_skew_violation(
    resources: Mapping[ResourceKey, SnapshotValue[Any]],
    keys: Iterable[ResourceKey],
    policy: SnapshotConsistencyPolicy,
) -> ObservationSkewViolation | None:
    """Return the timestamp-skew violation for resolved keys, when one exists."""

    requested = tuple(dict.fromkeys(keys))
    resolved = tuple((key, resources[key]) for key in requested if key in resources)
    if len(resolved) < 2:
        return None

    oldest_key, oldest_value = min(resolved, key=lambda item: item[1].observed_at)
    newest_key, newest_value = max(resolved, key=lambda item: item[1].observed_at)
    skew_seconds = max(0.0, newest_value.observed_at - oldest_value.observed_at)
    if skew_seconds <= policy.max_observation_skew_seconds:
        return None

    return ObservationSkewViolation(
        keys=tuple(key for key, _value in resolved),
        oldest_key=oldest_key,
        oldest_observed_at=oldest_value.observed_at,
        newest_key=newest_key,
        newest_observed_at=newest_value.observed_at,
        observation_skew_seconds=skew_seconds,
        max_observation_skew_seconds=policy.max_observation_skew_seconds,
    )
