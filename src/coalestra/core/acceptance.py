from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from math import isfinite
from types import MappingProxyType
from typing import Any

from coalestra.core.keys import ResourceKey
from coalestra.core.models import FreshnessPolicy, SnapshotValue


class SnapshotAcceptanceReason(str, Enum):
    """Stable reasons reported when a snapshot does not satisfy its acceptance policy."""

    STALE = "stale"
    TOO_OLD = "too_old"
    INSUFFICIENT_AUTHORITY = "insufficient_authority"
    REQUIREMENT_UNSATISFIED = "requirement_unsatisfied"


@dataclass(frozen=True)
class ResourceAcceptanceRule:
    """Quality constraints applied to one resolved resource.

    A rule does not make an optional resource mandatory. Presence requirements are expressed
    through ``SnapshotRequirement`` while ``SnapshotRequest.required`` retains its existing
    all-required semantics.
    """

    max_age_seconds: float | None = None
    allow_stale: bool = True
    minimum_authority_rank: int | None = None

    def __post_init__(self) -> None:
        if self.max_age_seconds is not None:
            raw_max_age = self.max_age_seconds
            if isinstance(raw_max_age, bool) or not isinstance(raw_max_age, (int, float)):
                raise TypeError("max_age_seconds must be a number or None")
            normalized_max_age = float(raw_max_age)
            if not isfinite(normalized_max_age):
                raise ValueError("max_age_seconds must be finite")
            if normalized_max_age < 0:
                raise ValueError("max_age_seconds cannot be negative")
            object.__setattr__(self, "max_age_seconds", normalized_max_age)

        if not isinstance(self.allow_stale, bool):
            raise TypeError("allow_stale must be a boolean")

        if self.minimum_authority_rank is not None:
            raw_rank = self.minimum_authority_rank
            if isinstance(raw_rank, bool) or not isinstance(raw_rank, int):
                raise TypeError("minimum_authority_rank must be an integer or None")
            object.__setattr__(self, "minimum_authority_rank", int(raw_rank))

    @property
    def has_constraints(self) -> bool:
        """Whether the rule can reject a resolved resource."""

        return (
            self.max_age_seconds is not None
            or not self.allow_stale
            or self.minimum_authority_rank is not None
        )


@dataclass(frozen=True)
class SnapshotRequirement:
    """Require at least ``minimum_count`` acceptable resources from a stable key group."""

    keys: tuple[ResourceKey, ...]
    minimum_count: int
    name: str = ""

    def __init__(
        self,
        keys: Iterable[ResourceKey],
        *,
        minimum_count: int,
        name: str = "",
    ) -> None:
        normalized_keys = tuple(dict.fromkeys(keys))
        if not normalized_keys:
            raise ValueError("snapshot requirement must contain at least one resource key")
        if any(not isinstance(key, ResourceKey) for key in normalized_keys):
            raise TypeError("snapshot requirement keys must be ResourceKey instances")
        if isinstance(minimum_count, bool) or not isinstance(minimum_count, int):
            raise TypeError("minimum_count must be an integer")
        if minimum_count < 1 or minimum_count > len(normalized_keys):
            raise ValueError("minimum_count must be between 1 and the number of keys")
        normalized_name = str(name).strip()
        object.__setattr__(self, "keys", normalized_keys)
        object.__setattr__(self, "minimum_count", int(minimum_count))
        object.__setattr__(self, "name", normalized_name)

    @classmethod
    def all_of(
        cls,
        keys: Iterable[ResourceKey],
        *,
        name: str = "",
    ) -> SnapshotRequirement:
        """Require every resource in ``keys`` to be present and acceptable."""

        normalized = tuple(dict.fromkeys(keys))
        return cls(normalized, minimum_count=len(normalized), name=name)

    @classmethod
    def any_of(
        cls,
        keys: Iterable[ResourceKey],
        *,
        name: str = "",
    ) -> SnapshotRequirement:
        """Require at least one resource in ``keys`` to be present and acceptable."""

        return cls(keys, minimum_count=1, name=name)

    @classmethod
    def at_least(
        cls,
        minimum_count: int,
        keys: Iterable[ResourceKey],
        *,
        name: str = "",
    ) -> SnapshotRequirement:
        """Require an explicit number of acceptable resources from ``keys``."""

        return cls(keys, minimum_count=minimum_count, name=name)


@dataclass(frozen=True)
class SnapshotAcceptancePolicy:
    """Declare freshness, authority, and resource-presence requirements for a snapshot.

    The default rule is applied to required request resources. Optional request resources are
    checked only when ``include_optional_resources`` is true, when they have an explicit rule, or
    when they participate in a requirement group.
    """

    default_rule: ResourceAcceptanceRule = field(default_factory=ResourceAcceptanceRule)
    resource_rules: Mapping[ResourceKey, ResourceAcceptanceRule] = field(default_factory=dict)
    requirements: tuple[SnapshotRequirement, ...] = ()
    include_optional_resources: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.default_rule, ResourceAcceptanceRule):
            raise TypeError("default_rule must be a ResourceAcceptanceRule")
        if not isinstance(self.include_optional_resources, bool):
            raise TypeError("include_optional_resources must be a boolean")

        normalized_rules: dict[ResourceKey, ResourceAcceptanceRule] = {}
        for key, rule in self.resource_rules.items():
            if not isinstance(key, ResourceKey):
                raise TypeError("resource_rules keys must be ResourceKey instances")
            if not isinstance(rule, ResourceAcceptanceRule):
                raise TypeError("resource_rules values must be ResourceAcceptanceRule instances")
            normalized_rules[key] = rule

        normalized_requirements = tuple(self.requirements)
        if any(not isinstance(item, SnapshotRequirement) for item in normalized_requirements):
            raise TypeError("requirements must contain SnapshotRequirement instances")

        if not (
            self.default_rule.has_constraints
            or any(rule.has_constraints for rule in normalized_rules.values())
            or normalized_requirements
        ):
            raise ValueError("snapshot acceptance policy must declare at least one constraint")

        object.__setattr__(self, "resource_rules", MappingProxyType(normalized_rules))
        object.__setattr__(self, "requirements", normalized_requirements)

    @property
    def referenced_keys(self) -> tuple[ResourceKey, ...]:
        """Return every key named explicitly by a rule or requirement."""

        keys: list[ResourceKey] = list(self.resource_rules)
        for requirement in self.requirements:
            keys.extend(requirement.keys)
        return tuple(dict.fromkeys(keys))

    def rule_for(self, key: ResourceKey) -> ResourceAcceptanceRule:
        """Return the explicit rule for ``key`` or the policy default."""

        return self.resource_rules.get(key, self.default_rule)


@dataclass(frozen=True)
class SnapshotAcceptanceViolation:
    """One immutable explanation for a rejected snapshot candidate."""

    reason: SnapshotAcceptanceReason
    keys: tuple[ResourceKey, ...]
    message: str
    key: ResourceKey | None = None
    requirement_name: str = ""
    current_age_seconds: float | None = None
    max_age_seconds: float | None = None
    stale: bool | None = None
    authority_rank: int | None = None
    minimum_authority_rank: int | None = None
    accepted_count: int | None = None
    required_count: int | None = None


def find_snapshot_acceptance_violations(
    resources: Mapping[ResourceKey, SnapshotValue[Any]],
    *,
    required_keys: Iterable[ResourceKey],
    optional_keys: Iterable[ResourceKey] = (),
    policy: SnapshotAcceptancePolicy,
    now: float,
    freshness_policy_for: Callable[[ResourceKey], FreshnessPolicy],
) -> tuple[SnapshotAcceptanceViolation, ...]:
    """Evaluate one snapshot candidate without mutating it or performing I/O."""

    evaluated_at = float(now)
    if not isfinite(evaluated_at):
        raise ValueError("snapshot acceptance evaluation time must be finite")

    normalized_required = tuple(dict.fromkeys(required_keys))
    normalized_optional = tuple(dict.fromkeys(optional_keys))
    directly_enforced: list[ResourceKey] = list(normalized_required)
    if policy.include_optional_resources:
        directly_enforced.extend(normalized_optional)
    directly_enforced.extend(policy.resource_rules)
    directly_enforced_keys = frozenset(directly_enforced)

    participating: list[ResourceKey] = list(directly_enforced)
    for requirement in policy.requirements:
        participating.extend(requirement.keys)
    participating_keys = tuple(dict.fromkeys(participating))

    violations: list[SnapshotAcceptanceViolation] = []
    resource_violations: dict[ResourceKey, list[SnapshotAcceptanceViolation]] = {}
    acceptable: dict[ResourceKey, bool] = {}

    for key in participating_keys:
        value = resources.get(key)
        if value is None:
            acceptable[key] = False
            continue

        rule = policy.rule_for(key)
        current_age = max(0.0, float(value.age_seconds), evaluated_at - value.observed_at)
        freshness_policy = freshness_policy_for(key)
        dynamically_stale = bool(value.stale or current_age > freshness_policy.ttl_seconds)
        key_violations: list[SnapshotAcceptanceViolation] = []

        if not rule.allow_stale and dynamically_stale:
            key_violations.append(
                SnapshotAcceptanceViolation(
                    reason=SnapshotAcceptanceReason.STALE,
                    keys=(key,),
                    key=key,
                    message=f"resource {key} is stale and the policy does not allow stale values",
                    current_age_seconds=current_age,
                    stale=True,
                )
            )

        if rule.max_age_seconds is not None and current_age > rule.max_age_seconds:
            key_violations.append(
                SnapshotAcceptanceViolation(
                    reason=SnapshotAcceptanceReason.TOO_OLD,
                    keys=(key,),
                    key=key,
                    message=(
                        f"resource {key} age {current_age:.6f}s exceeds the allowed "
                        f"{rule.max_age_seconds:.6f}s"
                    ),
                    current_age_seconds=current_age,
                    max_age_seconds=rule.max_age_seconds,
                    stale=dynamically_stale,
                )
            )

        if (
            rule.minimum_authority_rank is not None
            and value.authority_rank < rule.minimum_authority_rank
        ):
            key_violations.append(
                SnapshotAcceptanceViolation(
                    reason=SnapshotAcceptanceReason.INSUFFICIENT_AUTHORITY,
                    keys=(key,),
                    key=key,
                    message=(
                        f"resource {key} authority rank {value.authority_rank} is below the "
                        f"required {rule.minimum_authority_rank}"
                    ),
                    authority_rank=value.authority_rank,
                    minimum_authority_rank=rule.minimum_authority_rank,
                )
            )

        resource_violations[key] = key_violations
        acceptable[key] = not key_violations
        if key in directly_enforced_keys:
            violations.extend(key_violations)

    for requirement in policy.requirements:
        accepted_count = sum(1 for key in requirement.keys if acceptable.get(key, False))
        if accepted_count >= requirement.minimum_count:
            continue
        for key in requirement.keys:
            if key not in directly_enforced_keys:
                violations.extend(resource_violations.get(key, ()))
        label = f" {requirement.name!r}" if requirement.name else ""
        violations.append(
            SnapshotAcceptanceViolation(
                reason=SnapshotAcceptanceReason.REQUIREMENT_UNSATISFIED,
                keys=requirement.keys,
                requirement_name=requirement.name,
                message=(
                    f"snapshot requirement{label} accepted {accepted_count} of "
                    f"{len(requirement.keys)} resource(s), but requires at least "
                    f"{requirement.minimum_count}"
                ),
                accepted_count=accepted_count,
                required_count=requirement.minimum_count,
            )
        )

    return tuple(violations)
