from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from coalestra.core.acceptance import SnapshotAcceptancePolicy
from coalestra.core.consistency import SnapshotConsistencyPolicy
from coalestra.core.keys import ResourceKey


@dataclass(frozen=True)
class SnapshotRequest:
    """Declarative resource set with explicit required, optional, and quality constraints.

    Required failures raise ``SnapshotBuildError`` when resolved through
    ``SnapshotBuilder.build_request``. Optional failures remain available in the returned
    snapshot without making the request fail. Acceptance and consistency policies are opt-in
    invariants and reject an otherwise resolved snapshot when their constraints are not met.
    """

    required: tuple[ResourceKey, ...] = ()
    optional: tuple[ResourceKey, ...] = ()
    consistency_policy: SnapshotConsistencyPolicy | None = None
    acceptance_policy: SnapshotAcceptancePolicy | None = None

    def __init__(
        self,
        *,
        required: Iterable[ResourceKey] = (),
        optional: Iterable[ResourceKey] = (),
        consistency_policy: SnapshotConsistencyPolicy | None = None,
        acceptance_policy: SnapshotAcceptancePolicy | None = None,
    ) -> None:
        required_keys = tuple(dict.fromkeys(required))
        optional_keys = tuple(dict.fromkeys(optional))
        overlap = set(required_keys).intersection(optional_keys)
        if overlap:
            rendered = ", ".join(str(key) for key in sorted(overlap))
            raise ValueError(f"resource keys cannot be both required and optional: {rendered}")
        if not required_keys and not optional_keys:
            raise ValueError("snapshot request must contain at least one resource key")
        if consistency_policy is not None and not isinstance(
            consistency_policy, SnapshotConsistencyPolicy
        ):
            raise TypeError("consistency_policy must be a SnapshotConsistencyPolicy or None")
        if acceptance_policy is not None and not isinstance(
            acceptance_policy, SnapshotAcceptancePolicy
        ):
            raise TypeError("acceptance_policy must be a SnapshotAcceptancePolicy or None")

        request_keys = {*required_keys, *optional_keys}
        if acceptance_policy is not None:
            unknown = set(acceptance_policy.referenced_keys) - request_keys
            if unknown:
                rendered = ", ".join(str(key) for key in sorted(unknown))
                raise ValueError(
                    "acceptance policy references resources outside the snapshot request: "
                    f"{rendered}"
                )

        object.__setattr__(self, "required", required_keys)
        object.__setattr__(self, "optional", optional_keys)
        object.__setattr__(self, "consistency_policy", consistency_policy)
        object.__setattr__(self, "acceptance_policy", acceptance_policy)

    @property
    def keys(self) -> tuple[ResourceKey, ...]:
        """All keys in stable required-then-optional order."""

        return (*self.required, *self.optional)

    def is_required(self, key: ResourceKey) -> bool:
        return key in self.required
