from __future__ import annotations

from collections.abc import Callable, Mapping

from coalestra.core.models import FreshnessPolicy, ResourceKey

FreshnessResolver = Callable[[ResourceKey], FreshnessPolicy]


class PolicyResolver:
    def __init__(
        self,
        default: FreshnessPolicy,
        overrides: Mapping[ResourceKey, FreshnessPolicy] | None = None,
        dynamic: FreshnessResolver | None = None,
    ) -> None:
        self._default = default
        self._overrides = dict(overrides or {})
        self._dynamic = dynamic

    def resolve(self, key: ResourceKey) -> FreshnessPolicy:
        if key in self._overrides:
            return self._overrides[key]
        if self._dynamic is not None:
            return self._dynamic(key)
        return self._default
