from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from coalestra.core.keys import ResourceKey

AuthorityResolver = Callable[[ResourceKey], "SourceAuthorityPolicy"]


@dataclass(frozen=True)
class SourceAuthorityPolicy:
    """Assign stable authority ranks to source names.

    Higher ranks represent more authoritative sources. Sources with the same
    rank continue to use observation time as the conflict-resolution rule.
    """

    source_ranks: Mapping[str, int] = field(default_factory=dict)
    default_rank: int = 0

    def __post_init__(self) -> None:
        normalized: dict[str, int] = {}
        for source, rank in self.source_ranks.items():
            name = str(source or "").strip()
            if not name:
                raise ValueError("authority source names cannot be empty")
            if name in normalized:
                raise ValueError(f"duplicate authority source name: {name}")
            normalized[name] = int(rank)
        object.__setattr__(self, "source_ranks", MappingProxyType(normalized))
        object.__setattr__(self, "default_rank", int(self.default_rank))

    def rank_for(self, source: str) -> int:
        """Return the configured authority rank for ``source``."""

        normalized = str(source or "").strip()
        if not normalized:
            raise ValueError("source name cannot be empty")
        return self.source_ranks.get(normalized, self.default_rank)


class AuthorityPolicyResolver:
    """Resolve source-authority policy for one resource key."""

    def __init__(
        self,
        default: SourceAuthorityPolicy | None = None,
        overrides: Mapping[ResourceKey, SourceAuthorityPolicy] | None = None,
        dynamic: AuthorityResolver | None = None,
    ) -> None:
        self._default = default or SourceAuthorityPolicy()
        self._overrides = dict(overrides or {})
        self._dynamic = dynamic

    def resolve(self, key: ResourceKey) -> SourceAuthorityPolicy:
        if key in self._overrides:
            return self._overrides[key]
        if self._dynamic is not None:
            policy = self._dynamic(key)
            if not isinstance(policy, SourceAuthorityPolicy):
                raise TypeError("authority resolver must return SourceAuthorityPolicy")
            return policy
        return self._default

    @property
    def has_rules(self) -> bool:
        """Return whether this resolver can produce non-default authority semantics."""

        return bool(
            self._default.source_ranks
            or self._default.default_rank != 0
            or self._overrides
            or self._dynamic is not None
        )

    def rank_for(self, key: ResourceKey, source: str) -> int:
        """Return the authority rank for one resource and source."""

        return self.resolve(key).rank_for(source)
