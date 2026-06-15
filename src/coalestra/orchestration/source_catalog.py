from __future__ import annotations

from collections import OrderedDict
from collections.abc import Collection, Iterable
from typing import Literal

from coalestra.core.diagnostics import DiagnosticsCollector
from coalestra.core.models import ResourceKey
from coalestra.core.protocols import Source, SourceBase
from coalestra.resilience.circuit_breaker import CircuitBreaker, CircuitIdentity
from coalestra.resilience.policy import ResiliencePolicyResolver, SourceResiliencePolicy

SourceKind = Literal["single", "batch", "derived"]


class SourceCatalog:
    """Validate, order, classify, and query the builder's source collection."""

    def __init__(
        self,
        sources: Iterable[Source],
        *,
        resilience_resolver: ResiliencePolicyResolver,
        circuit_breaker: CircuitBreaker,
        cache_supports: bool,
        support_cache_max_entries: int | None,
    ) -> None:
        source_list = list(sources)
        if not source_list:
            raise ValueError("at least one source is required")
        if support_cache_max_entries is not None and support_cache_max_entries < 1:
            raise ValueError("source_support_cache_max_entries must be at least 1 or None")

        names = [source.name for source in source_list]
        if len(names) != len(set(names)):
            raise ValueError("source names must be unique")
        for source in source_list:
            self._validate_source(source)

        self.sources = tuple(sorted(source_list, key=lambda item: item.priority, reverse=True))
        self.kinds = {source.name: self._source_kind(source) for source in self.sources}
        self.resilience_resolver = resilience_resolver
        self.circuit_breaker = circuit_breaker
        self.cache_supports = bool(cache_supports)
        self.support_cache_max_entries = support_cache_max_entries
        self.support_cache: OrderedDict[tuple[str, ResourceKey], bool] = OrderedDict()

    def clear_support_cache(self) -> None:
        """Forget memoized ``source.supports(key)`` decisions."""

        self.support_cache.clear()

    @property
    def support_cache_size(self) -> int:
        return len(self.support_cache)

    def kind(self, source: SourceBase) -> SourceKind:
        return self.kinds[source.name]

    def supports(
        self,
        source: SourceBase,
        key: ResourceKey,
        diagnostics: DiagnosticsCollector,
    ) -> bool:
        cacheable = self.cache_supports and bool(getattr(source, "cache_supports", True))
        cache_key = (source.name, key)
        if cacheable and cache_key in self.support_cache:
            diagnostics.support_cache_hits += 1
            self.support_cache.move_to_end(cache_key)
            return self.support_cache[cache_key]

        diagnostics.support_cache_misses += 1
        supported = bool(source.supports(key))
        if cacheable:
            self.support_cache[cache_key] = supported
            self.support_cache.move_to_end(cache_key)
            if self.support_cache_max_entries is not None:
                while len(self.support_cache) > self.support_cache_max_entries:
                    self.support_cache.popitem(last=False)
        return supported

    def resilience_for(self, source: SourceBase) -> SourceResiliencePolicy:
        declared = getattr(source, "resilience_policy", None)
        if declared is not None and not isinstance(declared, SourceResiliencePolicy):
            raise TypeError(
                f"source {source.name} resilience_policy must be SourceResiliencePolicy or None"
            )
        return self.resilience_resolver.resolve(source.name, declared=declared)

    def circuit_groups(
        self,
        source: SourceBase,
        keys: Collection[ResourceKey],
        resilience: SourceResiliencePolicy,
    ) -> list[tuple[ResourceKey, tuple[ResourceKey, ...]]]:
        grouped: dict[CircuitIdentity, list[ResourceKey]] = {}
        for key in keys:
            identity = self.circuit_breaker.identity_for(
                source.name,
                key=key,
                scope=resilience.circuit.scope,
            )
            grouped.setdefault(identity, []).append(key)
        return [(items[0], tuple(items)) for items in grouped.values()]

    @staticmethod
    def _source_kind(source: Source) -> SourceKind:
        if callable(getattr(source, "dependencies", None)) and callable(
            getattr(source, "derive", None)
        ):
            return "derived"
        if callable(getattr(source, "fetch_many", None)):
            return "batch"
        return "single"

    @classmethod
    def _validate_source(cls, source: Source) -> None:
        if not str(getattr(source, "name", "")).strip():
            raise ValueError("source name cannot be empty")
        if not callable(getattr(source, "supports", None)):
            raise TypeError(f"source {source.name} must define supports()")
        max_concurrency = getattr(source, "max_concurrency", None)
        if max_concurrency is not None and int(max_concurrency) < 1:
            raise ValueError(f"source {source.name} max_concurrency must be at least 1")
        max_batch_size = getattr(source, "max_batch_size", None)
        if max_batch_size is not None and int(max_batch_size) < 1:
            raise ValueError(f"source {source.name} max_batch_size must be at least 1")
        declared_resilience = getattr(source, "resilience_policy", None)
        if declared_resilience is not None and not isinstance(
            declared_resilience, SourceResiliencePolicy
        ):
            raise TypeError(
                f"source {source.name} resilience_policy must be SourceResiliencePolicy or None"
            )
        kind = cls._source_kind(source)
        if kind == "single" and not callable(getattr(source, "fetch", None)):
            raise TypeError(
                f"source {source.name} must define fetch(), fetch_many(), "
                "or dependencies()+derive()"
            )
