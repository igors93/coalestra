from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import Any

from coalestra.core.models import CacheLookup, FreshnessPolicy, ResourceKey, SnapshotValue


class AsyncMemoryCache:
    """Concurrency-safe in-memory LRU cache with freshness-aware lookups."""

    def __init__(self, *, max_entries: int | None = None) -> None:
        if max_entries is not None and max_entries < 1:
            raise ValueError("max_entries must be at least 1 or None")
        self.max_entries = max_entries
        self._entries: OrderedDict[ResourceKey, SnapshotValue[Any]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(
        self,
        key: ResourceKey,
        *,
        now: float,
        policy: FreshnessPolicy,
    ) -> CacheLookup:
        async with self._lock:
            value = self._entries.get(key)
            if value is not None:
                self._entries.move_to_end(key)

        if value is None:
            return CacheLookup(value=None, fresh=False, usable_stale=False)

        age = max(0.0, now - value.observed_at)
        return CacheLookup(
            value=value,
            fresh=age <= policy.ttl_seconds,
            usable_stale=age <= policy.max_stale_seconds,
        )

    async def set(self, value: SnapshotValue[Any]) -> None:
        async with self._lock:
            self._entries[value.key] = value
            self._entries.move_to_end(value.key)
            if self.max_entries is not None:
                while len(self._entries) > self.max_entries:
                    self._entries.popitem(last=False)

    async def invalidate(self, key: ResourceKey) -> None:
        async with self._lock:
            self._entries.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._entries.clear()

    async def size(self) -> int:
        async with self._lock:
            return len(self._entries)
