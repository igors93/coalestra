from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass


@dataclass(frozen=True)
class CapacitySnapshot:
    """Current state of one concurrency limiter."""

    limit: int
    in_use: int
    waiting: int


class CapacityLimiter:
    """Cancellation-safe asynchronous capacity limiter with lightweight diagnostics.

    Counter updates do not await and therefore cannot be interrupted midway by task cancellation.
    The limiter is intended to be used from one event loop, matching the rest of Coalestra's
    asynchronous runtime model.
    """

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("capacity limit must be at least 1")
        self.limit = int(limit)
        self._semaphore = asyncio.Semaphore(self.limit)
        self._in_use = 0
        self._waiting = 0

    async def acquire(self) -> None:
        self._waiting += 1
        acquired = False
        try:
            await self._semaphore.acquire()
            acquired = True
        finally:
            self._waiting -= 1
            if acquired:
                self._in_use += 1

    def release(self) -> None:
        if self._in_use <= 0:
            raise RuntimeError("capacity limiter released without a matching acquire")
        self._in_use -= 1
        self._semaphore.release()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        await self.acquire()
        try:
            yield
        finally:
            self.release()

    async def snapshot(self) -> CapacitySnapshot:
        return CapacitySnapshot(
            limit=self.limit,
            in_use=self._in_use,
            waiting=self._waiting,
        )


class CapacityController:
    """Coordinates one builder-wide limit and optional limits for individual sources.

    Source capacity is acquired before global capacity. This prevents a busy, tightly limited
    source from occupying every global slot while its calls are still queued behind its own limit.
    """

    def __init__(
        self,
        *,
        global_limit: int,
        source_limits: Mapping[str, int] | None = None,
    ) -> None:
        self.global_limiter = CapacityLimiter(global_limit)
        self._source_limits = {
            self._normalize_source(name): self._validate_limit(limit)
            for name, limit in (source_limits or {}).items()
        }
        self._source_limiters: dict[str, CapacityLimiter] = {
            name: CapacityLimiter(limit) for name, limit in self._source_limits.items()
        }

    def limit_for(self, source: str) -> int | None:
        return self._source_limits.get(self._normalize_source(source))

    def register_source(self, source: str, limit: int | None) -> None:
        """Register a source-level limit before the controller starts serving work."""

        if limit is None:
            return
        normalized = self._normalize_source(source)
        validated = self._validate_limit(limit)
        existing = self._source_limits.get(normalized)
        if existing is not None and existing != validated:
            raise ValueError(
                f"conflicting concurrency limits for source {source}: {existing} and {validated}"
            )
        self._source_limits[normalized] = validated
        self._source_limiters.setdefault(normalized, CapacityLimiter(validated))

    @asynccontextmanager
    async def slot(self, source: str) -> AsyncIterator[None]:
        normalized = self._normalize_source(source)
        source_limiter = self._source_limiters.get(normalized)
        if source_limiter is None:
            async with self.global_limiter.slot():
                yield
            return

        async with source_limiter.slot(), self.global_limiter.slot():
            yield

    async def snapshot(self) -> dict[str, CapacitySnapshot]:
        result = {"__global__": await self.global_limiter.snapshot()}
        for source, limiter in self._source_limiters.items():
            result[source] = await limiter.snapshot()
        return result

    @staticmethod
    def _normalize_source(source: str) -> str:
        normalized = str(source or "").strip()
        if not normalized:
            raise ValueError("source name cannot be empty")
        return normalized

    @staticmethod
    def _validate_limit(limit: int) -> int:
        value = int(limit)
        if value < 1:
            raise ValueError("source concurrency limit must be at least 1")
        return value
