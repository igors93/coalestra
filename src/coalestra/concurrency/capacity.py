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
    """Cancellation-safe asynchronous capacity limiter with diagnostics.

    Counter updates do not await and therefore cannot be interrupted midway
    by task cancellation.

    The limiter is intended to be used from one event loop, matching the rest
    of Coalestra's asynchronous runtime model.
    """

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("capacity limit must be at least 1")

        self.limit = int(limit)
        self._semaphore = asyncio.Semaphore(self.limit)
        self._in_use = 0
        self._waiting = 0

    async def acquire(self) -> None:
        """Wait for and acquire one capacity slot."""

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
        """Release one previously acquired capacity slot."""

        if self._in_use <= 0:
            raise RuntimeError("capacity limiter released without a matching acquire")

        self._in_use -= 1
        self._semaphore.release()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """Acquire one slot and release it when the context exits."""

        await self.acquire()

        try:
            yield
        finally:
            self.release()

    async def snapshot(self) -> CapacitySnapshot:
        """Return the current limiter state."""

        return CapacitySnapshot(
            limit=self.limit,
            in_use=self._in_use,
            waiting=self._waiting,
        )


class CapacityLease:
    """Represents acquired capacity that must be released exactly once."""

    def __init__(
        self,
        limiters: tuple[CapacityLimiter, ...],
    ) -> None:
        self._limiters = limiters
        self._released = False

    @property
    def released(self) -> bool:
        """Return whether this lease has already been released."""

        return self._released

    def release(self) -> None:
        """Release every limiter in reverse acquisition order.

        Releasing in reverse order mirrors normal lock and resource management
        practices. The operation is idempotent, so calling release more than
        once does not release the underlying semaphores multiple times.
        """

        if self._released:
            return

        self._released = True

        for limiter in reversed(self._limiters):
            limiter.release()


class CapacityController:
    """Coordinate a builder-wide limit and optional per-source limits.

    Source capacity is acquired before global capacity. This prevents a busy,
    tightly limited source from occupying every global slot while its calls
    are still queued behind its own limit.
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
        """Return the configured limit for one source, when present."""

        return self._source_limits.get(self._normalize_source(source))

    def register_source(
        self,
        source: str,
        limit: int | None,
    ) -> None:
        """Register a source-level limit before serving work."""

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
        self._source_limiters.setdefault(
            normalized,
            CapacityLimiter(validated),
        )

    async def acquire(self, source: str) -> CapacityLease:
        """Acquire source and global capacity safely.

        When a source-specific limiter exists, it is acquired before the
        global limiter.

        If cancellation or another exception occurs after only some limiters
        have been acquired, every acquired limiter is released before the
        exception is propagated.
        """

        normalized = self._normalize_source(source)
        source_limiter = self._source_limiters.get(normalized)

        if source_limiter is None:
            limiters: tuple[CapacityLimiter, ...] = (self.global_limiter,)
        else:
            limiters = (
                source_limiter,
                self.global_limiter,
            )

        acquired: list[CapacityLimiter] = []

        try:
            for limiter in limiters:
                await limiter.acquire()
                acquired.append(limiter)
        except BaseException:
            for limiter in reversed(acquired):
                limiter.release()

            raise

        return CapacityLease(tuple(acquired))

    @asynccontextmanager
    async def slot(
        self,
        source: str,
    ) -> AsyncIterator[None]:
        """Acquire all required capacity and release it on context exit."""

        lease = await self.acquire(source)

        try:
            yield
        finally:
            lease.release()

    async def snapshot(
        self,
    ) -> dict[str, CapacitySnapshot]:
        """Return global and per-source capacity diagnostics."""

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
