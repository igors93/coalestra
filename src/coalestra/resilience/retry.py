from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar, cast

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 2
    base_delay_seconds: float = 0.05
    max_delay_seconds: float = 1.0
    jitter_ratio: float = 0.1

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds < 0 or self.max_delay_seconds < 0:
            raise ValueError("retry delays cannot be negative")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds cannot be smaller than base_delay_seconds")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")

    def delay_for_attempt(self, attempt: int) -> float:
        base = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** max(0, attempt - 1)),
        )
        if base == 0 or self.jitter_ratio == 0:
            return cast(float, base)
        jitter = base * self.jitter_ratio
        return cast(float, max(0.0, base + random.uniform(-jitter, jitter)))


async def run_with_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    retryable: Callable[[Exception], bool],
) -> tuple[T, int]:
    last_error: Exception | None = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await operation(), attempt
        except Exception as error:
            last_error = error
            if attempt >= policy.max_attempts or not retryable(error):
                raise
            await asyncio.sleep(policy.delay_for_attempt(attempt))

    if last_error is not None:  # pragma: no cover - defensive guard
        raise last_error
    raise RuntimeError("retry loop completed without result")  # pragma: no cover
