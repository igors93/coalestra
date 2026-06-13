from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar, cast

T = TypeVar("T")
_ATTEMPTS_ATTRIBUTE = "__coalestra_attempts__"


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


def attempts_for(error: Exception, default: int = 1) -> int:
    """Return the exact number of attempts recorded by :func:`run_with_retry`."""

    raw = getattr(error, _ATTEMPTS_ATTRIBUTE, default)
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return max(1, int(default))


def _remember_attempts(error: Exception, attempts: int) -> None:
    try:
        setattr(error, _ATTEMPTS_ATTRIBUTE, attempts)
    except Exception:
        # A third-party exception may prohibit custom attributes. The caller still receives the
        # original exception and falls back to a conservative attempt count.
        return


async def run_with_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    policy: RetryPolicy,
    retryable: Callable[[Exception], bool],
    deadline_monotonic: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> tuple[T, int]:
    """Execute ``operation`` with bounded, deadline-aware retries.

    A backoff sleep is never started when it would consume the remaining deadline. The original
    exception is re-raised and annotated with the exact attempt count for diagnostic accuracy.
    """

    last_error: Exception | None = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return await operation(), attempt
        except Exception as error:
            last_error = error
            _remember_attempts(error, attempt)
            if attempt >= policy.max_attempts or not retryable(error):
                raise

            delay = policy.delay_for_attempt(attempt)
            if deadline_monotonic is not None:
                remaining = deadline_monotonic - monotonic()
                if remaining <= 0 or delay >= remaining:
                    raise
            if delay > 0:
                await sleep(delay)

    if last_error is not None:  # pragma: no cover - defensive guard
        raise last_error
    raise RuntimeError("retry loop completed without result")  # pragma: no cover
