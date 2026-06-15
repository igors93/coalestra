from __future__ import annotations

from dataclasses import dataclass
from math import isfinite


def require_finite_timestamp(value: float, *, name: str) -> float:
    """Return a timestamp as a finite float or raise ``ValueError``."""

    try:
        timestamp = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError(f"{name} must be a finite number") from error

    if not isfinite(timestamp):
        raise ValueError(f"{name} must be finite")

    return timestamp


@dataclass(frozen=True)
class ObservationPolicy:
    """Validation policy for source observation timestamps.

    ``future_tolerance_seconds`` absorbs small clock differences between systems. Values further
    in the future are rejected by default because they would otherwise remain artificially fresh.
    Observation timestamps must always be finite.
    """

    future_tolerance_seconds: float = 1.0
    reject_future_observations: bool = True

    def __post_init__(self) -> None:
        tolerance = require_finite_timestamp(
            self.future_tolerance_seconds,
            name="future_tolerance_seconds",
        )
        if tolerance < 0:
            raise ValueError("future_tolerance_seconds cannot be negative")
        object.__setattr__(self, "future_tolerance_seconds", tolerance)
