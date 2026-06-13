from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ObservationPolicy:
    """Validation policy for source observation timestamps.

    ``future_tolerance_seconds`` absorbs small clock differences between systems. Values further
    in the future are rejected by default because they would otherwise remain artificially fresh.
    """

    future_tolerance_seconds: float = 1.0
    reject_future_observations: bool = True

    def __post_init__(self) -> None:
        if self.future_tolerance_seconds < 0:
            raise ValueError("future_tolerance_seconds cannot be negative")
