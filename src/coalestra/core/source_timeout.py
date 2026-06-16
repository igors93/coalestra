from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any


class SourceTimeoutGuaranteeStatus(str, Enum):
    """Stable classifications for synchronous blocking-source timeout safety."""

    UNDECLARED = "undeclared"
    NON_BLOCKING = "non_blocking"
    PROTECTED = "protected"
    UNSAFE_NOT_OFFLOADED = "unsafe_not_offloaded"
    UNSAFE_TIMEOUT_MISSING = "unsafe_timeout_missing"
    UNSAFE_TIMEOUT_NOT_WITHIN_SOURCE = "unsafe_timeout_not_within_source"


@dataclass(frozen=True)
class SourceTimeoutGuarantee:
    """Immutable timeout-safety declaration for one source.

    The declaration is contractual. Coalestra validates the relationship between the
    declared transport timeout and its own source/deadline budgets, but it cannot inspect
    a third-party client to prove that the client actually applies the declared timeout.
    """

    source: str
    status: SourceTimeoutGuaranteeStatus
    declaration_present: bool
    blocking_io: bool
    blocking_io_offloaded: bool
    transport_timeout_seconds: float | None
    source_timeout_seconds: float | None

    @property
    def safe(self) -> bool:
        """Whether this source is safe under its declared execution contract."""

        return self.status in {
            SourceTimeoutGuaranteeStatus.NON_BLOCKING,
            SourceTimeoutGuaranteeStatus.PROTECTED,
        }

    @property
    def protected(self) -> bool:
        """Whether blocking I/O has a bounded transport timeout and thread offload."""

        return self.status is SourceTimeoutGuaranteeStatus.PROTECTED


def inspect_source_timeout_guarantee(source: Any) -> SourceTimeoutGuarantee:
    """Return and validate the timeout-safety declaration exposed by one source."""

    name = str(getattr(source, "name", type(source).__name__))
    declaration_present = hasattr(source, "blocking_io")
    source_timeout = _optional_positive_number(
        getattr(source, "timeout_seconds", None),
        field_name=f"source {name} timeout_seconds",
    )

    if not declaration_present:
        return SourceTimeoutGuarantee(
            source=name,
            status=SourceTimeoutGuaranteeStatus.UNDECLARED,
            declaration_present=False,
            blocking_io=False,
            blocking_io_offloaded=False,
            transport_timeout_seconds=None,
            source_timeout_seconds=source_timeout,
        )

    blocking_io = source.blocking_io
    if not isinstance(blocking_io, bool):
        raise TypeError(f"source {name} blocking_io must be a boolean")

    offloaded_raw = getattr(source, "blocking_io_offloaded", False)
    if not isinstance(offloaded_raw, bool):
        raise TypeError(f"source {name} blocking_io_offloaded must be a boolean")
    offloaded = bool(offloaded_raw)

    transport_timeout = _optional_positive_number(
        getattr(source, "transport_timeout_seconds", None),
        field_name=f"source {name} transport_timeout_seconds",
    )

    if not blocking_io:
        if transport_timeout is not None:
            raise ValueError(f"source {name} transport_timeout_seconds requires blocking_io=True")
        return SourceTimeoutGuarantee(
            source=name,
            status=SourceTimeoutGuaranteeStatus.NON_BLOCKING,
            declaration_present=True,
            blocking_io=False,
            blocking_io_offloaded=offloaded,
            transport_timeout_seconds=None,
            source_timeout_seconds=source_timeout,
        )

    if not offloaded:
        status = SourceTimeoutGuaranteeStatus.UNSAFE_NOT_OFFLOADED
    elif transport_timeout is None:
        status = SourceTimeoutGuaranteeStatus.UNSAFE_TIMEOUT_MISSING
    elif source_timeout is not None and transport_timeout >= source_timeout:
        status = SourceTimeoutGuaranteeStatus.UNSAFE_TIMEOUT_NOT_WITHIN_SOURCE
    else:
        status = SourceTimeoutGuaranteeStatus.PROTECTED

    return SourceTimeoutGuarantee(
        source=name,
        status=status,
        declaration_present=True,
        blocking_io=True,
        blocking_io_offloaded=offloaded,
        transport_timeout_seconds=transport_timeout,
        source_timeout_seconds=source_timeout,
    )


def _optional_positive_number(value: Any, *, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number or None")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return normalized
