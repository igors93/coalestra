from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import replace
from typing import Any, TypeVar, cast

from coalestra.core.errors import PayloadIsolationError
from coalestra.core.models import SnapshotValue

T = TypeVar("T")
PayloadCopier = Callable[[Any], Any]


def deepcopy_payload(value: T) -> T:
    """Return a deep copy suitable for crossing a Coalestra ownership boundary."""

    return deepcopy(value)


class PayloadIsolator:
    """Create independent payload copies at source, cache, and snapshot boundaries."""

    def __init__(self, copier: PayloadCopier | None = None) -> None:
        self._copier = copier or deepcopy_payload

    def copy(self, value: T, *, context: str) -> T:
        """Copy one value and raise a stable Coalestra error when copying fails."""

        try:
            return cast(T, self._copier(value))
        except PayloadIsolationError:
            raise
        except Exception as error:
            raise PayloadIsolationError(
                context=context,
                value_type=type(value).__name__,
            ) from error

    def copy_metadata(
        self,
        metadata: Mapping[str, Any],
        *,
        context: str,
    ) -> dict[str, Any]:
        """Copy nested metadata and normalize it to a plain dictionary."""

        copied = self.copy(dict(metadata), context=context)
        if not isinstance(copied, Mapping):
            raise PayloadIsolationError(
                context=context,
                value_type=type(metadata).__name__,
            )
        return dict(copied)

    def clone_snapshot_value(
        self,
        value: SnapshotValue[Any],
        *,
        context: str,
        fetched_at: float | None = None,
        age_seconds: float | None = None,
        stale: bool | None = None,
        from_cache: bool | None = None,
        latency_ms: float | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotValue[Any]:
        """Clone a snapshot value while optionally replacing acquisition fields."""

        changes: dict[str, Any] = {
            "value": self.copy(value.value, context=f"{context} payload"),
            "metadata": self.copy_metadata(
                value.metadata if metadata is None else metadata,
                context=f"{context} metadata",
            ),
        }
        if fetched_at is not None:
            changes["fetched_at"] = fetched_at
        if age_seconds is not None:
            changes["age_seconds"] = age_seconds
        if stale is not None:
            changes["stale"] = stale
        if from_cache is not None:
            changes["from_cache"] = from_cache
        if latency_ms is not None:
            changes["latency_ms"] = latency_ms
        return replace(value, **changes)
