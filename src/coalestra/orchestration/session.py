from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from coalestra.core.errors import SessionClosedError, SnapshotBuildError
from coalestra.core.models import FetchContext, ResourceKey, Snapshot, SnapshotValue

if TYPE_CHECKING:
    from coalestra.orchestration.builder import SnapshotBuilder, _ResolutionRuntime


class SnapshotSession:
    """Incrementally builds one logically consistent snapshot.

    A session keeps one snapshot identity, creation time, deadline, and acquisition memo across
    every ``resolve`` call. Capacity is owned by the long-lived builder and shared by all sessions.
    Values resolved in an earlier stage are pinned for the remainder of the session, even if their
    normal cache TTL later expires.
    """

    def __init__(
        self,
        *,
        builder: SnapshotBuilder,
        context: FetchContext,
        runtime: _ResolutionRuntime,
    ) -> None:
        self._builder = builder
        self._context = context
        self._runtime = runtime
        self._resources: dict[ResourceKey, SnapshotValue[object]] = {}
        self._errors: dict[ResourceKey, Exception] = {}
        self._closed = False

    @property
    def snapshot_id(self) -> str:
        return self._context.snapshot_id

    @property
    def created_at(self) -> float:
        return self._context.requested_at

    @property
    def context(self) -> FetchContext:
        return self._context

    @property
    def closed(self) -> bool:
        return self._closed

    async def resolve(
        self,
        keys: Iterable[ResourceKey],
        *,
        strict: bool = True,
        retry_errors: bool = False,
    ) -> Snapshot:
        """Resolve another stage and return the complete session snapshot.

        Existing successful values are never fetched again. Existing errors are retained unless
        ``retry_errors=True`` is supplied, in which case the requested failed keys are attempted
        again while preserving the same session identity and deadline.
        """

        self._ensure_open()
        requested = tuple(dict.fromkeys(keys))
        if not requested:
            raise ValueError("at least one resource key is required")

        self._runtime.diagnostics.record_requested(requested)

        if retry_errors:
            for key in requested:
                self._errors.pop(key, None)

        pending = [
            key for key in requested if key not in self._resources and key not in self._errors
        ]
        if pending:
            values, errors = await self._builder._resolve_many(
                pending,
                context=self._context,
                runtime=self._runtime,
            )
            self._resources.update(values)
            self._errors.update(errors)

        requested_errors = {key: self._errors[key] for key in requested if key in self._errors}
        self._builder.metrics.increment(
            "snapshot_session_resolve_total",
            status="error" if requested_errors else "success",
        )
        self._builder.events.emit(
            "snapshot_session_resolved",
            snapshot_id=self.snapshot_id,
            requested=len(requested),
            resolved=sum(1 for key in requested if key in self._resources),
            failed=len(requested_errors),
            total_resources=len(self._resources),
            strict=strict,
            retry_errors=retry_errors,
        )

        if requested_errors and strict:
            raise SnapshotBuildError(requested_errors)
        return self.snapshot()

    def snapshot(self) -> Snapshot:
        """Return an immutable view of everything explicitly requested in this session."""

        diagnostics = self._runtime.diagnostics.snapshot(
            now_monotonic=self._builder.clock.monotonic(),
            resolved_resources=len(self._resources),
            failed_resources=len(self._errors),
            observed_at_values=tuple(value.observed_at for value in self._resources.values()),
        )
        return Snapshot(
            snapshot_id=self.snapshot_id,
            created_at=self.created_at,
            resources=self._resources,
            errors=self._errors,
            diagnostics=diagnostics,
        )

    async def close(self) -> None:
        self._closed = True

    async def __aenter__(self) -> SnapshotSession:
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> None:
        await self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise SessionClosedError("SnapshotSession is closed")
