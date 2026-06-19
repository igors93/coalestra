from __future__ import annotations

import asyncio

import pytest

from coalestra import (
    CallableSource,
    RequestDegradationPolicy,
    ResourceKey,
    SnapshotBuilder,
    SnapshotBuildError,
    SnapshotRequest,
    SnapshotResult,
    SnapshotResultState,
    SnapshotSession,
    SyncSnapshotBuilder,
    try_build_request,
    try_build_requests,
    try_resolve_request,
)
from coalestra.resilience import RetryPolicy

KEY_A = ResourceKey("op", "resource", "A")
KEY_B = ResourceKey("op", "resource", "B")
KEY_C = ResourceKey("op", "resource", "C")


def run(coro):
    return asyncio.run(coro)


def make_builder(*sources, **kwargs):
    return SnapshotBuilder(list(sources), retry_policy=RetryPolicy(max_attempts=1), **kwargs)


def always(value):
    async def fetch(_key, _ctx):
        return value

    return CallableSource(name="ok", priority=1, supports=lambda _: True, fetcher=fetch)


def always_fail(exc):
    async def fetch(_key, _ctx):
        raise exc

    return CallableSource(name="fail", priority=1, supports=lambda _: True, fetcher=fetch)


# ---------------------------------------------------------------------------
# RequestDegradationPolicy -construction and validation
# ---------------------------------------------------------------------------


def test_policy_defaults():
    p = RequestDegradationPolicy()
    assert p.minimum_resolved_resources == 1
    assert p.minimum_resolved_required_resources == 0
    assert not p.fail_closed
    assert tuple(p.accepted_actions) == ("read", "evaluate", "act")
    assert tuple(p.degraded_actions) == ("read", "evaluate")
    assert tuple(p.rejected_actions) == ()


def test_policy_rejects_negative_minimum():
    with pytest.raises(ValueError):
        RequestDegradationPolicy(minimum_resolved_resources=-1)


def test_policy_rejects_bool_minimum():
    with pytest.raises(TypeError):
        RequestDegradationPolicy(minimum_resolved_resources=True)


def test_policy_rejects_empty_action_name():
    with pytest.raises(ValueError):
        RequestDegradationPolicy(accepted_actions=["read", ""])


def test_policy_deduplicates_actions():
    p = RequestDegradationPolicy(accepted_actions=["read", "read", "act"])
    assert tuple(p.accepted_actions) == ("read", "act")


def test_policy_metadata_is_immutable():
    p = RequestDegradationPolicy(metadata={"k": "v"})
    assert p.metadata["k"] == "v"
    with pytest.raises(TypeError):
        p.metadata["k"] = "x"  # type: ignore[index]


def test_fail_closed_policy_factory():
    p = RequestDegradationPolicy.fail_closed_policy()
    assert p.fail_closed
    assert p.reject_on_required_error
    assert p.reject_on_acceptance_violation
    assert p.reject_on_consistency_violation
    assert tuple(p.degraded_actions) == ()


def test_read_only_degraded_factory():
    p = RequestDegradationPolicy.read_only_degraded()
    assert not p.fail_closed
    assert "act" not in p.accepted_actions


def test_risk_reduction_factory():
    p = RequestDegradationPolicy.risk_reduction()
    assert "cancel_orders" in p.accepted_actions
    assert "cancel_orders" in p.degraded_actions
    assert not p.reject_on_consistency_violation


# ---------------------------------------------------------------------------
# SnapshotResult -properties and helpers
# ---------------------------------------------------------------------------


def _make_snapshot(resources=None, errors=None):
    from coalestra.core.models import Snapshot

    return Snapshot(
        snapshot_id="test",
        created_at=0.0,
        resources=resources or {},
        errors=errors or {},
    )


def _make_result(state: SnapshotResultState, **kwargs) -> SnapshotResult:
    request = SnapshotRequest(required=[KEY_A])
    return SnapshotResult(
        state=state,
        snapshot=_make_snapshot(resources={KEY_A: object()}),
        request=request,
        **kwargs,
    )


def test_snapshot_result_accepted_properties():
    r = _make_result(SnapshotResultState.ACCEPTED)
    assert r.accepted
    assert not r.degraded
    assert not r.rejected


def test_snapshot_result_degraded_properties():
    r = _make_result(SnapshotResultState.DEGRADED)
    assert not r.accepted
    assert r.degraded
    assert not r.rejected


def test_snapshot_result_rejected_properties():
    r = _make_result(SnapshotResultState.REJECTED)
    assert not r.accepted
    assert not r.degraded
    assert r.rejected


def test_raise_for_rejected_raises_on_rejected():
    r = _make_result(SnapshotResultState.REJECTED, error=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        r.raise_for_rejected()


def test_raise_for_rejected_noop_on_accepted():
    r = _make_result(SnapshotResultState.ACCEPTED)
    r.raise_for_rejected()  # must not raise


def test_require_accepted_returns_snapshot():
    r = _make_result(SnapshotResultState.ACCEPTED)
    assert r.require_accepted() is r.snapshot


def test_require_accepted_raises_on_degraded():
    err = SnapshotBuildError({KEY_A: RuntimeError("x")}, snapshot=_make_snapshot())
    r = _make_result(SnapshotResultState.DEGRADED, error=err)
    with pytest.raises(SnapshotBuildError):
        r.require_accepted()


def test_snapshot_result_string_state_coercion():
    r = SnapshotResult(
        state="accepted",  # type: ignore[arg-type]
        snapshot=_make_snapshot(),
        request=SnapshotRequest(required=[KEY_A]),
    )
    assert r.state is SnapshotResultState.ACCEPTED


def test_to_dict_contains_expected_keys():
    r = _make_result(SnapshotResultState.ACCEPTED, allowed_actions=("read",), reason="accepted")
    d = r.to_dict()
    assert d["state"] == "accepted"
    assert d["allowed_actions"] == ["read"]
    assert d["reason"] == "accepted"
    assert "acceptance_violations" in d
    assert "consistency_violation" in d


# ---------------------------------------------------------------------------
# try_resolve_request -happy path, degraded, rejected
# ---------------------------------------------------------------------------


def test_try_resolve_request_accepted_when_all_resolve():
    async def scenario():
        builder = make_builder(always(42))
        session = builder.session()
        try:
            request = SnapshotRequest(required=[KEY_A])
            result = await try_resolve_request(session, request)
            assert result.accepted
            assert result.snapshot.value(KEY_A, int) == 42
            assert "read" in result.allowed_actions
        finally:
            await session.close()
            await builder.aclose()

    run(scenario())


def test_try_resolve_request_degraded_on_optional_failure():
    from coalestra import SourceUnavailableError

    async def fetch(key, _ctx):
        if key == KEY_B:
            raise SourceUnavailableError("missing B")
        return 1

    async def scenario():
        source = CallableSource(name="s", priority=1, supports=lambda _: True, fetcher=fetch)
        builder = make_builder(source)
        session = builder.session()
        try:
            request = SnapshotRequest(required=[KEY_A], optional=[KEY_B])
            result = await try_resolve_request(session, request)
            assert result.degraded
            assert KEY_B in result.optional_errors
            assert "read" in result.allowed_actions
        finally:
            await session.close()
            await builder.aclose()

    run(scenario())


def test_try_resolve_request_rejected_when_no_resources_resolved():
    from coalestra import SourceUnavailableError

    async def scenario():
        source = always_fail(SourceUnavailableError("always fails"))
        builder = make_builder(source)
        session = builder.session()
        try:
            request = SnapshotRequest(required=[KEY_A])
            result = await try_resolve_request(session, request)
            assert result.rejected
            assert tuple(result.allowed_actions) == ()
        finally:
            await session.close()
            await builder.aclose()

    run(scenario())


def test_try_resolve_request_rejected_with_fail_closed_policy():
    from coalestra import SourceUnavailableError

    async def fetch(key, _ctx):
        if key == KEY_B:
            raise SourceUnavailableError("missing B")
        return 1

    async def scenario():
        source = CallableSource(name="s", priority=1, supports=lambda _: True, fetcher=fetch)
        builder = make_builder(source)
        session = builder.session()
        try:
            request = SnapshotRequest(required=[KEY_A], optional=[KEY_B])
            result = await try_resolve_request(
                session,
                request,
                degradation_policy=RequestDegradationPolicy.fail_closed_policy(),
            )
            assert result.rejected
        finally:
            await session.close()
            await builder.aclose()

    run(scenario())


def test_try_resolve_request_rejected_on_required_error_with_policy():
    from coalestra import SourceUnavailableError

    async def scenario():
        source = always_fail(SourceUnavailableError("always fails"))
        builder = make_builder(source)
        session = builder.session()
        try:
            request = SnapshotRequest(required=[KEY_A])
            policy = RequestDegradationPolicy(reject_on_required_error=True)
            result = await try_resolve_request(session, request, degradation_policy=policy)
            assert result.rejected
            assert KEY_A in result.required_errors
        finally:
            await session.close()
            await builder.aclose()

    run(scenario())


# ---------------------------------------------------------------------------
# try_build_request
# ---------------------------------------------------------------------------


def test_try_build_request_accepted():
    async def scenario():
        builder = make_builder(always("hello"))
        request = SnapshotRequest(required=[KEY_A])
        result = await try_build_request(builder, request)
        assert result.accepted
        assert result.snapshot.value(KEY_A, str) == "hello"

    run(scenario())


def test_try_build_request_rejected_on_build_error():
    from coalestra import SourceUnavailableError

    async def scenario():
        builder = make_builder(always_fail(SourceUnavailableError("x")))
        request = SnapshotRequest(required=[KEY_A])
        result = await try_build_request(builder, request)
        assert result.rejected

    run(scenario())


def test_try_build_request_does_not_raise():
    from coalestra import SourceUnavailableError

    async def scenario():
        builder = make_builder(always_fail(SourceUnavailableError("x")))
        request = SnapshotRequest(required=[KEY_A])
        result = await try_build_request(builder, request)
        assert isinstance(result, SnapshotResult)

    run(scenario())


# ---------------------------------------------------------------------------
# try_build_requests -fanout with independent per-request degradation
# ---------------------------------------------------------------------------


def test_try_build_requests_returns_tuple_in_input_order():
    async def fetch(key, _ctx):
        return key.subject

    async def scenario():
        source = CallableSource(name="s", priority=1, supports=lambda _: True, fetcher=fetch)
        builder = make_builder(source)
        requests = [
            SnapshotRequest(required=[KEY_A]),
            SnapshotRequest(required=[KEY_B]),
            SnapshotRequest(required=[KEY_C]),
        ]
        results = await try_build_requests(builder, requests)
        assert len(results) == 3
        assert results[0].snapshot.value(KEY_A, str) == KEY_A.subject
        assert results[1].snapshot.value(KEY_B, str) == KEY_B.subject
        assert results[2].snapshot.value(KEY_C, str) == KEY_C.subject

    run(scenario())


def test_try_build_requests_empty_input_returns_empty_tuple():
    async def scenario():
        builder = make_builder(always(1))
        results = await try_build_requests(builder, [])
        assert results == ()

    run(scenario())


def test_try_build_requests_rejects_invalid_max_concurrent():
    async def scenario():
        builder = make_builder(always(1))
        with pytest.raises(ValueError):
            await try_build_requests(builder, [SnapshotRequest(required=[KEY_A])], max_concurrent=0)

    run(scenario())


def test_try_build_requests_rejects_bool_max_concurrent():
    async def scenario():
        builder = make_builder(always(1))
        with pytest.raises(TypeError):
            await try_build_requests(
                builder, [SnapshotRequest(required=[KEY_A])], max_concurrent=True
            )

    run(scenario())


def test_try_build_requests_one_failure_does_not_abort_others():
    from coalestra import SourceUnavailableError

    async def fetch(key, _ctx):
        if key == KEY_B:
            raise SourceUnavailableError("missing B")
        return "ok"

    async def scenario():
        source = CallableSource(name="s", priority=1, supports=lambda _: True, fetcher=fetch)
        builder = make_builder(source)
        requests = [SnapshotRequest(required=[KEY_A]), SnapshotRequest(required=[KEY_B])]
        results = await try_build_requests(builder, requests)
        assert results[0].accepted
        assert results[1].rejected

    run(scenario())


# ---------------------------------------------------------------------------
# install_operational_methods -idempotency
# ---------------------------------------------------------------------------


def test_install_operational_methods_is_idempotent():
    from coalestra import install_operational_methods

    install_operational_methods()
    install_operational_methods()

    assert hasattr(SnapshotSession, "try_resolve_request")
    assert hasattr(SnapshotBuilder, "try_build_request")
    assert hasattr(SnapshotBuilder, "try_build_requests")


# ---------------------------------------------------------------------------
# Sync wrappers installed by install_operational_methods
# ---------------------------------------------------------------------------


def test_sync_builder_try_build_request_accepted():
    builder = make_builder(always("sync"))
    sync = SyncSnapshotBuilder(builder)
    try:
        request = SnapshotRequest(required=[KEY_A])
        result = sync.try_build_request(request)
        assert result.accepted
        assert result.snapshot.value(KEY_A, str) == "sync"
    finally:
        sync.close()


def test_sync_builder_try_build_requests_returns_tuple():
    async def fetch(key, _ctx):
        return key.subject

    source = CallableSource(name="s", priority=1, supports=lambda _: True, fetcher=fetch)
    builder = make_builder(source)
    sync = SyncSnapshotBuilder(builder)
    try:
        requests = [SnapshotRequest(required=[KEY_A]), SnapshotRequest(required=[KEY_B])]
        results = sync.try_build_requests(requests)
        assert len(results) == 2
        assert results[0].accepted
        assert results[1].accepted
    finally:
        sync.close()


def test_sync_session_try_resolve_request_accepted():
    builder = make_builder(always(7))
    sync = SyncSnapshotBuilder(builder)
    try:
        session = sync.session()
        try:
            request = SnapshotRequest(required=[KEY_A])
            result = session.try_resolve_request(request)
            assert result.accepted
            assert result.snapshot.value(KEY_A, int) == 7
        finally:
            session.close()
    finally:
        sync.close()
