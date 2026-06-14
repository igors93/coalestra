import asyncio

import pytest

from coalestra import (
    CallableSource,
    ResourceKey,
    ResourceResolutionError,
    SnapshotBuilder,
    SnapshotBuildError,
    SourceFailure,
    SourceUnavailableError,
)


def test_snapshot_build_error_includes_nested_source_failure_details():
    key = ResourceKey("exchange", "info")

    resource_error = ResourceResolutionError(
        key,
        (
            SourceFailure(
                source="alphora-rest-exchange",
                error_type="SourceTimeoutError",
                message=("source alphora-rest-exchange timed out"),
                attempts=2,
            ),
        ),
    )

    error = SnapshotBuildError(
        {
            key: resource_error,
        }
    )

    rendered = str(error)

    assert "exchange:info=ResourceResolutionError" in rendered
    assert "alphora-rest-exchange" in rendered
    assert "SourceTimeoutError" in rendered
    assert "attempts=2" in rendered


def test_snapshot_build_error_exposes_structured_diagnostics():
    key = ResourceKey("exchange", "info")

    resource_error = ResourceResolutionError(
        key,
        (
            SourceFailure(
                source="rest",
                error_type="ConnectTimeout",
                message=("TLS handshake exceeded the connection budget"),
                attempts=2,
            ),
        ),
    )

    error = SnapshotBuildError(
        {
            key: resource_error,
        },
        snapshot=object(),
    )

    assert error.to_dict() == {
        "error_type": "SnapshotBuildError",
        "message": str(error),
        "has_partial_snapshot": True,
        "errors": [
            {
                "resource": "exchange:info",
                "error_type": "ResourceResolutionError",
                "message": str(resource_error),
                "failures": [
                    {
                        "source": "rest",
                        "error_type": "ConnectTimeout",
                        "message": ("TLS handshake exceeded the connection budget"),
                        "attempts": 2,
                    }
                ],
            }
        ],
    }


def test_snapshot_build_error_bounds_default_rendering():
    key = ResourceKey("exchange", "info")

    failures = tuple(
        SourceFailure(
            source=f"source-{index}",
            error_type="SourceUnavailableError",
            message="x" * 1_000,
            attempts=1,
        )
        for index in range(6)
    )

    error = SnapshotBuildError(
        {
            key: ResourceResolutionError(
                key,
                failures,
            ),
        }
    )

    rendered = str(error)

    assert "+3 more failure(s)" in rendered
    assert len(rendered) < 1_200

    structured = error.to_dict()

    assert len(structured["errors"][0]["failures"]) == 6


def test_builder_failure_keeps_source_details_available():
    key = ResourceKey("exchange", "info")

    def fail_source(_key, _context):
        raise SourceUnavailableError("synthetic connect timeout")

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="rest-exchange",
                priority=10,
                supports=lambda candidate: candidate == key,
                fetcher=fail_source,
            )
        ]
    )

    with pytest.raises(SnapshotBuildError) as captured:
        asyncio.run(
            builder.build(
                [key],
            )
        )

    diagnostic = captured.value.to_dict()

    assert diagnostic["errors"][0]["resource"] == ("exchange:info")

    assert diagnostic["errors"][0]["failures"][0] == {
        "source": "rest-exchange",
        "error_type": "SourceUnavailableError",
        "message": "synthetic connect timeout",
        "attempts": 2,
    }


def test_source_timeout_reports_effective_timeout_budget():
    key = ResourceKey("exchange", "info")

    async def slow_source(_key, _context):
        await asyncio.sleep(0.05)
        return {
            "symbols": [],
        }

    builder = SnapshotBuilder(
        [
            CallableSource(
                name="slow-rest",
                priority=10,
                supports=lambda candidate: candidate == key,
                fetcher=slow_source,
                timeout_seconds=0.01,
            )
        ]
    )

    with pytest.raises(SnapshotBuildError) as captured:
        asyncio.run(
            builder.build(
                [key],
            )
        )

    rendered = str(captured.value)

    assert "SourceTimeoutError" in rendered
    assert "timed out after 0.010s" in rendered
