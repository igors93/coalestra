from __future__ import annotations

import argparse
import inspect
import json
from pathlib import Path

import coalestra

_REQUIRED_FEATURES = (
    "automatic_observability_buffering",
    "blocking_source_timeout_guarantees",
    "builder_health_assessment",
    "builder_health_serialization",
    "controlled_payload_copy_shutdown",
    "snapshot_acceptance",
    "snapshot_consistency",
    "transactional_revalidation",
)

_REQUIRED_SCHEMAS = {
    "builder_health": 1,
    "builder_health_assessment": 1,
    "capabilities": 1,
    "error_diagnostics": 1,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the Coalestra 0.6 release contract.")
    parser.add_argument("--expected-version", default="0.6.0")
    parser.add_argument("--forbid-path", type=Path, default=None)
    args = parser.parse_args()

    if coalestra.__version__ != args.expected_version:
        raise SystemExit(
            f"ERROR: installed version {coalestra.__version__!r} does not match "
            f"{args.expected_version!r}"
        )

    if args.forbid_path is not None:
        installed_path = Path(inspect.getfile(coalestra)).resolve()
        forbidden = args.forbid_path.resolve()
        if installed_path.is_relative_to(forbidden):
            raise SystemExit(f"ERROR: imported Coalestra from forbidden source tree {forbidden}")

    installed = coalestra.require_capabilities(
        features=_REQUIRED_FEATURES,
        schemas=_REQUIRED_SCHEMAS,
    )
    payload = installed.to_dict()
    json.dumps(payload, allow_nan=False, sort_keys=True)

    if installed.stability != "beta":
        raise SystemExit(f"ERROR: unexpected stability classification {installed.stability!r}")
    if installed.defaults.get("require_source_timeout_declarations") is not True:
        raise SystemExit("ERROR: 0.6 must require source timeout declarations by default")
    if installed.defaults.get("allow_unsafe_blocking_sources") is not False:
        raise SystemExit("ERROR: 0.6 must reject unsafe blocking sources by default")

    builder_signature = inspect.signature(coalestra.SnapshotBuilder)
    if builder_signature.parameters["require_source_timeout_declarations"].default is not True:
        raise SystemExit("ERROR: SnapshotBuilder strict timeout declarations are not the default")
    if builder_signature.parameters["allow_unsafe_blocking_sources"].default is not False:
        raise SystemExit("ERROR: SnapshotBuilder allows unsafe blocking sources by default")

    print(
        "Release contract verified: "
        f"version={installed.package_version}, features={len(installed.features)}, "
        f"schemas={len(installed.schemas)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
