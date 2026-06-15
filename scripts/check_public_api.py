from __future__ import annotations

import argparse
import importlib.metadata
from pathlib import Path

import coalestra


def _manifest_names(path: Path) -> tuple[str, ...]:
    names = tuple(
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    if names != tuple(sorted(names)):
        raise ValueError("Public API manifest must be sorted")
    if len(names) != len(set(names)):
        raise ValueError("Public API manifest contains duplicate names")
    return names


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the installed Coalestra public API.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).with_name("public_api.txt"),
    )
    parser.add_argument("--expected-version", default=None)
    parser.add_argument("--forbid-path", type=Path, default=None)
    args = parser.parse_args()

    expected = _manifest_names(args.manifest.resolve())
    exported = tuple(coalestra.__all__)
    exported_set = set(exported)

    errors: list[str] = []
    if len(exported) != len(exported_set):
        errors.append("coalestra.__all__ contains duplicate names")

    missing = sorted(set(expected) - exported_set)
    unexpected = sorted(exported_set - set(expected))
    if missing:
        errors.append(f"Missing public exports: {', '.join(missing)}")
    if unexpected:
        errors.append(f"Unexpected public exports: {', '.join(unexpected)}")

    absent_attributes = sorted(name for name in expected if not hasattr(coalestra, name))
    if absent_attributes:
        errors.append(f"Exports without module attributes: {', '.join(absent_attributes)}")

    distribution_version = importlib.metadata.version("coalestra")
    if coalestra.__version__ != distribution_version:
        errors.append(
            "coalestra.__version__ does not match installed distribution metadata: "
            f"{coalestra.__version__!r} != {distribution_version!r}"
        )
    if args.expected_version is not None and coalestra.__version__ != args.expected_version:
        errors.append(
            f"Installed version {coalestra.__version__!r} does not match "
            f"expected version {args.expected_version!r}"
        )

    module_path = Path(coalestra.__file__).resolve()
    if args.forbid_path is not None:
        forbidden = args.forbid_path.resolve()
        if module_path.is_relative_to(forbidden):
            errors.append(f"Coalestra was imported from forbidden source path: {module_path}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(
        f"Public API verified: {len(expected)} exports, version {coalestra.__version__}, "
        f"module {module_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
