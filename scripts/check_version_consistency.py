from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10 in CI
    import tomli as tomllib

_VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+(?:[a-zA-Z0-9.-]+)?$")
_CHANGELOG_VERSION_PATTERN = re.compile(r"^## (?P<version>\d+\.\d+\.\d+) - \d{4}-\d{2}-\d{2}$")


def _project_version(pyproject_path: Path) -> str:
    with pyproject_path.open("rb") as file:
        data: dict[str, Any] = tomllib.load(file)
    version = str(data["project"]["version"])
    if not _VERSION_PATTERN.fullmatch(version):
        raise ValueError(f"Invalid project version: {version!r}")
    return version


def _package_version(init_path: Path) -> str:
    module = ast.parse(init_path.read_text(encoding="utf-8"), filename=str(init_path))
    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in statement.targets
        ):
            continue
        if not isinstance(statement.value, ast.Constant) or not isinstance(
            statement.value.value, str
        ):
            raise ValueError("coalestra.__version__ must be a string literal")
        return statement.value.value
    raise ValueError("coalestra.__version__ was not found")


def _changelog_versions(changelog_path: Path) -> tuple[str, ...]:
    versions: list[str] = []
    for line in changelog_path.read_text(encoding="utf-8").splitlines():
        match = _CHANGELOG_VERSION_PATTERN.fullmatch(line.strip())
        if match is not None:
            versions.append(match.group("version"))
    return tuple(versions)


def _lock_version(lock_path: Path) -> str | None:
    if not lock_path.exists():
        return None
    with lock_path.open("rb") as file:
        data: dict[str, Any] = tomllib.load(file)
    for package in data.get("package", []):
        if package.get("name") == "coalestra":
            return str(package.get("version"))
    return None


def _normalize_tag(tag: str) -> str:
    normalized = tag.strip()
    if normalized.startswith("refs/tags/"):
        normalized = normalized.removeprefix("refs/tags/")
    if normalized.startswith("v"):
        normalized = normalized[1:]
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Coalestra release version consistency.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--tag", default=None)
    parser.add_argument("--expected", default=None)
    args = parser.parse_args()

    root = args.root.resolve()
    project_version = _project_version(root / "pyproject.toml")
    package_version = _package_version(root / "src" / "coalestra" / "__init__.py")
    changelog_versions = _changelog_versions(root / "CHANGELOG.md")
    lock_version = _lock_version(root / "uv.lock")

    errors: list[str] = []
    if package_version != project_version:
        errors.append(
            "Package version "
            f"{package_version!r} does not match pyproject version {project_version!r}"
        )
    if lock_version is not None and lock_version != project_version:
        errors.append(
            f"uv.lock version {lock_version!r} does not match project version {project_version!r}"
        )
    if project_version not in changelog_versions:
        errors.append(f"CHANGELOG.md does not contain a dated section for {project_version}")
    if changelog_versions.count(project_version) != 1:
        errors.append(f"CHANGELOG.md must contain exactly one dated section for {project_version}")

    if args.expected is not None and project_version != args.expected:
        errors.append(
            f"Project version {project_version!r} does not match expected version {args.expected!r}"
        )

    if args.tag is not None:
        tag_version = _normalize_tag(args.tag)
        if tag_version != project_version:
            errors.append(
                f"Tag version {tag_version!r} does not match project version {project_version!r}"
            )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(f"Version consistency verified: {project_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
