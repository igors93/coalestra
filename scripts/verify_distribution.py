from __future__ import annotations

import argparse
import os
import subprocess
import tarfile
import tempfile
import venv
import zipfile
from email.parser import Parser
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10 in CI
    import tomli as tomllib


def _project_version(pyproject_path: Path) -> str:
    with pyproject_path.open("rb") as file:
        data: dict[str, Any] = tomllib.load(file)
    return str(data["project"]["version"])


def _metadata_version(content: str) -> str:
    message = Parser().parsestr(content)
    version = message.get("Version")
    if not version:
        raise ValueError("Distribution metadata does not contain Version")
    return version


def _wheel_version(wheel_path: Path) -> str:
    with zipfile.ZipFile(wheel_path) as archive:
        metadata_names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise ValueError(f"Expected one wheel METADATA file, found {len(metadata_names)}")
        return _metadata_version(archive.read(metadata_names[0]).decode("utf-8"))


def _sdist_version(sdist_path: Path) -> str:
    with tarfile.open(sdist_path, mode="r:gz") as archive:
        metadata_members = [
            member
            for member in archive.getmembers()
            if member.name.count("/") == 1 and member.name.endswith("/PKG-INFO")
        ]
        if len(metadata_members) != 1:
            raise ValueError(f"Expected one sdist PKG-INFO file, found {len(metadata_members)}")
        extracted = archive.extractfile(metadata_members[0])
        if extracted is None:
            raise ValueError("Unable to read sdist PKG-INFO")
        return _metadata_version(extracted.read().decode("utf-8"))


def _venv_python(environment: Path) -> Path:
    if os.name == "nt":
        return environment / "Scripts" / "python.exe"
    return environment / "bin" / "python"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify built Coalestra distributions.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("scripts/public_api.txt"),
    )
    args = parser.parse_args()

    root = args.root.resolve()
    dist = args.dist if args.dist.is_absolute() else root / args.dist
    manifest = args.manifest if args.manifest.is_absolute() else root / args.manifest
    version = _project_version(root / "pyproject.toml")

    wheels = tuple(dist.glob("coalestra-*.whl"))
    sdists = tuple(dist.glob("coalestra-*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        print(
            "ERROR: expected exactly one wheel and one source distribution, "
            f"found {len(wheels)} wheel(s) and {len(sdists)} sdist(s)"
        )
        return 1

    wheel = wheels[0]
    sdist = sdists[0]
    errors: list[str] = []
    if _wheel_version(wheel) != version:
        errors.append(f"Wheel metadata version does not match {version}")
    if _sdist_version(sdist) != version:
        errors.append(f"Source distribution metadata version does not match {version}")
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    with tempfile.TemporaryDirectory(prefix="coalestra-wheel-") as temporary:
        environment = Path(temporary) / "venv"
        venv.EnvBuilder(with_pip=True, clear=True).create(environment)
        python = _venv_python(environment)
        clean_environment = dict(os.environ)
        clean_environment.pop("PYTHONPATH", None)
        clean_environment.pop("PYTHONHOME", None)

        subprocess.run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-deps",
                str(wheel),
            ],
            check=True,
            cwd=temporary,
            env=clean_environment,
        )
        subprocess.run(
            [
                str(python),
                str(root / "scripts" / "check_public_api.py"),
                "--manifest",
                str(manifest),
                "--expected-version",
                version,
                "--forbid-path",
                str(root),
            ],
            check=True,
            cwd=temporary,
            env=clean_environment,
        )

    print(f"Distribution verified: {wheel.name}, {sdist.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
