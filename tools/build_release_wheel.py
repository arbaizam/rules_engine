"""Build one clean, version-matched rules engine wheel."""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = REPO_ROOT / "build"
DIST_DIR = REPO_ROOT / "dist"
EGG_INFO_DIR = REPO_ROOT / "rules_engine.egg-info"


def _project_version() -> str:
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', pyproject_text, flags=re.MULTILINE)
    if match is None:
        raise RuntimeError("Could not find [project] version in pyproject.toml.")
    return match.group(1)


def _remove_build_directory(path: Path) -> None:
    resolved = path.resolve()
    if resolved.parent != REPO_ROOT:
        raise RuntimeError(f"Refusing to remove build directory outside repository: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)


def _remove_old_wheels() -> None:
    DIST_DIR.mkdir(exist_ok=True)
    resolved_dist = DIST_DIR.resolve()
    for wheel_path in DIST_DIR.glob("rules_engine-*.whl"):
        resolved_wheel = wheel_path.resolve()
        if resolved_wheel.parent != resolved_dist:
            raise RuntimeError(f"Refusing to remove wheel outside dist: {resolved_wheel}")
        resolved_wheel.unlink()


def main() -> None:
    """Remove stale build outputs, build one wheel, and verify its identity."""
    version = _project_version()
    expected_wheel = DIST_DIR / f"rules_engine-{version}-py3-none-any.whl"

    _remove_build_directory(BUILD_DIR)
    _remove_build_directory(EGG_INFO_DIR)
    _remove_old_wheels()
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel"],
        cwd=REPO_ROOT,
        check=True,
    )

    built_wheels = sorted(DIST_DIR.glob("rules_engine-*.whl"))
    if built_wheels != [expected_wheel]:
        raise RuntimeError(
            f"Expected only {expected_wheel.name}, found "
            f"{[path.name for path in built_wheels]}."
        )
    digest = hashlib.sha256(expected_wheel.read_bytes()).hexdigest().upper()
    print(f"Built {expected_wheel.name} SHA256={digest}")


if __name__ == "__main__":
    main()
