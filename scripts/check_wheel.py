"""Verify built wheels contain exactly the current package sources and version."""

from __future__ import annotations

import argparse
import ast
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]


def check_wheel(path: Path) -> None:
    """Fail on stale/extra source files, a wrong version, or accidental non-package content."""
    source = ROOT / "src"
    expected_files = {
        file.relative_to(source).as_posix(): file.read_bytes()
        for file in (source / "rules_engine").rglob("*.py")
    }
    version_tree = ast.parse((source / "rules_engine" / "version.py").read_text(encoding="utf-8"))
    version = next(
        ast.literal_eval(node.value)
        for node in version_tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets)
    )
    with ZipFile(path) as wheel:
        names = {name for name in wheel.namelist() if not name.endswith("/")}
        actual_files = {name for name in names if name.startswith("rules_engine/")}
        if actual_files != set(expected_files):
            raise ValueError(
                f"Wheel source mismatch: missing={sorted(set(expected_files) - actual_files)}, "
                f"extra={sorted(actual_files - set(expected_files))}"
            )
        for name, content in expected_files.items():
            if wheel.read(name).replace(b"\r\n", b"\n") != content.replace(b"\r\n", b"\n"):
                raise ValueError(f"Wheel contains stale source: {name}")
        unexpected = names - actual_files
        if any(".dist-info/" not in name for name in unexpected):
            raise ValueError(f"Unexpected wheel content: {sorted(unexpected)}")
        metadata_files = [name for name in unexpected if name.endswith(".dist-info/METADATA")]
        if len(metadata_files) != 1:
            raise ValueError("Expected exactly one wheel metadata record.")
        metadata = Parser().parsestr(wheel.read(metadata_files[0]).decode("utf-8"))
        if metadata["Name"] != "rules-engine" or metadata["Version"] != version:
            raise ValueError("Wheel metadata does not match the source package name and version.")
    print(f"Verified {path.name}: {len(expected_files)} current source modules, version {version}.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Directory containing built wheels.")
    args = parser.parse_args()
    wheels = sorted(args.directory.glob("*.whl"))
    if not wheels:
        parser.error(f"No wheels found in {args.directory}")
    for wheel in wheels:
        check_wheel(wheel)


if __name__ == "__main__":
    main()
