"""
YAML writer for translated reconciliation specs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def write_yaml(payload: dict[str, Any], path: str | Path) -> None:
    """
    Write a translated ruleset payload to YAML.
    """
    Path(path).write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )


def to_yaml(payload: dict[str, Any]) -> str:
    """
    Convert a translated ruleset payload to YAML text.
    """
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=False)

