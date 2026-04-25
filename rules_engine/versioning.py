"""
Version comparison helpers for production ruleset cutover checks.
"""

from __future__ import annotations

import re


def parse_numeric_version(version: str) -> tuple[int, ...]:
    """
    Parse numeric dot-notation versions such as ``1.0.0``.
    """
    if not re.fullmatch(r"\d+(\.\d+)*", version):
        raise ValueError(
            f"Version must be numeric dot notation for automatic retirement: {version}"
        )
    return tuple(int(part) for part in version.split("."))


def compare_versions(left: str, right: str) -> int:
    """
    Compare numeric dot-notation versions.
    """
    left_parts = parse_numeric_version(left)
    right_parts = parse_numeric_version(right)
    max_len = max(len(left_parts), len(right_parts))
    padded_left = left_parts + (0,) * (max_len - len(left_parts))
    padded_right = right_parts + (0,) * (max_len - len(right_parts))
    if padded_left > padded_right:
        return 1
    if padded_left < padded_right:
        return -1
    return 0
