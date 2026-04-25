import pytest

from rules_engine.versioning import compare_versions, parse_numeric_version


def test_compare_versions_orders_numeric_dot_versions():
    """
    What: Compares numeric dot-notation versions using integer segments.
    Why: Production auto-retire must not publish older YAML over a newer version.
    Fails when: Version ordering falls back to lexicographic string comparison.
    """
    assert compare_versions("2.1.0", "1.0.0") == 1
    assert compare_versions("1.10.0", "1.2.0") == 1
    assert compare_versions("1.0.0", "2.1.0") == -1
    assert compare_versions("1.0", "1.0.0") == 0


def test_parse_numeric_version_rejects_tags_and_dates():
    """
    What: Rejects non-numeric versions for automatic retirement.
    Why: The production cutover guard intentionally avoids tag/date semantics.
    Fails when: Ambiguous versions can bypass numeric version ordering.
    """
    for version in ["v1.0.0", "2024-Q4", "pilot", "1.0.0-beta"]:
        with pytest.raises(ValueError, match="numeric dot notation"):
            parse_numeric_version(version)
