"""Static contracts that keep executable notebooks and release docs aligned."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SYSTEM_NOTEBOOK = REPO_ROOT / "notebooks" / "rules_engine_system_tests.py"
SYSTEM_PLAN = REPO_ROOT / "docs" / "rules_engine_system_test_uat_plan.md"


def _test_ids(path: Path, prefix: str) -> list[int]:
    text = path.read_text(encoding="utf-8")
    return sorted({int(value) for value in re.findall(rf"\b{prefix}-(\d{{3}})\b", text)})


def test_system_notebook_and_plan_have_one_contiguous_test_inventory():
    """Named Databricks cases and the maintained plan cannot silently diverge."""
    expected = list(range(1, 68))

    assert _test_ids(SYSTEM_NOTEBOOK, "ST") == expected
    assert _test_ids(SYSTEM_PLAN, "ST") == expected
    assert _test_ids(SYSTEM_PLAN, "UAT") == list(range(1, 25))


def test_system_notebook_defines_fixtures_and_proves_wheel_provenance_first():
    """The bundle gate must be self-contained and test the installed artifact."""
    text = SYSTEM_NOTEBOOK.read_text(encoding="utf-8")
    first_named_test = text.index('print("ST-001:')
    preflight = text[:first_named_test]

    assert "RULESET_YAML_PATH =" in preflight
    assert "distribution(\"rules-engine\")" in preflight
    assert "package_file.relative_to(distribution_root)" in preflight
    assert "test/scratch/tmp" in preflight
    assert "sys.path.insert" not in text


def test_notebook_examples_do_not_shadow_the_installed_package_or_overwrite_by_default():
    """Shipped examples remain safe when copied into a Databricks workspace."""
    notebook_names = [
        "rules_engine_quickstart.py",
        "rules_engine_developer_guide.py",
        "python_ruleset_authoring_guide.py",
        "custom_function_authoring_guide.py",
        "production_yaml_publish_pipeline.py",
        "retire_ruleset_pipeline.py",
    ]
    notebook_text = {
        name: (REPO_ROOT / "notebooks" / name).read_text(encoding="utf-8")
        for name in notebook_names
    }

    assert all("sys.path.insert" not in text for text in notebook_text.values())
    assert all("repo_root = Path.cwd()" not in text for text in notebook_text.values())
    assert 'create_tables(mode="overwrite")' not in notebook_text[
        "rules_engine_quickstart.py"
    ]
    developer = notebook_text["rules_engine_developer_guide.py"]
    assert developer.index("RULES_ENGINE_GUIDE_RESET_CONFIRMATION") < developer.index(
        "DROP SCHEMA IF EXISTS"
    )


def test_system_notebook_rejects_assignment_type_conflicts_and_covers_governance():
    """The executable gate follows current strict typing and governance behavior."""
    text = SYSTEM_NOTEBOOK.read_text(encoding="utf-8")

    assert "falls back to string" not in text
    assert "SPARK_ASSIGNMENT_TYPE_CONFLICT" in text
    assert "Embedded expected cases are a hard publication gate" in text
    assert "Audit levels retain immutable execution identity" in text
    assert "Semantic diffs compare immutable published versions" in text
    assert "Coverage reports dead, broad, and closest rules" in text


def test_release_docs_report_the_current_system_gate_and_performance_schema():
    """Operator docs state the executable count and dynamic full-output contract."""
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    qmd = (REPO_ROOT / "README.qmd").read_text(encoding="utf-8")
    plan = SYSTEM_PLAN.read_text(encoding="utf-8")
    performance_notebook = (
        REPO_ROOT / "notebooks" / "rules_engine_serverless_performance.py"
    ).read_text(encoding="utf-8")

    assert "67 named system tests" in readme
    assert "67 named system tests" in qmd
    assert "67 system tests and 24 UAT tests" in plan
    assert 'column_name.startswith("rules_engine_")' in performance_notebook
