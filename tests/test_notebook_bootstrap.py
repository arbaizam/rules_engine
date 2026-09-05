"""Exercise notebook root discovery without importing Databricks or starting Spark."""

import ast
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = (
    "notebooks/99.rules_engine_system_tests.py",
    "examples/rules_engine_quickstart_guide.py",
    "examples/rules_engine_developer_guide.py",
    "examples/rules_engine_custom_function_authoring_guide.py",
)


@pytest.mark.parametrize("notebook", NOTEBOOKS)
def test_notebook_discovers_a_standalone_checkout(notebook, tmp_path, monkeypatch):
    """The shipped bootstrap works from a nested directory using tracked layout markers."""
    repository = tmp_path / "checkout"
    (repository / "src" / "rules_engine").mkdir(parents=True)
    (repository / "pyproject.toml").write_text("[project]\nname = 'rules-engine'\n")
    nested = repository / "notebooks" / "nested"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    tree = ast.parse((REPOSITORY_ROOT / notebook).read_text(encoding="utf-8"))
    root_assignment = next(
        node for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "root" for target in node.targets)
    )
    namespace = {"Path": Path}
    exec(compile(ast.Module(body=[root_assignment], type_ignores=[]), notebook, "exec"), namespace)

    assert namespace["root"] == repository
    assert not (repository / "databricks.yml").exists()
