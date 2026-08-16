import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def _project_version() -> str:
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', pyproject_text, flags=re.MULTILINE)
    if match is None:
        raise AssertionError("Could not find [project] version in pyproject.toml.")
    return match.group(1)


def _bundle_config() -> dict:
    return yaml.safe_load((REPO_ROOT / "databricks.yml").read_text(encoding="utf-8"))


def test_bundle_artifact_and_job_use_the_project_version():
    """The deployed wheel cannot silently drift from package metadata."""
    version = _project_version()
    expected_wheel = f"./dist/rules_engine-{version}-py3-none-any.whl"
    config = _bundle_config()
    artifact = config["artifacts"]["rules_engine"]
    task = config["resources"]["jobs"]["rules_engine_system_tests"]["tasks"][0]

    assert artifact["files"] == [{"source": expected_wheel}]
    assert artifact["build"] == "python tools/build_release_wheel.py"
    assert {library.get("whl") for library in task["libraries"]} >= {expected_wheel}


def test_bundle_system_test_job_is_parameterized_and_non_retrying():
    """The release gate targets an explicit cluster and disposable schema."""
    config = _bundle_config()
    job = config["resources"]["jobs"]["rules_engine_system_tests"]
    task = job["tasks"][0]
    parameter_defaults = {
        item["name"]: item["default"]
        for item in job["parameters"]
    }

    assert task["existing_cluster_id"] == "${var.existing_cluster_id}"
    assert task["max_retries"] == 0
    assert task["notebook_task"] == {
        "notebook_path": "./notebooks/rules_engine_system_tests.py",
        "source": "WORKSPACE",
    }
    assert parameter_defaults == {
        "SCHEMA": "${var.system_test_schema}",
        "RULES_ENGINE_RUN_SPARK_TESTS": "1",
    }
