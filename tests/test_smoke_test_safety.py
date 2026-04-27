import importlib.util
from pathlib import Path

import pytest


def _load_smoke_module():
    module_path = Path(__file__).resolve().parents[1] / "databricks" / "smoke_test_rules_engine.py"
    spec = importlib.util.spec_from_file_location("smoke_test_rules_engine", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_smoke_test_allows_disposable_targets():
    """
    What: Allows smoke-test table targets that are visibly disposable.
    Why: The smoke test overwrites metadata tables and should remain safe by default.
    Fails when: the safety guard rejects the default smoke-test prefix.
    """
    module = _load_smoke_module()

    module.assert_disposable_smoke_target("default", "rules_engine_smoke_test_deleteme")


def test_smoke_test_rejects_non_disposable_targets():
    """
    What: Rejects smoke-test table targets without smoke/test/deleteme markers.
    Why: A misconfigured smoke test must not overwrite production-looking tables.
    Fails when: the safety guard permits production-looking database and prefix names.
    """
    module = _load_smoke_module()

    with pytest.raises(ValueError, match="Smoke test target"):
        module.assert_disposable_smoke_target("prod_rules", "rules_engine")
