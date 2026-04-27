import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.exceptions import RepositoryError
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesEngineTableNames
from rules_engine.service import RulesEngineService
from rules_engine.standard_functions import standard_function_rows


class RecordingRepository:
    def __init__(self):
        self.created_mode = None
        self.saved_ruleset = None
        self.published_by = None
        self.saved_function_rows = None
        self.retired = None

    def create_base_tables(self, mode="error"):
        self.created_mode = mode

    def save_function_registry_rows(self, rows):
        self.saved_function_rows = rows

    def save_published(self, ruleset, *, published_by=None):
        self.saved_ruleset = ruleset
        self.published_by = published_by

    def load_published(self, ruleset_name, version=None):
        if self.saved_ruleset is None or self.saved_ruleset.ruleset_name != ruleset_name:
            raise RepositoryError(f"Published ruleset not found: {ruleset_name}")
        return self.saved_ruleset

    def retire(self, ruleset_id, version, *, retired_by=None):
        self.retired = (ruleset_id, version, retired_by)


def _yaml_text():
    return """
ruleset_id: service_ruleset
ruleset_name: Service Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Rule 1
    rule_order: 1
    when:
      all:
        - left: {field: account}
          operator: eq
          right: {literal: A}
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      bucket: A
"""


def _service(repository=None):
    return RulesEngineService(
        repository=repository or RecordingRepository(),
        registry=FunctionRegistry(),
    )


def test_service_from_schema_uses_standard_table_names():
    """
    What: Builds a service from schema and checks repository table names.
    Why: The facade should hide repetitive repository wiring without changing table names.
    Fails when: from_schema stops using RulesEngineTableNames.from_schema.
    """
    service = RulesEngineService.from_schema(None, "catalog.schema")

    assert isinstance(service.repository.table_names, RulesEngineTableNames)
    assert service.table_names.ruleset_versions == "catalog.schema.ruleset_versions"
    assert service.table_names.function_registry == "catalog.schema.function_registry"


def test_service_publish_yaml_text_and_loads_published_ruleset():
    """
    What: Publishes YAML through the facade and loads it back.
    Why: Notebook users should have one public entry point for common publish/load work.
    Fails when: service wiring loses publish provenance or repository calls.
    """
    repository = RecordingRepository()
    service = _service(repository)

    ruleset = service.publish_yaml_text(_yaml_text(), published_by="tester")
    loaded = service.load_published("Service Ruleset", version="1")

    assert ruleset.ruleset_id == "service_ruleset"
    assert loaded == repository.saved_ruleset
    assert repository.published_by == "tester"


def test_service_create_tables_save_standard_functions_and_retire():
    """
    What: Exercises table creation, standard function registry save, and retire facade calls.
    Why: These are common notebook/pipeline operations that should stay easy to call.
    Fails when: facade methods stop delegating to the repository.
    """
    repository = RecordingRepository()
    service = _service(repository)

    service.create_tables(mode="ignore")
    service.save_standard_function_registry()
    service.retire("rs1", "1", retired_by="tester")

    assert repository.created_mode == "ignore"
    assert any(row.function_name == "substring" for row in repository.saved_function_rows)
    assert repository.retired == ("rs1", "1", "tester")


def test_service_saves_supplied_function_registry_rows():
    """
    What: Saves caller-supplied function registry rows through the facade.
    Why: Custom function notebooks should not need to reach through service.repository.
    Fails when: service stops exposing custom registry persistence.
    """
    repository = RecordingRepository()
    service = _service(repository)
    rows = standard_function_rows()

    service.save_function_registry_rows(rows)

    assert repository.saved_function_rows == rows


def test_service_evaluate_dataframe_requires_ruleset_or_name():
    """
    What: Rejects evaluate calls without a ruleset or ruleset name.
    Why: Silent evaluation against no ruleset would hide caller errors.
    Fails when: evaluate_dataframe accepts ambiguous input.
    """
    service = _service()

    with pytest.raises(ValueError, match="ruleset or ruleset_name"):
        service.evaluate_dataframe(None)


def test_service_publish_accepts_compiled_ruleset():
    """
    What: Publishes an already compiled ruleset through the facade.
    Why: Code-authored and precompiled YAML workflows should share the same service.
    Fails when: service.publish only supports YAML text/path entry points.
    """
    repository = RecordingRepository()
    service = _service(repository)
    ruleset = YamlRulesetCompiler().compile_text(_yaml_text())

    service.publish(ruleset, published_by="tester")

    assert repository.saved_ruleset.ruleset_id == "service_ruleset"
