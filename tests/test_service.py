import pytest

from rules_engine.compiler_yaml import YamlRulesetCompiler
from rules_engine.enums import AuditLevel
from rules_engine.exceptions import RepositoryError
from rules_engine.registry import FunctionRegistry
from rules_engine.repository import RulesEngineTableNames
from rules_engine.service import RulesEngineService
from rules_engine.spark_validator import SparkRulesetCompatibilityValidator
from rules_engine.standard_functions import standard_function_rows


class RecordingRepository:
    def __init__(self):
        self.created_mode = None
        self.saved_ruleset = None
        self.published_by = None
        self.saved_function_rows = None
        self.update_existing = None
        self.retired = None

    def create_base_tables(self, mode="error"):
        self.created_mode = mode

    def save_function_registry_rows(self, rows, *, update_existing=True):
        self.saved_function_rows = rows
        self.update_existing = update_existing

    def save_published(
        self,
        ruleset,
        *,
        published_by=None,
        effective_start_date=None,
        effective_end_date=None,
    ):
        self.saved_ruleset = ruleset
        self.published_by = published_by
        self.effective_start_date = effective_start_date
        self.effective_end_date = effective_end_date

    def load_published(self, ruleset_name, version=None):
        if self.saved_ruleset is None or self.saved_ruleset.ruleset_name != ruleset_name:
            raise RepositoryError(f"Published ruleset not found: {ruleset_name}")
        return self.saved_ruleset

    def retire(self, ruleset_id, version, *, retired_by=None, effective_end_date=None):
        self.retired = (ruleset_id, version, retired_by, effective_end_date)


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


def test_service_runtime_reuses_injected_compatibility_validator():
    """Publish and runtime schema checks share the configured validator."""
    validator = SparkRulesetCompatibilityValidator(FunctionRegistry())

    service = RulesEngineService(
        repository=RecordingRepository(),
        registry=FunctionRegistry(),
        validator=validator,
    )

    assert service.runtime._compatibility_validator is validator


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


def test_service_from_schema_accepts_custom_table_names():
    """
    What: Builds a service from schema while overriding metadata table names.
    Why: Deployments may need project-specific table names without manual repository wiring.
    Fails when: from_schema ignores custom table-name overrides.
    """
    service = RulesEngineService.from_schema(
        None,
        "catalog.schema",
        ruleset_versions_table="catalog.schema.custom_ruleset_versions",
        function_registry_table="catalog.schema.custom_function_registry",
    )

    assert service.table_names.ruleset_versions == "catalog.schema.custom_ruleset_versions"
    assert service.table_names.function_registry == "catalog.schema.custom_function_registry"


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
    assert repository.update_existing is True
    assert repository.retired == ("rs1", "1", "tester", None)


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
    assert repository.update_existing is True


def test_service_can_preserve_standard_function_registry_when_requested():
    """
    What: Allows callers to preserve existing standard registry rows explicitly.
    Why: Package upgrades upsert by default, but controlled deployments may pin metadata.
    Fails when: The update_existing option is not passed through.
    """
    repository = RecordingRepository()
    service = _service(repository)

    service.save_standard_function_registry(update_existing=False)

    assert repository.update_existing is False


def test_service_evaluate_dataframe_requires_ruleset_or_name():
    """
    What: Rejects evaluate calls without a ruleset or ruleset name.
    Why: Silent evaluation against no ruleset would hide caller errors.
    Fails when: evaluate_dataframe accepts ambiguous input.
    """
    service = _service()

    with pytest.raises(ValueError, match="ruleset or ruleset_name"):
        service.evaluate_dataframe(None)


def test_service_passes_runtime_error_options_through(monkeypatch):
    ruleset = YamlRulesetCompiler().compile_text(_yaml_text())
    service = _service()
    captured = {}

    def evaluate_dataframe(df, supplied_ruleset, **kwargs):
        captured.update(kwargs)
        assert supplied_ruleset is ruleset
        return "evaluated"

    monkeypatch.setattr(service.runtime, "evaluate_dataframe", evaluate_dataframe)

    result = service.evaluate_dataframe(
        "input",
        ruleset=ruleset,
        fail_on_error=False,
        include_error_traceback=True,
    )

    assert result == "evaluated"
    assert captured == {
        "column_prefix": "rules_engine",
        "fail_on_error": False,
        "include_error_traceback": True,
        "audit_level": AuditLevel.FULL,
    }


def test_service_describe_rules_formats_supplied_ruleset():
    """
    What: Formats compiled rule metadata into readable table-shaped rows.
    Why: Notebook users need a compact audit view of rule logic and match payloads.
    Fails when: Service-level rule descriptions expose raw metadata or omit payload details.
    """
    ruleset = YamlRulesetCompiler().compile_text(
        """
ruleset_id: trace_ruleset
ruleset_name: Trace Ruleset
version: "1"
rules:
  - rule_id: r1560
    rule_name: A Rule
    rule_order: 1
    when:
      all:
        - left: {field: BK_AccountID}
          operator: eq
          right: {literal: DN}
          null_input_mode: propagate
          null_result_mode: "null"
    assign:
      leaf_key: "15656"
"""
    )

    rows = _service().describe_rules(ruleset=ruleset)

    assert rows == [
        {
            "rule_id": "r1560",
            "rule_name": "A Rule",
            "rule_logic": "BK_AccountID == 'DN'",
            "match_payload": "leaf_key = '15656'",
        }
    ]


def test_service_describe_rules_loads_published_ruleset_and_formats_nested_logic():
    """
    What: Loads a published ruleset and renders nested condition groups.
    Why: The service helper should work for persisted rules without losing boolean structure.
    Fails when: Nested groups are flattened ambiguously or repository loading is bypassed.
    """
    repository = RecordingRepository()
    service = _service(repository)
    service.publish_yaml_text(
        """
ruleset_id: trace_ruleset
ruleset_name: Trace Ruleset
version: "1"
owner: Rules Team
owner_department: ALM Engineering
rules:
  - rule_id: r1
    rule_name: Nested Rule
    rule_order: 1
    when:
      all:
        - left: {field: account}
          operator: eq
          right: {literal: A}
          null_input_mode: propagate
          null_result_mode: "null"
        - any:
            - left: {field: amount}
              operator: gt
              right: {literal: 100}
              null_input_mode: propagate
              null_result_mode: "null"
            - left: {field: status}
              operator: eq
              right: {literal: OPEN}
              null_input_mode: propagate
              null_result_mode: "null"
    assign:
      bucket: matched
"""
    )

    rows = service.describe_rules(ruleset_name="Trace Ruleset", version="1")

    assert rows == [
        {
            "rule_id": "r1",
            "rule_name": "Nested Rule",
            "rule_logic": "account == 'A' AND (amount > 100 OR status == 'OPEN')",
            "match_payload": "bucket = 'matched'",
        }
    ]


def test_service_describe_rules_requires_ruleset_or_name():
    """
    What: Rejects describe calls without a ruleset or ruleset name.
    Why: The helper should fail clearly like evaluate_dataframe.
    Fails when: describe_rules accepts ambiguous input.
    """
    service = _service()

    with pytest.raises(ValueError, match="ruleset or ruleset_name"):
        service.describe_rules()


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


def test_service_publish_and_retire_pass_effective_dates():
    """
    What: Passes effective date overrides through the facade.
    Why: Notebook callers should not need to reach into lower-level services for lifecycle dating.
    Fails when: Service methods drop effective-date arguments.
    """
    repository = RecordingRepository()
    service = _service(repository)
    ruleset = YamlRulesetCompiler().compile_text(_yaml_text())

    service.publish(
        ruleset,
        effective_start_date="2026-05-01",
        effective_end_date="2026-12-31",
    )
    service.retire(
        "rs1",
        "1",
        retired_by="tester",
        effective_end_date="2026-10-31",
    )

    assert repository.effective_start_date == "2026-05-01"
    assert repository.effective_end_date == "2026-12-31"
    assert repository.retired == ("rs1", "1", "tester", "2026-10-31")
