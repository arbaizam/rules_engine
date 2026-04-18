"""
Spark/Delta repository for rules engine metadata.

This module is Databricks-oriented. It uses explicit Spark schemas for stable
table creation and writes row payloads to Delta-compatible Spark tables.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Protocol

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import RepositoryError
from rules_engine.models import (
    AssignmentRow,
    ConditionGroupRow,
    ConditionRow,
    DeltaRows,
    FunctionRegistryRow,
    RuleRow,
    Ruleset,
    RulesetRow,
    ValidationResult,
    ValidationResultRow,
)
from rules_engine.serializer import DeltaRowSerializer


@dataclass(frozen=True)
class RulesEngineTableNames:
    """
    Target table names for rules engine metadata.
    """

    rulesets: str
    rules: str
    condition_groups: str
    conditions: str
    assignments: str
    function_registry: str
    validation_results: str


class RulesetRepository(Protocol):
    """Repository protocol used by publish and runtime services."""

    def save_draft(self, ruleset: Ruleset, *, created_by: str | None = None) -> None:
        """Persist draft metadata."""

    def publish(self, ruleset_id: str, version: str, *, published_by: str | None = None) -> None:
        """Mark a persisted ruleset version as published."""

    def retire(self, ruleset_id: str, version: str) -> None:
        """Mark a persisted ruleset version as retired."""

    def load_published(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """Load published metadata."""

    def save_validation_results(
        self,
        ruleset_id: str,
        version: str,
        validation_result: ValidationResult,
    ) -> None:
        """Persist validation result rows."""


class SparkDeltaRulesetRepository:
    """
    Spark-backed repository for Databricks Delta metadata tables.
    """

    def __init__(
        self,
        spark: SparkSession,
        table_names: RulesEngineTableNames,
        serializer: DeltaRowSerializer | None = None,
    ) -> None:
        self.spark = spark
        self.table_names = table_names
        self.serializer = serializer or DeltaRowSerializer()

    @property
    def ruleset_schema(self) -> StructType:
        """Return the ruleset table schema."""
        return StructType(
            [
                StructField("ruleset_id", StringType(), False),
                StructField("ruleset_name", StringType(), False),
                StructField("version", StringType(), False),
                StructField("status", StringType(), False),
                StructField("description", StringType(), True),
                StructField("created_by", StringType(), False),
                StructField("created_at", StringType(), False),
                StructField("published_by", StringType(), True),
                StructField("published_at", StringType(), True),
                StructField("content_hash", StringType(), False),
            ]
        )

    @property
    def rule_schema(self) -> StructType:
        """Return the rule table schema."""
        return StructType(
            [
                StructField("rule_id", StringType(), False),
                StructField("ruleset_id", StringType(), False),
                StructField("ruleset_version", StringType(), False),
                StructField("rule_name", StringType(), False),
                StructField("rule_order", IntegerType(), False),
                StructField("active_flag", BooleanType(), False),
                StructField("stop_on_match", BooleanType(), False),
                StructField("description", StringType(), True),
                StructField("created_by", StringType(), False),
                StructField("created_at", StringType(), False),
            ]
        )

    @property
    def condition_group_schema(self) -> StructType:
        """Return the condition-group table schema."""
        return StructType(
            [
                StructField("condition_group_id", StringType(), False),
                StructField("ruleset_id", StringType(), False),
                StructField("ruleset_version", StringType(), False),
                StructField("rule_id", StringType(), False),
                StructField("parent_condition_group_id", StringType(), True),
                StructField("logical_operator", StringType(), False),
                StructField("group_order", IntegerType(), False),
                StructField("created_by", StringType(), False),
                StructField("created_at", StringType(), False),
            ]
        )

    @property
    def condition_schema(self) -> StructType:
        """Return the condition table schema."""
        return StructType(
            [
                StructField("condition_id", StringType(), False),
                StructField("ruleset_id", StringType(), False),
                StructField("ruleset_version", StringType(), False),
                StructField("rule_id", StringType(), False),
                StructField("condition_group_id", StringType(), False),
                StructField("condition_order", IntegerType(), False),
                StructField("left_operand_kind", StringType(), False),
                StructField("left_operand_payload_json", StringType(), False),
                StructField("operator", StringType(), False),
                StructField("right_operand_kind", StringType(), True),
                StructField("right_operand_payload_json", StringType(), True),
                StructField("aggregate_scope", StringType(), True),
                StructField("tolerance_abs", StringType(), False),
                StructField("null_input_mode", StringType(), False),
                StructField("null_result_mode", StringType(), False),
                StructField("null_default_value_json", StringType(), True),
                StructField("active_flag", BooleanType(), False),
                StructField("created_by", StringType(), False),
                StructField("created_at", StringType(), False),
            ]
        )

    @property
    def assignment_schema(self) -> StructType:
        """Return the assignment table schema."""
        return StructType(
            [
                StructField("assignment_id", StringType(), False),
                StructField("ruleset_id", StringType(), False),
                StructField("ruleset_version", StringType(), False),
                StructField("rule_id", StringType(), False),
                StructField("assignment_order", IntegerType(), False),
                StructField("target_field", StringType(), False),
                StructField("assign_operand_kind", StringType(), False),
                StructField("assign_operand_payload_json", StringType(), False),
                StructField("created_by", StringType(), False),
                StructField("created_at", StringType(), False),
            ]
        )

    @property
    def function_registry_schema(self) -> StructType:
        """Return the function registry table schema."""
        return StructType(
            [
                StructField("function_name", StringType(), False),
                StructField("implementation_reference", StringType(), False),
                StructField("arg_contract_payload_json", StringType(), False),
                StructField("return_type_hint", StringType(), True),
                StructField("allowed_in_condition_flag", BooleanType(), False),
                StructField("allowed_in_assignment_flag", BooleanType(), False),
                StructField("active_flag", BooleanType(), False),
                StructField("description", StringType(), True),
                StructField("version", StringType(), True),
            ]
        )

    @property
    def validation_result_schema(self) -> StructType:
        """Return the validation result table schema."""
        return StructType(
            [
                StructField("ruleset_id", StringType(), False),
                StructField("version", StringType(), False),
                StructField("severity", StringType(), False),
                StructField("check_name", StringType(), False),
                StructField("message", StringType(), False),
                StructField("object_type", StringType(), False),
                StructField("object_id", StringType(), False),
                StructField("details_payload_json", StringType(), True),
                StructField("run_at", StringType(), False),
            ]
        )

    def create_base_tables(self, mode: str = "errorifexists") -> None:
        """
        Create empty metadata tables using explicit schemas.
        """
        specs = [
            (self.table_names.rulesets, self.ruleset_schema),
            (self.table_names.rules, self.rule_schema),
            (self.table_names.condition_groups, self.condition_group_schema),
            (self.table_names.conditions, self.condition_schema),
            (self.table_names.assignments, self.assignment_schema),
            (self.table_names.function_registry, self.function_registry_schema),
            (self.table_names.validation_results, self.validation_result_schema),
        ]
        for table_name, schema in specs:
            self.spark.createDataFrame([], schema=schema).write.format("delta").mode(
                mode
            ).saveAsTable(table_name)

    def save_draft(self, ruleset: Ruleset, *, created_by: str | None = None) -> None:
        """
        Persist a draft ruleset version.

        Notes
        -----
        Existing rows for the same ruleset_id/version are replaced to keep a
        development publish loop deterministic.
        """
        existing_status = self._existing_ruleset_status(ruleset.ruleset_id, ruleset.version)
        if existing_status is not None and existing_status != RulesetStatus.DRAFT.value:
            raise RepositoryError(
                f"Cannot overwrite ruleset version with status={existing_status}: "
                f"ruleset_id={ruleset.ruleset_id}, version={ruleset.version}"
            )
        rows = self.serializer.serialize_ruleset(
            ruleset,
            created_by=self._actor_or_system(created_by),
            created_at=self._utc_now(),
        )
        self._delete_ruleset_version(ruleset.ruleset_id, ruleset.version)
        self._write_rows(
            self.table_names.rulesets,
            [asdict(rows.ruleset_row)],
            self.ruleset_schema,
        )
        self._write_rows(
            self.table_names.rules,
            [asdict(row) for row in rows.rule_rows],
            self.rule_schema,
        )
        self._write_rows(
            self.table_names.condition_groups,
            [asdict(row) for row in rows.condition_group_rows],
            self.condition_group_schema,
        )
        self._write_rows(
            self.table_names.conditions,
            [self._condition_to_spark_dict(row) for row in rows.condition_rows],
            self.condition_schema,
        )
        self._write_rows(
            self.table_names.assignments,
            [self._assignment_to_spark_dict(row) for row in rows.assignment_rows],
            self.assignment_schema,
        )

    def publish(self, ruleset_id: str, version: str, *, published_by: str | None = None) -> None:
        """
        Mark a persisted ruleset version as published.
        """
        self._assert_publish_allowed(ruleset_id, version)
        self._set_status(
            ruleset_id,
            version,
            RulesetStatus.PUBLISHED.value,
            published_by=self._actor_or_system(published_by),
            published_at=self._utc_now(),
        )

    def retire(self, ruleset_id: str, version: str) -> None:
        """
        Mark a persisted ruleset version as retired.
        """
        self._set_status(ruleset_id, version, RulesetStatus.RETIRED.value)

    def load_published(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """
        Load a published ruleset by name and optional version.
        """
        ruleset_filter = (
            (F.col("ruleset_name") == ruleset_name)
            & (F.col("status") == RulesetStatus.PUBLISHED.value)
        )
        if version is not None:
            ruleset_filter = ruleset_filter & (F.col("version") == version)
        ruleset_rows = self.spark.table(self.table_names.rulesets).where(ruleset_filter)
        if version is None:
            ruleset_rows = ruleset_rows.orderBy(F.col("version").desc())
        collected = ruleset_rows.limit(2).collect()
        if not collected:
            raise RepositoryError(f"Published ruleset not found: {ruleset_name}")
        if len(collected) > 1 and version is None:
            raise RepositoryError(
                f"Multiple published versions found for {ruleset_name}; specify version."
            )
        ruleset_row = RulesetRow(**collected[0].asDict())
        return self._load_ruleset_by_row(ruleset_row)

    def save_validation_results(
        self,
        ruleset_id: str,
        version: str,
        validation_result: ValidationResult,
    ) -> None:
        """
        Persist validation result rows for a ruleset version.
        """
        self._delete_validation_results(ruleset_id, version)
        rows = self.serializer.serialize_validation_result(
            ruleset_id,
            version,
            validation_result,
            run_at=self._utc_now(),
        )
        self._write_rows(
            self.table_names.validation_results,
            [self._validation_to_spark_dict(row) for row in rows],
            self.validation_result_schema,
        )

    def save_function_registry_rows(self, rows: list[FunctionRegistryRow]) -> None:
        """
        Upsert function registry metadata rows by function_name.
        """
        for row in rows:
            self._delete_from_table(
                self.table_names.function_registry,
                f"function_name = {self._sql(row.function_name)}",
            )
        self._write_rows(
            self.table_names.function_registry,
            [self._function_to_spark_dict(row) for row in rows],
            self.function_registry_schema,
        )

    def _load_ruleset_by_row(self, ruleset_row: RulesetRow) -> Ruleset:
        rule_dicts = [
            RuleRow(**row.asDict())
            for row in self.spark.table(self.table_names.rules)
            .where(
                (F.col("ruleset_id") == ruleset_row.ruleset_id)
                & (F.col("ruleset_version") == ruleset_row.version)
            )
            .collect()
        ]
        group_rows = [
            ConditionGroupRow(**row.asDict())
            for row in self.spark.table(self.table_names.condition_groups)
            .where(
                (F.col("ruleset_id") == ruleset_row.ruleset_id)
                & (F.col("ruleset_version") == ruleset_row.version)
            )
            .collect()
        ]
        condition_rows = [
            self._condition_from_spark_dict(row.asDict())
            for row in self.spark.table(self.table_names.conditions)
            .where(
                (F.col("ruleset_id") == ruleset_row.ruleset_id)
                & (F.col("ruleset_version") == ruleset_row.version)
            )
            .collect()
        ]
        assignment_rows = [
            self._assignment_from_spark_dict(row.asDict())
            for row in self.spark.table(self.table_names.assignments)
            .where(
                (F.col("ruleset_id") == ruleset_row.ruleset_id)
                & (F.col("ruleset_version") == ruleset_row.version)
            )
            .collect()
        ]
        return self.serializer.deserialize_ruleset(
            DeltaRows(
                ruleset_row=ruleset_row,
                rule_rows=rule_dicts,
                condition_group_rows=group_rows,
                condition_rows=condition_rows,
                assignment_rows=assignment_rows,
            )
        )

    def _write_rows(self, table_name: str, rows: list[dict], schema: StructType) -> None:
        if not rows:
            return
        self.spark.createDataFrame(rows, schema=schema).write.format("delta").mode(
            "append"
        ).saveAsTable(table_name)

    def _delete_ruleset_version(self, ruleset_id: str, version: str) -> None:
        self._delete_from_table(
            self.table_names.rulesets,
            f"ruleset_id = {self._sql(ruleset_id)} AND version = {self._sql(version)}",
        )
        version_predicate = (
            f"ruleset_id = {self._sql(ruleset_id)} "
            f"AND ruleset_version = {self._sql(version)}"
        )
        self._delete_from_table(self.table_names.rules, version_predicate)
        for table_name in (
            self.table_names.condition_groups,
            self.table_names.conditions,
            self.table_names.assignments,
        ):
            self._delete_from_table(table_name, version_predicate)

    def _delete_validation_results(self, ruleset_id: str, version: str) -> None:
        self._delete_from_table(
            self.table_names.validation_results,
            f"ruleset_id = {self._sql(ruleset_id)} AND version = {self._sql(version)}",
        )

    def _set_status(
        self,
        ruleset_id: str,
        version: str,
        status: str,
        *,
        published_by: str | None = None,
        published_at: str | None = None,
    ) -> None:
        if self._existing_ruleset_status(ruleset_id, version) is None:
            raise RepositoryError(
                f"Ruleset version not found: ruleset_id={ruleset_id}, version={version}"
            )
        publish_assignment = ""
        if status == RulesetStatus.PUBLISHED.value:
            publish_assignment = (
                f", published_by = {self._sql(published_by or '')}, "
                f"published_at = {self._sql(published_at or '')}"
            )
        self.spark.sql(
            f"""
            UPDATE {self.table_names.rulesets}
            SET status = {self._sql(status)}
                {publish_assignment}
            WHERE ruleset_id = {self._sql(ruleset_id)}
              AND version = {self._sql(version)}
            """
        )

    def _assert_publish_allowed(self, ruleset_id: str, version: str) -> None:
        existing = self._ruleset_row_dict(ruleset_id, version)
        if existing is None:
            raise RepositoryError(
                f"Ruleset version not found: ruleset_id={ruleset_id}, version={version}"
            )
        if existing["status"] == RulesetStatus.PUBLISHED.value:
            raise RepositoryError(
                f"Cannot overwrite an already published ruleset version: "
                f"ruleset_id={ruleset_id}, version={version}"
            )
        if existing["status"] != RulesetStatus.DRAFT.value:
            raise RepositoryError(
                f"Only draft ruleset versions can be published: "
                f"ruleset_id={ruleset_id}, version={version}, status={existing['status']}"
            )
        published_count = (
            self.spark.table(self.table_names.rulesets)
            .where(
                (F.col("ruleset_name") == existing["ruleset_name"])
                & (F.col("status") == RulesetStatus.PUBLISHED.value)
            )
            .count()
        )
        if published_count:
            raise RepositoryError(
                f"Cannot publish {existing['ruleset_name']} while another version is published."
            )

    def _existing_ruleset_status(self, ruleset_id: str, version: str) -> str | None:
        row = self._ruleset_row_dict(ruleset_id, version)
        return row["status"] if row is not None else None

    def _ruleset_row_dict(self, ruleset_id: str, version: str) -> dict | None:
        if not self._table_exists(self.table_names.rulesets):
            return None
        rows = (
            self.spark.table(self.table_names.rulesets)
            .where(
                (F.col("ruleset_id") == ruleset_id)
                & (F.col("version") == version)
            )
            .limit(1)
            .collect()
        )
        return rows[0].asDict() if rows else None

    def _delete_from_table(self, table_name: str, predicate: str) -> None:
        if self._table_exists(table_name):
            self.spark.sql(f"DELETE FROM {table_name} WHERE {predicate}")

    def _table_exists(self, table_name: str) -> bool:
        return bool(self.spark.catalog.tableExists(table_name))

    def _sql(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _actor_or_system(self, value: str | None) -> str:
        if value is None:
            return "system"
        stripped = value.strip()
        return stripped or "system"

    def _condition_to_spark_dict(self, row: ConditionRow) -> dict:
        return {
            "condition_id": row.condition_id,
            "ruleset_id": row.ruleset_id,
            "ruleset_version": row.ruleset_version,
            "rule_id": row.rule_id,
            "condition_group_id": row.condition_group_id,
            "condition_order": row.condition_order,
            "left_operand_kind": row.left_operand_kind,
            "left_operand_payload_json": json.dumps(row.left_operand_payload, sort_keys=True),
            "operator": row.operator,
            "right_operand_kind": row.right_operand_kind,
            "right_operand_payload_json": (
                json.dumps(row.right_operand_payload, sort_keys=True)
                if row.right_operand_payload is not None
                else None
            ),
            "aggregate_scope": row.aggregate_scope,
            "tolerance_abs": row.tolerance_abs,
            "null_input_mode": row.null_input_mode,
            "null_result_mode": row.null_result_mode,
            "null_default_value_json": (
                json.dumps(row.null_default_value, sort_keys=True)
                if row.null_default_value is not None
                else None
            ),
            "active_flag": row.active_flag,
            "created_by": row.created_by,
            "created_at": row.created_at,
        }

    def _condition_from_spark_dict(self, row: dict) -> ConditionRow:
        return ConditionRow(
            condition_id=row["condition_id"],
            ruleset_id=row["ruleset_id"],
            ruleset_version=row["ruleset_version"],
            rule_id=row["rule_id"],
            condition_group_id=row["condition_group_id"],
            condition_order=row["condition_order"],
            left_operand_kind=row["left_operand_kind"],
            left_operand_payload=json.loads(row["left_operand_payload_json"]),
            operator=row["operator"],
            right_operand_kind=row["right_operand_kind"],
            right_operand_payload=(
                json.loads(row["right_operand_payload_json"])
                if row["right_operand_payload_json"] is not None
                else None
            ),
            aggregate_scope=row["aggregate_scope"],
            tolerance_abs=row["tolerance_abs"],
            null_input_mode=row["null_input_mode"],
            null_result_mode=row["null_result_mode"],
            null_default_value=(
                json.loads(row["null_default_value_json"])
                if row["null_default_value_json"] is not None
                else None
            ),
            active_flag=row["active_flag"],
            created_by=row["created_by"],
            created_at=row["created_at"],
        )

    def _assignment_to_spark_dict(self, row: AssignmentRow) -> dict:
        return {
            "assignment_id": row.assignment_id,
            "ruleset_id": row.ruleset_id,
            "ruleset_version": row.ruleset_version,
            "rule_id": row.rule_id,
            "assignment_order": row.assignment_order,
            "target_field": row.target_field,
            "assign_operand_kind": row.assign_operand_kind,
            "assign_operand_payload_json": json.dumps(row.assign_operand_payload, sort_keys=True),
            "created_by": row.created_by,
            "created_at": row.created_at,
        }

    def _assignment_from_spark_dict(self, row: dict) -> AssignmentRow:
        return AssignmentRow(
            assignment_id=row["assignment_id"],
            ruleset_id=row["ruleset_id"],
            ruleset_version=row["ruleset_version"],
            rule_id=row["rule_id"],
            assignment_order=row["assignment_order"],
            target_field=row["target_field"],
            assign_operand_kind=row["assign_operand_kind"],
            assign_operand_payload=json.loads(row["assign_operand_payload_json"]),
            created_by=row["created_by"],
            created_at=row["created_at"],
        )

    def _function_to_spark_dict(self, row: FunctionRegistryRow) -> dict:
        payload = asdict(row)
        payload["arg_contract_payload_json"] = json.dumps(
            payload.pop("arg_contract_payload"),
            sort_keys=True,
        )
        return payload

    def _validation_to_spark_dict(self, row: ValidationResultRow) -> dict:
        payload = asdict(row)
        payload["details_payload_json"] = (
            json.dumps(payload.pop("details_payload"), sort_keys=True)
            if row.details_payload is not None
            else None
        )
        return payload
