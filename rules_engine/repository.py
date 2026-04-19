"""
Spark/Delta repository for rules engine metadata.

The repository treats a ruleset version as one immutable metadata document.
The authoritative runtime table stores one row per ruleset_id/version with the
canonical YAML/JSON payload, summary counts, lifecycle status, provenance, and
content hash. Function registry metadata remains separate because it is
environment-level metadata rather than ruleset-version metadata.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from typing import Protocol

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import RepositoryError
from rules_engine.models import FunctionRegistryRow, Ruleset, RulesetVersionRow
from rules_engine.serializer import DeltaRowSerializer


@dataclass(frozen=True)
class RulesEngineTableNames:
    """
    Target table names for rules engine metadata.
    """

    ruleset_versions: str
    function_registry: str


class RulesetRepository(Protocol):
    """Repository protocol used by publish and runtime services."""

    def save_draft(self, ruleset: Ruleset, *, created_by: str | None = None) -> None:
        """Persist draft metadata."""

    def publish(self, ruleset_id: str, version: str, *, published_by: str | None = None) -> None:
        """Mark a persisted ruleset version as published."""

    def retire(self, ruleset_id: str, version: str, *, retired_by: str | None = None) -> None:
        """Mark a persisted ruleset version as retired."""

    def load_published(self, ruleset_name: str, version: str | None = None) -> Ruleset:
        """Load published metadata."""


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
    def ruleset_version_schema(self) -> StructType:
        """Return the authoritative ruleset-version table schema."""
        return StructType(
            [
                StructField("ruleset_id", StringType(), False),
                StructField("ruleset_name", StringType(), False),
                StructField("version", StringType(), False),
                StructField("status", StringType(), False),
                StructField("description", StringType(), True),
                StructField("payload_json", StringType(), False),
                StructField("content_hash", StringType(), False),
                StructField("rule_count", IntegerType(), False),
                StructField("condition_count", IntegerType(), False),
                StructField("assignment_count", IntegerType(), False),
                StructField("aggregate_count", IntegerType(), False),
                StructField("custom_function_count", IntegerType(), False),
                StructField("created_by", StringType(), False),
                StructField("created_at", StringType(), False),
                StructField("published_by", StringType(), True),
                StructField("published_at", StringType(), True),
                StructField("retired_by", StringType(), True),
                StructField("retired_at", StringType(), True),
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

    def create_base_tables(self, mode: str = "errorifexists") -> None:
        """
        Create empty metadata tables using explicit schemas.
        """
        specs = [
            (self.table_names.ruleset_versions, self.ruleset_version_schema),
            (self.table_names.function_registry, self.function_registry_schema),
        ]
        for table_name, schema in specs:
            self.spark.createDataFrame([], schema=schema).write.format("delta").mode(
                mode
            ).saveAsTable(table_name)

    def save_draft(self, ruleset: Ruleset, *, created_by: str | None = None) -> None:
        """
        Persist a draft ruleset version.

        Existing draft rows for the same ruleset_id/version are replaced to keep
        a development publish loop deterministic. Published and retired rows are
        immutable through this path.
        """
        existing_status = self._existing_ruleset_status(ruleset.ruleset_id, ruleset.version)
        if existing_status is not None and existing_status != RulesetStatus.DRAFT.value:
            raise RepositoryError(
                f"Cannot overwrite ruleset version with status={existing_status}: "
                f"ruleset_id={ruleset.ruleset_id}, version={ruleset.version}"
            )
        row = self.serializer.serialize_ruleset_version(
            ruleset,
            created_by=self._actor_or_system(created_by),
            created_at=self._utc_now(),
        )
        self._delete_ruleset_version(ruleset.ruleset_id, ruleset.version)
        self._write_rows(
            self.table_names.ruleset_versions,
            [asdict(row)],
            self.ruleset_version_schema,
        )

    def publish(self, ruleset_id: str, version: str, *, published_by: str | None = None) -> None:
        """
        Mark a persisted ruleset version as published.
        """
        self._assert_publish_allowed(ruleset_id, version)
        self._set_status(
            ruleset_id,
            version,
            RulesetStatus.PUBLISHED,
            published_by=self._actor_or_system(published_by),
            published_at=self._utc_now(),
        )

    def retire(self, ruleset_id: str, version: str, *, retired_by: str | None = None) -> None:
        """
        Mark a persisted ruleset version as retired.
        """
        self._set_status(
            ruleset_id,
            version,
            RulesetStatus.RETIRED,
            retired_by=self._actor_or_system(retired_by),
            retired_at=self._utc_now(),
        )

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
        rows_df = self.spark.table(self.table_names.ruleset_versions).where(ruleset_filter)
        collected = rows_df.limit(2).collect()
        if not collected:
            raise RepositoryError(f"Published ruleset not found: {ruleset_name}")
        if len(collected) > 1 and version is None:
            raise RepositoryError(
                f"Multiple published versions found for {ruleset_name}; specify version."
            )
        return self.serializer.deserialize_ruleset_version(
            RulesetVersionRow(**collected[0].asDict())
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

    def _write_rows(self, table_name: str, rows: list[dict], schema: StructType) -> None:
        if not rows:
            return
        self.spark.createDataFrame(rows, schema=schema).write.format("delta").mode(
            "append"
        ).saveAsTable(table_name)

    def _delete_ruleset_version(self, ruleset_id: str, version: str) -> None:
        self._delete_from_table(
            self.table_names.ruleset_versions,
            f"ruleset_id = {self._sql(ruleset_id)} AND version = {self._sql(version)}",
        )

    def _set_status(
        self,
        ruleset_id: str,
        version: str,
        status: RulesetStatus,
        *,
        published_by: str | None = None,
        published_at: str | None = None,
        retired_by: str | None = None,
        retired_at: str | None = None,
    ) -> None:
        row = self._ruleset_row_dict(ruleset_id, version)
        if row is None:
            raise RepositoryError(
                f"Ruleset version not found: ruleset_id={ruleset_id}, version={version}"
            )

        assignments = [
            f"status = {self._sql(status.value)}",
        ]
        if status is RulesetStatus.PUBLISHED:
            assignments.extend(
                [
                    f"published_by = {self._sql(published_by or '')}",
                    f"published_at = {self._sql(published_at or '')}",
                ]
            )
        if status is RulesetStatus.RETIRED:
            assignments.extend(
                [
                    f"retired_by = {self._sql(retired_by or '')}",
                    f"retired_at = {self._sql(retired_at or '')}",
                ]
            )

        self.spark.sql(
            f"""
            UPDATE {self.table_names.ruleset_versions}
            SET {", ".join(assignments)}
            WHERE ruleset_id = {self._sql(ruleset_id)}
              AND version = {self._sql(version)}
            """
        )
        updated = self._ruleset_row_dict(ruleset_id, version)
        if updated is None or updated["status"] != status.value:
            raise RepositoryError(
                f"Status update failed: ruleset_id={ruleset_id}, "
                f"version={version}, status={status.value}"
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
            self.spark.table(self.table_names.ruleset_versions)
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
        if not self._table_exists(self.table_names.ruleset_versions):
            return None
        rows = (
            self.spark.table(self.table_names.ruleset_versions)
            .where((F.col("ruleset_id") == ruleset_id) & (F.col("version") == version))
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

    def _function_to_spark_dict(self, row: FunctionRegistryRow) -> dict:
        payload = asdict(row)
        payload["arg_contract_payload_json"] = json.dumps(
            payload.pop("arg_contract_payload"),
            sort_keys=True,
        )
        return payload
