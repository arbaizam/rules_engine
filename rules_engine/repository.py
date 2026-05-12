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
import logging
from typing import Protocol
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from rules_engine.enums import RulesetStatus
from rules_engine.exceptions import RepositoryError
from rules_engine.models import FunctionRegistryRow, Ruleset, RulesetVersionRow
from rules_engine.serializer import DeltaRowSerializer


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RulesEngineTableNames:
    """
    Target table names for rules engine metadata.
    """

    ruleset_versions: str
    function_registry: str

    @classmethod
    def from_schema(cls, schema: str) -> "RulesEngineTableNames":
        """
        Build the standard rules engine table names under a catalog.schema path.
        """
        return cls(
            ruleset_versions=f"{schema}.ruleset_versions",
            function_registry=f"{schema}.function_registry",
        )


class RulesetRepository(Protocol):
    """
    Repository protocol used by publish and runtime services.

    Implementations persist canonical ruleset metadata, expose lifecycle
    transitions, and load only published rulesets for runtime execution.
    """

    def save_published(
        self,
        ruleset: Ruleset,
        *,
        published_by: str | None = None,
    ) -> None:
        """Persist published metadata."""

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
        """
        Create a Spark/Delta repository for the configured metadata tables.
        """
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
                StructField("owner", StringType(), True),
                StructField("owner_department", StringType(), True),
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

    def create_base_tables(self, mode: str = "error") -> None:
        """
        Create empty metadata tables using explicit Delta DDL.
        """
        specs = [
            (self.table_names.ruleset_versions, self._ruleset_version_ddl_columns()),
            (self.table_names.function_registry, self._function_registry_ddl_columns()),
        ]
        for table_name, ddl_columns in specs:
            logger.info("Creating rules engine metadata table: table=%s mode=%s", table_name, mode)
            self._create_delta_table(table_name, ddl_columns, mode=mode)

    def _create_delta_table(
        self,
        table_name: str,
        ddl_columns: list[str],
        *,
        mode: str,
    ) -> None:
        """
        Create one Delta table with explicit nullability metadata.

        Spark can drop ``nullable=False`` catalog metadata when creating an
        empty table through a DataFrame write, so bootstrap uses DDL.
        """
        normalized_mode = mode.lower()
        if normalized_mode == "error":
            normalized_mode = "errorifexists"
        if normalized_mode not in {"errorifexists", "ignore", "overwrite"}:
            raise RepositoryError(
                "Base table creation mode must be one of: error, errorifexists, ignore, overwrite"
            )
        exists_clause = "IF NOT EXISTS " if normalized_mode == "ignore" else ""
        if normalized_mode == "overwrite":
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        column_sql = ",\n                ".join(ddl_columns)
        self.spark.sql(
            f"""
            CREATE TABLE {exists_clause}{table_name} (
                {column_sql}
            )
            USING DELTA
            """
        )

    def _ruleset_version_ddl_columns(self) -> list[str]:
        """Return DDL columns for the authoritative ruleset-version table."""
        return [
            "ruleset_id STRING NOT NULL",
            "ruleset_name STRING NOT NULL",
            "version STRING NOT NULL",
            "status STRING NOT NULL",
            "description STRING",
            "payload_json STRING NOT NULL",
            "content_hash STRING NOT NULL",
            "rule_count INT NOT NULL",
            "condition_count INT NOT NULL",
            "assignment_count INT NOT NULL",
            "aggregate_count INT NOT NULL",
            "custom_function_count INT NOT NULL",
            "owner STRING",
            "owner_department STRING",
            "published_by STRING",
            "published_at STRING",
            "retired_by STRING",
            "retired_at STRING",
        ]

    def _function_registry_ddl_columns(self) -> list[str]:
        """Return DDL columns for the function registry table."""
        return [
            "function_name STRING NOT NULL",
            "implementation_reference STRING NOT NULL",
            "arg_contract_payload_json STRING NOT NULL",
            "return_type_hint STRING",
            "allowed_in_condition_flag BOOLEAN NOT NULL",
            "allowed_in_assignment_flag BOOLEAN NOT NULL",
            "active_flag BOOLEAN NOT NULL",
            "description STRING",
            "version STRING",
        ]

    def save_published(
        self,
        ruleset: Ruleset,
        *,
        published_by: str | None = None,
    ) -> None:
        """
        Persist a published ruleset version.

        Published and retired versions are immutable by ruleset_name/version.
        Multiple versions of the same ruleset_name may be published at once so
        callers can test candidate versions side by side.
        """
        existing_status = self._existing_ruleset_status(
            ruleset.ruleset_name,
            ruleset.version,
        )
        if existing_status is not None:
            logger.error(
                "Rejected published overwrite for immutable ruleset version: ruleset_name=%s version=%s existing_status=%s",
                ruleset.ruleset_name,
                ruleset.version,
                existing_status,
            )
            raise RepositoryError(
                f"Cannot overwrite ruleset version with status={existing_status}: "
                f"ruleset_name={ruleset.ruleset_name}, version={ruleset.version}"
            )
        logger.info(
            "Persisting published ruleset version: table=%s ruleset_id=%s ruleset_name=%s version=%s",
            self.table_names.ruleset_versions,
            ruleset.ruleset_id,
            ruleset.ruleset_name,
            ruleset.version,
        )
        row = self.serializer.serialize_ruleset_version(
            ruleset,
            published_by=self._actor_or_system(published_by),
            published_at=self._utc_now(),
        )
        self._write_rows(
            self.table_names.ruleset_versions,
            [asdict(row)],
            self.ruleset_version_schema,
        )
        logger.info(
            "Published ruleset version persisted: ruleset_id=%s version=%s content_hash=%s",
            ruleset.ruleset_id,
            ruleset.version,
            row.content_hash,
        )

    def retire(self, ruleset_id: str, version: str, *, retired_by: str | None = None) -> None:
        """
        Mark a persisted ruleset version as retired.
        """
        logger.info("Retiring ruleset version: ruleset_id=%s version=%s", ruleset_id, version)
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
        logger.info(
            "Loading published ruleset: table=%s ruleset_name=%s version=%s",
            self.table_names.ruleset_versions,
            ruleset_name,
            version,
        )
        rows_df = self.spark.table(self.table_names.ruleset_versions).where(ruleset_filter)
        collected = rows_df.limit(2).collect()
        if not collected:
            logger.error("Published ruleset not found: ruleset_name=%s version=%s", ruleset_name, version)
            raise RepositoryError(f"Published ruleset not found: {ruleset_name}")
        if len(collected) > 1 and version is None:
            logger.error("Multiple published ruleset versions found: ruleset_name=%s", ruleset_name)
            raise RepositoryError(
                f"Multiple published versions found for {ruleset_name}; specify version."
            )
        row = RulesetVersionRow(**collected[0].asDict(recursive=True))
        logger.info(
            "Published ruleset loaded: ruleset_id=%s ruleset_name=%s version=%s content_hash=%s",
            row.ruleset_id,
            row.ruleset_name,
            row.version,
            row.content_hash,
        )
        return self.serializer.deserialize_ruleset_version(row)

    def save_function_registry_rows(self, rows: list[FunctionRegistryRow]) -> None:
        """
        Upsert function registry metadata rows by function_name.
        """
        logger.info("Saving function registry rows: table=%s row_count=%s", self.table_names.function_registry, len(rows))
        prepared_rows = [self._function_to_spark_dict(row) for row in rows]
        if not prepared_rows:
            return
        if not self._table_exists(self.table_names.function_registry):
            self._write_rows(
                self.table_names.function_registry,
                prepared_rows,
                self.function_registry_schema,
            )
            return
        staging_view = f"_rules_engine_function_registry_{uuid4().hex}"
        self.spark.createDataFrame(prepared_rows, schema=self.function_registry_schema).createOrReplaceTempView(
            staging_view
        )
        columns = [field.name for field in self.function_registry_schema.fields]
        update_assignments = ", ".join(f"target.{column} = source.{column}" for column in columns)
        insert_columns = ", ".join(columns)
        insert_values = ", ".join(f"source.{column}" for column in columns)
        try:
            self.spark.sql(
                f"""
                MERGE INTO {self.table_names.function_registry} AS target
                USING {staging_view} AS source
                ON target.function_name = source.function_name
                WHEN MATCHED THEN UPDATE SET {update_assignments}
                WHEN NOT MATCHED THEN INSERT ({insert_columns})
                VALUES ({insert_values})
                """
            )
        finally:
            self.spark.catalog.dropTempView(staging_view)

    def _write_rows(self, table_name: str, rows: list[dict], schema: StructType) -> None:
        """
        Append row dictionaries to a Delta table using the supplied schema.

        Empty row lists are treated as no-ops so callers can pass filtered
        write sets without guarding every call.
        """
        if not rows:
            return
        logger.debug("Appending rows to Delta table: table=%s row_count=%s", table_name, len(rows))
        self.spark.createDataFrame(rows, schema=schema).write.format("delta").mode(
            "append"
        ).saveAsTable(table_name)

    def _set_status(
        self,
        ruleset_id: str,
        version: str,
        status: RulesetStatus,
        *,
        retired_by: str | None = None,
        retired_at: str | None = None,
    ) -> None:
        """
        Update lifecycle status fields for one version.
        """
        row = self._ruleset_row_dict(ruleset_id, version)
        if row is None:
            raise RepositoryError(
                f"Ruleset version not found: ruleset_id={ruleset_id}, version={version}"
            )

        assignments = [
            f"status = {self._sql(status.value)}",
        ]
        if status is RulesetStatus.RETIRED:
            assignments.append(f"retired_by = {self._sql_nullable(retired_by)}")
            assignments.append(f"retired_at = {self._sql_nullable(retired_at)}")

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
            logger.error(
                "Ruleset status update verification failed: ruleset_id=%s version=%s expected_status=%s",
                ruleset_id,
                version,
                status.value,
            )
            raise RepositoryError(
                f"Status update failed: ruleset_id={ruleset_id}, "
                f"version={version}, status={status.value}"
            )
        logger.info(
            "Ruleset status updated: ruleset_id=%s version=%s status=%s",
            ruleset_id,
            version,
            status.value,
        )

    def _existing_ruleset_status(self, ruleset_name: str, version: str) -> str | None:
        """
        Return the persisted status for one version, or None when absent.
        """
        row = self._ruleset_row_dict_by_name_version(ruleset_name, version)
        return row["status"] if row is not None else None

    def _ruleset_row_dict_by_name_version(
        self,
        ruleset_name: str,
        version: str,
    ) -> dict | None:
        """
        Load one ruleset version row by public identity.

        The publish guard uses ruleset_name/version because ruleset_id can be
        environment-specific or generated, while the authored name and version
        are the caller-facing duplicate boundary.
        """
        if not self._table_exists(self.table_names.ruleset_versions):
            return None
        rows = (
            self.spark.table(self.table_names.ruleset_versions)
            .where((F.col("ruleset_name") == ruleset_name) & (F.col("version") == version))
            .limit(1)
            .collect()
        )
        return rows[0].asDict(recursive=True) if rows else None

    def _ruleset_row_dict(self, ruleset_id: str, version: str) -> dict | None:
        """
        Load one ruleset version row as a recursive Python dictionary.

        Recursive conversion is required because Spark returns nested structs
        as Row objects by default, while serializer/model construction expects
        ordinary nested dictionaries.
        """
        if not self._table_exists(self.table_names.ruleset_versions):
            return None
        rows = (
            self.spark.table(self.table_names.ruleset_versions)
            .where((F.col("ruleset_id") == ruleset_id) & (F.col("version") == version))
            .limit(1)
            .collect()
        )
        return rows[0].asDict(recursive=True) if rows else None

    def _table_exists(self, table_name: str) -> bool:
        """
        Return whether Spark catalog metadata contains the target table.
        """
        return bool(self.spark.catalog.tableExists(table_name))

    def _sql(self, value: str) -> str:
        """
        Return a single-quoted SQL string literal with quotes escaped.
        """
        return "'" + value.replace("'", "''") + "'"

    def _sql_nullable(self, value: str | None) -> str:
        """
        Return a SQL literal for optional string metadata values.
        """
        return "NULL" if value is None else self._sql(value)

    def _utc_now(self) -> str:
        """
        Return the current UTC timestamp in ISO-8601 string form.
        """
        return datetime.now(timezone.utc).isoformat()

    def _actor_or_system(self, value: str | None) -> str:
        """
        Normalize optional actor metadata to a non-empty string.

        Locked-down production jobs may omit actor arguments; in those cases
        ``system`` is stored explicitly.
        """
        if value is None:
            return "system"
        stripped = value.strip()
        return stripped or "system"

    def _function_to_spark_dict(self, row: FunctionRegistryRow) -> dict:
        """
        Convert a function registry row into the Spark table row shape.

        The in-memory model stores ``arg_contract_payload`` as a dictionary;
        the Delta table stores the same value as canonical JSON.
        """
        payload = asdict(row)
        payload["arg_contract_payload_json"] = json.dumps(
            payload.pop("arg_contract_payload"),
            sort_keys=True,
        )
        return payload
