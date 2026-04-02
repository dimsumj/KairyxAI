from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class RedshiftColumn:
    name: str
    sql_type: str
    kind: str = "text"


@dataclass(frozen=True)
class RedshiftTargetSpec:
    table_name: str
    columns: tuple[RedshiftColumn, ...]
    sortkey: str = ""


class RedshiftWarehouseService:
    _TARGET_SPECS: dict[str, RedshiftTargetSpec] = {
        "events_staging": RedshiftTargetSpec(
            table_name="events_staging",
            columns=(
                RedshiftColumn("job_id", "VARCHAR(256)"),
                RedshiftColumn("job_identifier", "VARCHAR(256)"),
                RedshiftColumn("source", "VARCHAR(256)"),
                RedshiftColumn("source_config_id", "VARCHAR(256)"),
                RedshiftColumn("shard_index", "INTEGER", "int"),
                RedshiftColumn("schema_version", "VARCHAR(64)"),
                RedshiftColumn("raw_gcs_uri", "VARCHAR(4096)"),
                RedshiftColumn("player_id", "VARCHAR(256)"),
                RedshiftColumn("canonical_user_id", "VARCHAR(256)"),
                RedshiftColumn("event_type", "VARCHAR(256)"),
                RedshiftColumn("event_time", "TIMESTAMP", "timestamp"),
                RedshiftColumn("event_date", "DATE", "date"),
                RedshiftColumn("event_fingerprint", "VARCHAR(512)"),
                RedshiftColumn("source_event_id", "VARCHAR(512)"),
                RedshiftColumn("event_properties", "SUPER", "super"),
                RedshiftColumn("user_properties", "SUPER", "super"),
                RedshiftColumn("data_quality_flags", "SUPER", "super"),
                RedshiftColumn("payload", "SUPER", "super"),
            ),
            sortkey="COMPOUND SORTKEY(job_id, event_time)",
        ),
        "events_curated": RedshiftTargetSpec(
            table_name="events_curated",
            columns=(
                RedshiftColumn("job_id", "VARCHAR(256)"),
                RedshiftColumn("job_identifier", "VARCHAR(256)"),
                RedshiftColumn("source", "VARCHAR(256)"),
                RedshiftColumn("source_config_id", "VARCHAR(256)"),
                RedshiftColumn("shard_index", "INTEGER", "int"),
                RedshiftColumn("schema_version", "VARCHAR(64)"),
                RedshiftColumn("raw_gcs_uri", "VARCHAR(4096)"),
                RedshiftColumn("player_id", "VARCHAR(256)"),
                RedshiftColumn("canonical_user_id", "VARCHAR(256)"),
                RedshiftColumn("event_type", "VARCHAR(256)"),
                RedshiftColumn("event_time", "TIMESTAMP", "timestamp"),
                RedshiftColumn("event_date", "DATE", "date"),
                RedshiftColumn("event_fingerprint", "VARCHAR(512)"),
                RedshiftColumn("source_event_id", "VARCHAR(512)"),
                RedshiftColumn("event_properties", "SUPER", "super"),
                RedshiftColumn("user_properties", "SUPER", "super"),
                RedshiftColumn("data_quality_flags", "SUPER", "super"),
                RedshiftColumn("payload", "SUPER", "super"),
            ),
            sortkey="COMPOUND SORTKEY(job_id, event_time)",
        ),
        "player_latest_state": RedshiftTargetSpec(
            table_name="player_latest_state",
            columns=(
                RedshiftColumn("job_id", "VARCHAR(256)"),
                RedshiftColumn("job_identifier", "VARCHAR(256)"),
                RedshiftColumn("last_job_id", "VARCHAR(256)"),
                RedshiftColumn("player_id", "VARCHAR(256)"),
                RedshiftColumn("canonical_user_id", "VARCHAR(256)"),
                RedshiftColumn("email", "VARCHAR(512)"),
                RedshiftColumn("first_seen_at", "TIMESTAMP", "timestamp"),
                RedshiftColumn("last_seen_at", "TIMESTAMP", "timestamp"),
                RedshiftColumn("lifetime_events", "BIGINT", "int"),
                RedshiftColumn("total_events", "BIGINT", "int"),
                RedshiftColumn("lifetime_revenue_usd", "DOUBLE PRECISION", "float"),
                RedshiftColumn("total_revenue", "DOUBLE PRECISION", "float"),
                RedshiftColumn("total_sessions", "INTEGER", "int"),
                RedshiftColumn("sessions_7d", "INTEGER", "int"),
                RedshiftColumn("sessions_30d", "INTEGER", "int"),
                RedshiftColumn("days_since_last_seen", "INTEGER", "int"),
                RedshiftColumn("last_campaign", "VARCHAR(512)"),
                RedshiftColumn("last_media_source", "VARCHAR(512)"),
                RedshiftColumn("payload", "SUPER", "super"),
            ),
            sortkey="COMPOUND SORTKEY(canonical_user_id, last_seen_at)",
        ),
        "pipeline_dead_letters": RedshiftTargetSpec(
            table_name="pipeline_dead_letters",
            columns=(
                RedshiftColumn("job_id", "VARCHAR(256)"),
                RedshiftColumn("job_identifier", "VARCHAR(256)"),
                RedshiftColumn("source", "VARCHAR(256)"),
                RedshiftColumn("source_config_id", "VARCHAR(256)"),
                RedshiftColumn("shard_index", "INTEGER", "int"),
                RedshiftColumn("raw_gcs_uri", "VARCHAR(4096)"),
                RedshiftColumn("player_id", "VARCHAR(256)"),
                RedshiftColumn("canonical_user_id", "VARCHAR(256)"),
                RedshiftColumn("event_type", "VARCHAR(256)"),
                RedshiftColumn("event_time", "TIMESTAMP", "timestamp"),
                RedshiftColumn("event_date", "DATE", "date"),
                RedshiftColumn("data_quality_flags", "SUPER", "super"),
                RedshiftColumn("rejection_reason", "VARCHAR(512)"),
                RedshiftColumn("normalized_event", "SUPER", "super"),
                RedshiftColumn("payload", "SUPER", "super"),
            ),
            sortkey="COMPOUND SORTKEY(job_id, event_time)",
        ),
        "prediction_results": RedshiftTargetSpec(
            table_name="prediction_results",
            columns=(
                RedshiftColumn("prediction_job_id", "VARCHAR(256)"),
                RedshiftColumn("job_id", "VARCHAR(256)"),
                RedshiftColumn("canonical_user_id", "VARCHAR(256)"),
                RedshiftColumn("user_id", "VARCHAR(256)"),
                RedshiftColumn("email", "VARCHAR(512)"),
                RedshiftColumn("churn_state", "VARCHAR(128)"),
                RedshiftColumn("predicted_churn_risk", "DOUBLE PRECISION", "float"),
                RedshiftColumn("prediction_source", "VARCHAR(256)"),
                RedshiftColumn("suggested_action", "VARCHAR(512)"),
                RedshiftColumn("completed_at", "TIMESTAMP", "timestamp"),
                RedshiftColumn("baseline_churn_score", "DOUBLE PRECISION", "float"),
                RedshiftColumn("model_version", "VARCHAR(256)"),
                RedshiftColumn("score_timestamp", "TIMESTAMP", "timestamp"),
                RedshiftColumn("eligibility_reason", "VARCHAR(512)"),
                RedshiftColumn("effective_local_model_version", "VARCHAR(256)"),
                RedshiftColumn("effective_local_model_state", "VARCHAR(128)"),
                RedshiftColumn("recommended_template_id", "VARCHAR(256)"),
                RedshiftColumn("recommended_variant", "VARCHAR(256)"),
                RedshiftColumn("policy_snapshot_id", "VARCHAR(256)"),
                RedshiftColumn("payload", "SUPER", "super"),
            ),
            sortkey="COMPOUND SORTKEY(prediction_job_id, completed_at)",
        ),
    }

    def __init__(self):
        try:
            import boto3
        except ImportError as exc:
            raise RuntimeError("boto3 is required for WAREHOUSE_BACKEND=redshift.") from exc

        self._region_name = os.getenv("AWS_REGION") or None
        self._client = boto3.client("redshift-data", region_name=self._region_name)
        self._workgroup_name = str(os.getenv("REDSHIFT_WORKGROUP_NAME") or "").strip()
        self._database = str(os.getenv("REDSHIFT_DATABASE") or "").strip()
        self._schema = str(os.getenv("REDSHIFT_SCHEMA") or "public").strip() or "public"
        self._secret_arn = str(os.getenv("REDSHIFT_SECRET_ARN") or "").strip()
        self._db_user = str(os.getenv("REDSHIFT_DB_USER") or "").strip()
        if not self._workgroup_name or not self._database:
            raise RuntimeError("REDSHIFT_WORKGROUP_NAME and REDSHIFT_DATABASE are required for WAREHOUSE_BACKEND=redshift.")
        self._ensured_targets: set[str] = set()

    @property
    def schema(self) -> str:
        return self._schema

    def table_id(self, target: str) -> str:
        spec = self._TARGET_SPECS[target]
        return f'{self._schema}.{spec.table_name}'

    def _statement_args(self) -> dict[str, Any]:
        args: dict[str, Any] = {
            "WorkgroupName": self._workgroup_name,
            "Database": self._database,
        }
        if self._secret_arn:
            args["SecretArn"] = self._secret_arn
        elif self._db_user:
            args["DbUser"] = self._db_user
        return args

    def _run_statement(
        self,
        sql: str,
        *,
        parameters: Optional[list[dict[str, str]]] = None,
        timeout_seconds: int = 30,
        fetch: bool = False,
    ) -> list[dict[str, Any]]:
        kwargs = self._statement_args()
        kwargs["Sql"] = sql
        if parameters:
            kwargs["Parameters"] = parameters
        response = self._client.execute_statement(**kwargs)
        statement_id = response["Id"]
        deadline = time.time() + max(5, int(timeout_seconds))
        while True:
            description = self._client.describe_statement(Id=statement_id)
            status = str(description.get("Status") or "").upper()
            if status == "FINISHED":
                break
            if status in {"FAILED", "ABORTED"}:
                raise RuntimeError(
                    f"Redshift statement failed: {description.get('Error') or status}"
                )
            if time.time() >= deadline:
                raise TimeoutError(f"Redshift statement timed out after {timeout_seconds} seconds.")
            time.sleep(0.25)

        if not fetch:
            return []
        rows: list[dict[str, Any]] = []
        next_token: Optional[str] = None
        while True:
            result_kwargs = {"Id": statement_id}
            if next_token:
                result_kwargs["NextToken"] = next_token
            result = self._client.get_statement_result(**result_kwargs)
            column_names = [str(item.get("name") or "") for item in result.get("ColumnMetadata", [])]
            for record in result.get("Records", []):
                row: dict[str, Any] = {}
                for column_name, value in zip(column_names, record):
                    row[column_name] = self._decode_field(value)
                rows.append(row)
            next_token = result.get("NextToken")
            if not next_token:
                break
        return rows

    @staticmethod
    def _decode_field(field: dict[str, Any]) -> Any:
        if field.get("isNull"):
            return None
        for key in ("stringValue", "longValue", "doubleValue", "booleanValue", "blobValue"):
            if key in field:
                return field[key]
        return None

    def ensure_target(self, target: str) -> None:
        if target in self._ensured_targets:
            return
        spec = self._TARGET_SPECS[target]
        self._run_statement(f'CREATE SCHEMA IF NOT EXISTS {self._schema};', timeout_seconds=30)
        columns_sql = ", ".join(f"{column.name} {column.sql_type}" for column in spec.columns)
        sortkey_sql = f" {spec.sortkey}" if spec.sortkey else ""
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {self.table_id(target)} "
            f"({columns_sql}) DISTSTYLE AUTO{sortkey_sql};"
        )
        self._run_statement(create_sql, timeout_seconds=30)
        self._ensured_targets.add(target)

    def fetch_payload_rows(self, target: str, *, where_sql: str = "", order_sql: str = "", limit: Optional[int] = None) -> List[Dict[str, Any]]:
        self.ensure_target(target)
        limit_sql = f" LIMIT {max(1, int(limit))}" if limit is not None else ""
        sql = (
            f"SELECT JSON_SERIALIZE(payload) AS payload_json FROM {self.table_id(target)}"
            f"{(' WHERE ' + where_sql) if where_sql else ''}"
            f"{(' ' + order_sql) if order_sql else ''}"
            f"{limit_sql}"
        )
        rows = self._run_statement(sql, timeout_seconds=60, fetch=True)
        payloads: List[Dict[str, Any]] = []
        for row in rows:
            raw_payload = row.get("payload_json")
            if raw_payload in (None, "", "null"):
                continue
            try:
                parsed = json.loads(str(raw_payload))
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                payloads.append(parsed)
        return payloads

    def replace_rows(self, target: str, rows: List[Dict[str, Any]]) -> None:
        self.ensure_target(target)
        self._run_statement(f"TRUNCATE TABLE {self.table_id(target)};", timeout_seconds=120)
        self.append_rows(target, rows)

    def append_rows(self, target: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        self.ensure_target(target)
        spec = self._TARGET_SPECS[target]
        batch_size = 25
        for start_index in range(0, len(rows), batch_size):
            batch = rows[start_index:start_index + batch_size]
            parameters: list[dict[str, str]] = []
            value_clauses: list[str] = []
            for row_index, row in enumerate(batch):
                value_parts: list[str] = []
                for column in spec.columns:
                    parameter_name = f"{column.name}_{row_index}"
                    value_parts.append(self._column_value_sql(column, parameter_name))
                    parameters.append({"name": parameter_name, "value": self._column_value(column, row)})
                value_clauses.append(f"({', '.join(value_parts)})")
            insert_sql = (
                f"INSERT INTO {self.table_id(target)} ({', '.join(column.name for column in spec.columns)}) "
                f"VALUES {', '.join(value_clauses)};"
            )
            self._run_statement(insert_sql, parameters=parameters, timeout_seconds=120)

    def delete_rows_for_job(self, target: str, job_identifier: str, *, prediction_job: bool = False) -> None:
        self.ensure_target(target)
        field = "prediction_job_id" if prediction_job else "job_id"
        fallback_field = None if prediction_job else "job_identifier"
        if fallback_field:
            where_sql = f"DELETE FROM {self.table_id(target)} WHERE {field} = :job_id OR {fallback_field} = :job_id;"
        else:
            where_sql = f"DELETE FROM {self.table_id(target)} WHERE {field} = :job_id;"
        self._run_statement(
            where_sql,
            parameters=[{"name": "job_id", "value": str(job_identifier)}],
            timeout_seconds=120,
        )

    def count_rows(self, target: str, *, where_sql: str, parameters: list[dict[str, str]]) -> int:
        self.ensure_target(target)
        sql = f"SELECT COUNT(*) AS total FROM {self.table_id(target)} WHERE {where_sql};"
        rows = self._run_statement(sql, parameters=parameters, timeout_seconds=60, fetch=True)
        if not rows:
            return 0
        return int(rows[0].get("total") or 0)

    @staticmethod
    def _column_value_sql(column: RedshiftColumn, parameter_name: str) -> str:
        if column.kind == "super":
            return f"JSON_PARSE(:{parameter_name})"
        if column.kind == "timestamp":
            return f"CAST(NULLIF(:{parameter_name}, '') AS TIMESTAMP)"
        if column.kind == "date":
            return f"CAST(NULLIF(:{parameter_name}, '') AS DATE)"
        if column.kind == "int":
            return f"CAST(NULLIF(:{parameter_name}, '') AS BIGINT)"
        if column.kind == "float":
            return f"CAST(NULLIF(:{parameter_name}, '') AS DOUBLE PRECISION)"
        return f"NULLIF(:{parameter_name}, '')"

    @staticmethod
    def _column_value(column: RedshiftColumn, row: Dict[str, Any]) -> str:
        if column.name == "payload":
            value = row
        else:
            value = row.get(column.name)

        if column.kind == "super":
            return json.dumps(value)
        if value is None:
            return ""
        if column.kind == "timestamp":
            return str(value).replace("Z", "+00:00")
        if column.kind == "date":
            return str(value)
        if column.kind == "float":
            numeric = float(value)
            if math.isnan(numeric) or math.isinf(numeric):
                return ""
            return str(numeric)
        if column.kind == "int":
            return str(int(value))
        return str(value)
