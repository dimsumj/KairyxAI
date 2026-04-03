# bigquery_service.py

from collections import Counter
from functools import lru_cache
import os
import json
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any, Optional

from app.core.request_context import get_request_context
from provider_backends import resolve_warehouse_backend
from redshift_warehouse import RedshiftWarehouseService
from runtime_paths import normalize_env_text, resolve_runtime_file_path

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)


def _is_int_like_scalar(value: Any) -> bool:
    """Returns True for scalar integers (excluding booleans)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_oversized_int(value: Any) -> bool:
    """Checks if a scalar integer is outside the signed 64-bit range."""
    if not _is_int_like_scalar(value):
        return False
    return value > INT64_MAX or value < INT64_MIN


def _sanitize_for_storage(data: Any) -> Any:
    """
    Recursively traverses nested structures and normalizes values for storage:
    - empty dictionaries -> None
    - oversized integers (outside int64 range) -> str
    """
    if isinstance(data, dict):
        if not data:
            return None
        return {k: _sanitize_for_storage(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_sanitize_for_storage(item) for item in data]
    if _is_oversized_int(data):
        return str(data)
    return data


def _normalize_mock_storage_backend(raw_value: Any) -> str:
    value = normalize_env_text(raw_value).lower()
    if value in {"", "files", "file", "local", "local_files", "parquet"}:
        return "local_files"
    if value in {"database", "db", "sql", "postgres", "postgresql"}:
        return "database"
    raise ValueError("KAIRYX_MOCK_STORAGE_BACKEND must be 'local_files' or 'database'.")


def _shared_service_cache_key() -> tuple[Any, ...]:
    backend = resolve_warehouse_backend()
    tenant_scope = _tenant_scope_key()
    project_scope = _project_scope_key()
    if backend == "bigquery":
        return (
            backend,
            tenant_scope,
            project_scope,
            normalize_env_text(os.getenv("BIGQUERY_PROJECT_ID", "")),
            normalize_env_text(os.getenv("BIGQUERY_DATASET_ID", "kairyx")),
            normalize_env_text(os.getenv("BIGQUERY_TABLE_NAME", "processed_events")),
            normalize_env_text(os.getenv("BIGQUERY_TABLE_ID", "")),
            normalize_env_text(os.getenv("BIGQUERY_EVENTS_CURATED_TABLE_ID", "")),
            normalize_env_text(os.getenv("BIGQUERY_PLAYER_LATEST_STATE_TABLE_ID", "")),
            normalize_env_text(os.getenv("BIGQUERY_PIPELINE_DEAD_LETTERS_TABLE_ID", "")),
            normalize_env_text(os.getenv("BIGQUERY_PREDICTION_RESULTS_TABLE_ID", "")),
        )
    if backend == "redshift":
        return (
            backend,
            tenant_scope,
            project_scope,
            normalize_env_text(os.getenv("AWS_REGION", "")),
            normalize_env_text(os.getenv("REDSHIFT_WORKGROUP_NAME", "")),
            normalize_env_text(os.getenv("REDSHIFT_DATABASE", "")),
            normalize_env_text(os.getenv("REDSHIFT_SCHEMA", "public")),
        )
    return (
        backend,
        tenant_scope,
        project_scope,
        _normalize_mock_storage_backend(os.getenv("KAIRYX_MOCK_STORAGE_BACKEND", "local_files")),
        normalize_env_text(os.getenv("CONTROL_PLANE_DATABASE_URL", "")),
        normalize_env_text(os.getenv("DATABASE_URL", "")),
        os.getcwd(),
    )


def _tenant_scope_key() -> str:
    context = get_request_context()
    raw_value = context.tenant_id if context is not None else os.getenv("BOOTSTRAP_TENANT_ID", "default")
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw_value or "default").strip()).strip("_").lower()
    return normalized or "default"


def _project_scope_key() -> str:
    context = get_request_context()
    raw_value = context.project_id if context is not None else os.getenv("BOOTSTRAP_PROJECT_ID", "default")
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw_value or "default").strip()).strip("_").lower()
    return normalized or "default"


@lru_cache(maxsize=8)
def _get_shared_service(cache_key: tuple[Any, ...]) -> "BigQueryService":
    return BigQueryService()


def get_shared_bigquery_service() -> "BigQueryService":
    return _get_shared_service(_shared_service_cache_key())


def clear_shared_bigquery_service_cache() -> None:
    _get_shared_service.cache_clear()


class BigQueryService:
    """
    BigQuery service with dual backend support:
    - mock: local pandas/parquet cache (dev/qa)
    - gcp: real Google BigQuery client (prod)
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._query_state_lock = threading.RLock()
        self._active_queries = 0
        self.mode = resolve_warehouse_backend()
        self._mock_storage_backend = self.mode
        if self.mode not in {"mock", "bigquery", "redshift"}:
            raise ValueError("WAREHOUSE_BACKEND must resolve to 'mock', 'bigquery', or 'redshift'.")

        if self.mode == "bigquery":
            self._init_gcp_backend()
            print(f"BigQueryService initialized in GCP mode (table: {self._table_id}).")
        elif self.mode == "redshift":
            self._init_redshift_backend()
            print(f"BigQueryService initialized in Redshift mode (schema: {self._redshift.schema}).")
        else:
            self._init_mock_backend()
            if self._mock_storage_backend == "database":
                print("BigQueryService initialized in MOCK mode (database-backed cache).")
            else:
                print("BigQueryService initialized in MOCK mode (local parquet cache).")

    def _init_gcp_backend(self):
        try:
            from google.cloud import bigquery
        except ImportError as e:
            raise RuntimeError(
                "google-cloud-bigquery is required for DATA_BACKEND_MODE=gcp."
            ) from e

        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        if not project_id:
            raise ValueError("BIGQUERY_PROJECT_ID must be set for DATA_BACKEND_MODE=gcp.")

        tenant_scope = _tenant_scope_key()
        project_scope = _project_scope_key()
        dataset_base = os.getenv("BIGQUERY_DATASET_ID", "kairyx")
        dataset_id = os.getenv("BIGQUERY_DATASET_ID_EFFECTIVE", f"{dataset_base}_{tenant_scope}_{project_scope}")
        table_name = os.getenv("BIGQUERY_TABLE_NAME", "processed_events")
        self._table_id = os.getenv("BIGQUERY_TABLE_ID", f"{project_id}.{dataset_id}.{table_name}")
        self._curated_table_id = os.getenv(
            "BIGQUERY_EVENTS_CURATED_TABLE_ID",
            f"{project_id}.{dataset_id}.events_curated",
        )
        self._player_latest_state_table_id = os.getenv(
            "BIGQUERY_PLAYER_LATEST_STATE_TABLE_ID",
            f"{project_id}.{dataset_id}.player_latest_state",
        )
        self._dead_letter_table_id = os.getenv(
            "BIGQUERY_PIPELINE_DEAD_LETTERS_TABLE_ID",
            f"{project_id}.{dataset_id}.pipeline_dead_letters",
        )
        self._prediction_results_table_id = os.getenv(
            "BIGQUERY_PREDICTION_RESULTS_TABLE_ID",
            f"{project_id}.{dataset_id}.prediction_results",
        )

        self._bigquery = bigquery
        self._client = bigquery.Client(project=project_id)

    def _init_redshift_backend(self):
        self._redshift = RedshiftWarehouseService()
        self._table_id = self._redshift.table_id("events_staging")
        self._curated_table_id = self._redshift.table_id("events_curated")
        self._player_latest_state_table_id = self._redshift.table_id("player_latest_state")
        self._dead_letter_table_id = self._redshift.table_id("pipeline_dead_letters")
        self._prediction_results_table_id = self._redshift.table_id("prediction_results")

    def _init_mock_backend(self):
        self._mock_storage_backend = _normalize_mock_storage_backend(
            os.getenv("KAIRYX_MOCK_STORAGE_BACKEND", "local_files")
        )
        cache_root = Path(".cache") / _tenant_scope_key() / _project_scope_key()
        self._cache_path = str(resolve_runtime_file_path(cache_root / "bigquery_table.parquet", ensure_parent=True))
        self._curated_cache_path = str(resolve_runtime_file_path(cache_root / "events_curated.parquet", ensure_parent=True))
        self._player_latest_state_cache_path = str(resolve_runtime_file_path(cache_root / "player_latest_state.parquet", ensure_parent=True))
        self._dead_letter_cache_path = str(resolve_runtime_file_path(cache_root / "pipeline_dead_letters.parquet", ensure_parent=True))
        self._prediction_results_cache_path = str(resolve_runtime_file_path(cache_root / "prediction_results.parquet", ensure_parent=True))
        if self._mock_storage_backend == "database":
            from app.core.db import init_db

            init_db()
            self._table = pd.DataFrame()
            self._curated_table = pd.DataFrame()
            self._player_latest_state_table = pd.DataFrame()
            self._dead_letter_table = pd.DataFrame()
            self._prediction_results_table = pd.DataFrame()
            return
        self._table = self._load_mock_table(self._cache_path)
        self._curated_table = self._load_mock_table(self._curated_cache_path)
        self._player_latest_state_table = self._load_mock_table(self._player_latest_state_cache_path)
        self._dead_letter_table = self._load_mock_table(self._dead_letter_cache_path)
        self._prediction_results_table = self._load_mock_table(self._prediction_results_cache_path)
        os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)

    def get_mock_state_backend(self) -> str:
        if self.mode == "bigquery":
            return "gcp"
        if self.mode == "redshift":
            return "redshift"
        return self._mock_storage_backend

    def is_mock_state_persistent(self) -> bool:
        if self.mode in {"bigquery", "redshift"}:
            return True
        if self._mock_storage_backend != "database":
            return False

        from app.core.db import is_control_plane_database_persistent

        return is_control_plane_database_persistent()

    def _uses_database_mock_storage(self) -> bool:
        return self.mode == "mock" and self._mock_storage_backend == "database"

    @staticmethod
    def _mock_storage_model():
        from app.infrastructure.db_models import MockWarehouseRowModel

        return MockWarehouseRowModel

    def _load_mock_rows_from_database(self, target: str) -> List[Dict[str, Any]]:
        from app.core.db import session_scope

        model = self._mock_storage_model()
        with session_scope() as session:
            rows = (
                session.query(model)
                .filter(model.target_name == target)
                .order_by(model.id.asc())
                .all()
            )

        parsed_rows: List[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row.payload_json)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                parsed_rows.append(payload)
        return parsed_rows

    def _append_mock_rows_to_database(self, target: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        from app.core.db import session_scope

        model = self._mock_storage_model()
        with session_scope() as session:
            session.add_all(
                [
                    model(
                        target_name=target,
                        payload_json=json.dumps(_sanitize_for_storage(dict(row)), default=str),
                    )
                    for row in rows
                ]
            )

    def _replace_mock_rows_in_database(self, target: str, rows: List[Dict[str, Any]]) -> None:
        from app.core.db import session_scope

        model = self._mock_storage_model()
        with session_scope() as session:
            session.query(model).filter(model.target_name == target).delete(synchronize_session=False)
            if rows:
                session.add_all(
                    [
                        model(
                            target_name=target,
                            payload_json=json.dumps(_sanitize_for_storage(dict(row)), default=str),
                        )
                        for row in rows
                    ]
                )

    def _get_mock_table(self, target: str) -> pd.DataFrame:
        if self.mode != "mock":
            raise RuntimeError("Mock table access is only available in mock mode.")

        if self._uses_database_mock_storage():
            return pd.DataFrame(self._load_mock_rows_from_database(target))

        with self._lock:
            table = getattr(self, self._target_meta(target)["table_attr"])
            if table is None or table.empty:
                return pd.DataFrame()
            return table.copy()

    def _load_mock_table(self, cache_path: str) -> pd.DataFrame:
        if os.path.exists(cache_path):
            print(f"Loading BigQuery cache from {cache_path}")
            table = pd.read_parquet(cache_path)
            return self._restore_complex_columns_from_parquet(table)
        return pd.DataFrame()

    def _prepare_for_parquet(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Converts dict/list values in object columns into JSON strings so Parquet
        can store mixed nested types without schema-conversion failures.
        """
        table_to_persist = table.copy()
        for column in table_to_persist.columns:
            series = table_to_persist[column]
            if series.dtype != "object":
                continue

            non_null = series[series.notna()]
            if non_null.empty:
                continue

            has_complex_values = non_null.map(lambda v: isinstance(v, (dict, list))).any()
            if not has_complex_values:
                continue

            table_to_persist[column] = series.map(
                lambda value: json.dumps(value) if isinstance(value, (dict, list)) else value
            )

        return table_to_persist

    def _restore_complex_columns_from_parquet(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Restores JSON-serialized dict/list values after loading Parquet data.
        """
        restored = table.copy()
        for column in restored.columns:
            series = restored[column]
            if series.dtype != "object":
                continue

            non_null = series[series.notna()]
            if non_null.empty:
                continue

            sample = non_null.iloc[0]
            if not isinstance(sample, str):
                continue

            if not (sample.startswith("{") or sample.startswith("[")):
                continue

            def _maybe_parse_json(value: Any) -> Any:
                if not isinstance(value, str):
                    return value
                if not (value.startswith("{") or value.startswith("[")):
                    return value
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    return value

            restored[column] = series.map(_maybe_parse_json)
        return restored

    def _coerce_oversized_integer_columns(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Parquet cannot store integers outside int64 range.
        If a scalar-integer column contains oversized values, cast the full
        column to string to keep a consistent, writable schema.
        """
        coerced = table.copy()
        for column in coerced.columns:
            series = coerced[column]
            non_null = series[series.notna()]
            if non_null.empty:
                continue

            if not non_null.map(_is_int_like_scalar).all():
                continue

            if non_null.map(_is_oversized_int).any():
                coerced[column] = series.map(
                    lambda value: str(value) if _is_int_like_scalar(value) else value
                )
        return coerced

    def _persist_mock_table(self, table: pd.DataFrame, cache_path: str):
        if table.empty:
            if os.path.exists(cache_path):
                os.remove(cache_path)
            return
        table_to_persist = self._prepare_for_parquet(table)
        table_to_persist.to_parquet(cache_path)

    def _target_meta(self, target: str) -> Dict[str, str]:
        mapping = {
            "events_staging": {
                "table_attr": "_table",
                "cache_path": getattr(self, "_cache_path", ""),
                "table_id": getattr(self, "_table_id", ""),
            },
            "events_curated": {
                "table_attr": "_curated_table",
                "cache_path": getattr(self, "_curated_cache_path", ""),
                "table_id": getattr(self, "_curated_table_id", ""),
            },
            "player_latest_state": {
                "table_attr": "_player_latest_state_table",
                "cache_path": getattr(self, "_player_latest_state_cache_path", ""),
                "table_id": getattr(self, "_player_latest_state_table_id", ""),
            },
            "pipeline_dead_letters": {
                "table_attr": "_dead_letter_table",
                "cache_path": getattr(self, "_dead_letter_cache_path", ""),
                "table_id": getattr(self, "_dead_letter_table_id", ""),
            },
            "prediction_results": {
                "table_attr": "_prediction_results_table",
                "cache_path": getattr(self, "_prediction_results_cache_path", ""),
                "table_id": getattr(self, "_prediction_results_table_id", ""),
            },
        }
        if target not in mapping:
            raise ValueError(f"Unknown BigQueryService target '{target}'.")
        return mapping[target]

    def get_v1_table_aliases(self) -> Dict[str, str]:
        return {
            "standardized": "events_staging",
            "fact_events_unified": "events_curated",
            "mart_user_daily": "player_latest_state",
            "prediction_results": "prediction_results",
            "pipeline_dead_letters": "pipeline_dead_letters",
        }

    def _default_columns_for_alias(self, alias: str) -> List[str]:
        mapping = {
            "standardized": [
                "job_id", "source", "player_id", "canonical_user_id", "event_type", "event_time",
                "event_properties", "user_properties", "data_quality_flags",
            ],
            "fact_events_unified": [
                "job_id", "source", "player_id", "canonical_user_id", "event_type", "event_time",
                "event_properties", "user_properties", "data_quality_flags",
            ],
            "mart_user_daily": [
                "player_id", "canonical_user_id", "email", "sessions_7d", "sessions_30d",
                "lifetime_revenue_usd", "days_since_last_seen", "last_campaign", "last_media_source",
            ],
            "prediction_results": [
                "prediction_job_id", "user_id", "canonical_user_id", "email", "churn_state",
                "predicted_churn_risk", "prediction_source", "suggested_action", "completed_at",
                "baseline_churn_score", "model_version", "score_timestamp", "eligibility_reason",
                "effective_local_model_version", "effective_local_model_state",
                "recommended_template_id", "recommended_variant", "policy_snapshot_id",
            ],
            "pipeline_dead_letters": [
                "job_id", "player_id", "event_type", "event_time", "rejection_reason",
            ],
        }
        return mapping.get(alias, ["id"])

    @staticmethod
    def _top20_field_specs() -> List[Dict[str, Any]]:
        return [
            {"field": "player_id", "expected_type": "string"},
            {"field": "internal_account_id", "expected_type": "string"},
            {"field": "game_uid", "expected_type": "string"},
            {"field": "login_user_id", "expected_type": "string"},
            {"field": "anonymous_id", "expected_type": "string"},
            {"field": "device_id", "expected_type": "string"},
            {"field": "email_hash", "expected_type": "string"},
            {"field": "phone_hash", "expected_type": "string"},
            {"field": "event_type", "expected_type": "string"},
            {"field": "event_time", "expected_type": "datetime"},
            {"field": "source_event_id", "expected_type": "string"},
            {"field": "session_id", "expected_type": "string"},
            {"field": "app_version", "expected_type": "string"},
            {"field": "platform", "expected_type": "string"},
            {"field": "campaign", "expected_type": "string"},
            {"field": "adset", "expected_type": "string"},
            {"field": "media_source", "expected_type": "string"},
            {"field": "channel", "expected_type": "string"},
            {"field": "revenue_usd", "expected_type": "number"},
            {"field": "country", "expected_type": "string"},
        ]

    def _load_rows_to_gcp_table(
        self,
        rows: List[Dict[str, Any]],
        table_id: str,
        write_disposition: Any,
    ):
        if rows:
            job_config = self._bigquery.LoadJobConfig(
                source_format=self._bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=write_disposition,
                create_disposition=self._bigquery.CreateDisposition.CREATE_IF_NEEDED,
                autodetect=True,
                ignore_unknown_values=True,
            )
            load_job = self._client.load_table_from_json(rows, table_id, job_config=job_config)
            load_job.result()
            return

        try:
            self._client.query(f"TRUNCATE TABLE `{table_id}`").result()
        except Exception:
            return

    def _append_rows(self, rows: List[Dict[str, Any]], target: str = "events_staging"):
        with self._lock:
            self._append_rows_unlocked(rows, target=target)

    def _append_rows_unlocked(self, rows: List[Dict[str, Any]], target: str = "events_staging"):
        prepared_events = []
        for event in rows:
            event_copy = _sanitize_for_storage(dict(event))
            prepared_events.append(_sanitize_for_storage(event_copy))

        if not prepared_events:
            return

        meta = self._target_meta(target)
        if self.mode == "bigquery":
            self._load_rows_to_gcp_table(
                prepared_events,
                meta["table_id"],
                self._bigquery.WriteDisposition.WRITE_APPEND,
            )
            print(f"Wrote {len(prepared_events)} rows to BigQuery table {meta['table_id']}.")
            return
        if self.mode == "redshift":
            self._redshift.append_rows(target, prepared_events)
            print(f"Wrote {len(prepared_events)} rows to Redshift table {meta['table_id']}.")
            return

        if self._uses_database_mock_storage():
            self._append_mock_rows_to_database(target, prepared_events)
            print(f"Wrote {len(prepared_events)} rows to database-backed mock target '{target}'.")
            return

        table_attr = meta["table_attr"]
        cache_path = meta["cache_path"]
        current_table = getattr(self, table_attr)
        new_data_df = pd.DataFrame(prepared_events)
        if current_table.empty:
            current_table = new_data_df
        else:
            current_table = pd.concat([current_table, new_data_df], ignore_index=True)

        print("Sanitizing DataFrame for Parquet compatibility...")
        if hasattr(current_table, "map"):
            current_table = current_table.map(_sanitize_for_storage)
        else:
            current_table = current_table.applymap(_sanitize_for_storage)
        current_table = self._coerce_oversized_integer_columns(current_table)
        setattr(self, table_attr, current_table)

        print(f"Wrote {len(new_data_df)} rows to local BigQuery mock target '{target}'. Table now has {len(current_table)} total rows.")
        self._persist_mock_table(current_table, cache_path)

    def _replace_rows(self, rows: List[Dict[str, Any]], target: str):
        with self._lock:
            self._replace_rows_unlocked(rows, target=target)

    def _replace_rows_unlocked(self, rows: List[Dict[str, Any]], target: str):
        prepared_rows = [_sanitize_for_storage(dict(row)) for row in rows]
        meta = self._target_meta(target)

        if self.mode == "bigquery":
            self._load_rows_to_gcp_table(
                prepared_rows,
                meta["table_id"],
                self._bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            return
        if self.mode == "redshift":
            self._redshift.replace_rows(target, prepared_rows)
            return

        if self._uses_database_mock_storage():
            self._replace_mock_rows_in_database(target, prepared_rows)
            return

        table = pd.DataFrame(prepared_rows)
        if not table.empty:
            if hasattr(table, "map"):
                table = table.map(_sanitize_for_storage)
            else:
                table = table.applymap(_sanitize_for_storage)
            table = self._coerce_oversized_integer_columns(table)
        setattr(self, meta["table_attr"], table)
        self._persist_mock_table(table, meta["cache_path"])

    def _get_local_rows(self, target: str) -> List[Dict[str, Any]]:
        with self._lock:
            if self._uses_database_mock_storage():
                return self._load_mock_rows_from_database(target)
            table = getattr(self, self._target_meta(target)["table_attr"])
            if table.empty:
                return []
            return table.to_dict(orient="records")

    def _filter_table_by_job(self, table: pd.DataFrame, job_id: Optional[str] = None) -> pd.DataFrame:
        if table.empty or not job_id:
            return table

        match_value = str(job_id)
        masks = []
        for column in ("job_id", "job_identifier", "last_job_id"):
            if column not in table.columns:
                continue
            masks.append(table[column].map(lambda value: str(value) == match_value if pd.notna(value) else False))

        if not masks:
            return table

        combined_mask = masks[0]
        for mask in masks[1:]:
            combined_mask = combined_mask | mask
        return table[combined_mask].copy()

    def _get_local_events_for_identity(
        self,
        player_id: Any,
        table: Optional[pd.DataFrame] = None,
        job_id: Optional[str] = None,
        target: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        if target:
            current_table = self._get_mock_table(target)
        else:
            current_table = table if table is not None else self._get_mock_table("events_staging")
        if current_table.empty:
            return None
        current_table = self._filter_table_by_job(current_table, job_id=job_id)
        if current_table.empty:
            return None

        match_value = str(player_id)
        candidate_frames = []
        for column in ("player_id", "canonical_user_id"):
            if column not in current_table.columns:
                continue
            mask = current_table[column].map(lambda value: str(value) == match_value if pd.notna(value) else False)
            matched = current_table[mask]
            if not matched.empty:
                candidate_frames.append(matched)

        if not candidate_frames:
            return None

        player_df = pd.concat(candidate_frames, ignore_index=True)
        row_signature = player_df.apply(
            lambda row: json.dumps(_sanitize_for_storage(row.to_dict()), sort_keys=True, default=str),
            axis=1,
        )
        player_df = player_df.loc[~row_signature.duplicated()].copy()
        return player_df.copy() if not player_df.empty else None

    def _query_rows_by_identity_gcp(
        self,
        table_id: str,
        player_id: Any,
        limit: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query = f"""
            SELECT *
            FROM `{table_id}`
            WHERE (
                CAST(player_id AS STRING) = @player_id
                OR CAST(canonical_user_id AS STRING) = @player_id
            )
        """
        if job_id:
            query += """
               AND (
                    CAST(job_id AS STRING) = @job_id
                    OR CAST(job_identifier AS STRING) = @job_id
               )
            """
        if limit is not None:
            query += f"\nLIMIT {max(1, int(limit))}"
        query_parameters = [
            self._bigquery.ScalarQueryParameter("player_id", "STRING", str(player_id))
        ]
        if job_id:
            query_parameters.append(self._bigquery.ScalarQueryParameter("job_id", "STRING", str(job_id)))
        job_config = self._bigquery.QueryJobConfig(query_parameters=query_parameters)
        try:
            return [dict(row.items()) for row in self._client.query(query, job_config=job_config).result()]
        except Exception:
            return []

    def _load_all_rows_from_target(self, target: str) -> List[Dict[str, Any]]:
        if self.mode == "bigquery":
            table_id = self._target_meta(target)["table_id"]
            try:
                rows = [dict(row.items()) for row in self._client.query(f"SELECT * FROM `{table_id}`").result()]
                return rows
            except Exception:
                return []
        if self.mode == "redshift":
            try:
                return self._redshift.fetch_payload_rows(target)
            except Exception:
                return []
        return self._get_local_rows(target)

    def get_rows_for_alias(self, alias: str) -> List[Dict[str, Any]]:
        resolved_alias = str(alias or "").strip()
        target = self.get_v1_table_aliases().get(resolved_alias, resolved_alias)
        return self._load_all_rows_from_target(target)

    @staticmethod
    def _lookup_row_value(row: Dict[str, Any], field: str) -> Any:
        if field in row and row.get(field) not in (None, ""):
            return row.get(field)
        for container_name in ("event_properties", "user_properties"):
            container = row.get(container_name)
            if isinstance(container, dict) and container.get(field) not in (None, ""):
                return container.get(field)
        return None

    @classmethod
    def _expected_type_matches(cls, value: Any, expected_type: str) -> bool:
        if value in (None, "", []):
            return True
        kind = str(expected_type or "string")
        if kind == "string":
            return isinstance(value, str)
        if kind == "number":
            try:
                float(value)
                return True
            except (TypeError, ValueError):
                return False
        if kind == "datetime":
            try:
                datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                return True
            except ValueError:
                return False
        return True

    def _resolve_canonical_user_id(self, row: Dict[str, Any]) -> tuple[str | None, str]:
        direct = self._normalize_identity_value(row.get("canonical_user_id"))
        if direct:
            return direct, "canonical_user_id"

        ordered_fields = (
            ("internal_account_id", "internal_account_id"),
            ("game_uid", "game_uid"),
            ("login_user_id", "login_user_id"),
            ("player_id", "player_id"),
            ("device_id", "device_id"),
            ("email_hash", "email_hash"),
            ("phone_hash", "phone_hash"),
            ("anonymous_id", "anonymous_id"),
            ("source_user_id", "source_user_id"),
        )
        for field, method in ordered_fields:
            value = self._normalize_identity_value(self._lookup_row_value(row, field))
            if value:
                prefix = "uid" if field in {"internal_account_id", "game_uid", "login_user_id", "player_id"} else field
                return f"{prefix}:{value}" if ":" not in value else value, method

        source = self._normalize_identity_value(row.get("source")) or "unknown"
        fallback = self._normalize_identity_value(self._lookup_row_value(row, "source_event_id")) or self._normalize_identity_value(self._lookup_row_value(row, "event_type"))
        if fallback:
            return f"{source}:{fallback}", "source_fallback"
        return None, "unresolved"

    @staticmethod
    def _source_priority(source: str, field: str) -> int:
        value = str(source or "").strip().lower()
        identity_priority = {"game_backend": 0, "internal": 0, "game": 0, "server": 0, "analytics_sdk": 1, "sdk": 1, "amplitude": 1, "mixpanel": 1, "adjust": 2, "mmp": 2}
        attribution_priority = {"analytics_sdk": 0, "sdk": 0, "amplitude": 0, "mixpanel": 0, "adjust": 1, "mmp": 1, "game_backend": 2, "internal": 2}
        mapping = attribution_priority if field in {"campaign", "adset", "media_source", "channel"} else identity_priority
        return int(mapping.get(value, 9))

    def _normalize_event_for_curation(self, row: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(row)
        resolved_canonical, method = self._resolve_canonical_user_id(payload)
        if resolved_canonical:
            payload["canonical_user_id"] = resolved_canonical
        payload.setdefault("identity_resolution_method", method)
        decisions = dict(payload.get("source_of_truth_decision") or {})
        for field in ("campaign", "adset", "media_source", "channel"):
            value = self._lookup_row_value(payload, field)
            if value in (None, "", []):
                continue
            decisions[field] = {
                "selected_value": value,
                "selected_source": payload.get("source"),
                "priority": self._source_priority(str(payload.get("source") or ""), field),
                "rule": "attribution_source_priority" if field in {"campaign", "adset", "media_source", "channel"} else "identity_source_priority",
            }
        payload["source_of_truth_decision"] = decisions
        return payload

    def _referenced_aliases(self, sql: str) -> List[str]:
        lowered = str(sql or "").lower()
        aliases = []
        for alias in self.get_v1_table_aliases():
            if re.search(rf"\b{re.escape(alias.lower())}\b", lowered):
                aliases.append(alias)
        return aliases or ["standardized"]

    def _estimate_scan_rows(self, sql: str) -> int:
        return sum(len(self.get_rows_for_alias(alias)) for alias in self._referenced_aliases(sql))

    def top20_field_quality(self, *, job_id: Optional[str] = None, alias: str = "standardized") -> Dict[str, Any]:
        rows = self.get_rows_for_alias(alias)
        if job_id:
            rows = [row for row in rows if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)]
        total_rows = len(rows)
        fields: Dict[str, Any] = {}
        for spec in self._top20_field_specs():
            field = spec["field"]
            samples: List[str] = []
            non_null = 0
            mismatches = 0
            impacted_rows = 0
            for row in rows:
                value = self._lookup_row_value(row, field)
                if value in (None, "", []):
                    impacted_rows += 1
                    continue
                non_null += 1
                if len(samples) < 3 and str(value) not in samples:
                    samples.append(str(value))
                if not self._expected_type_matches(value, spec["expected_type"]):
                    mismatches += 1
                    impacted_rows += 1
            fields[field] = {
                "coverage": round((non_null / total_rows * 100.0), 2) if total_rows else 0.0,
                "null_rate": round(((total_rows - non_null) / total_rows * 100.0), 2) if total_rows else 0.0,
                "type_mismatch_rate": round((mismatches / total_rows * 100.0), 2) if total_rows else 0.0,
                "sample_values": samples,
                "impacted_row_count": impacted_rows,
                "expected_type": spec["expected_type"],
            }
        return {
            "rows_evaluated": total_rows,
            "alias": alias,
            "fields": fields,
        }

    @staticmethod
    def _schema_field_type(value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, (int, float)):
            return "number"
        if isinstance(value, dict):
            return "object"
        if isinstance(value, list):
            return "array"
        try:
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return "datetime"
        except ValueError:
            return "string"

    def get_schema_contract(self, alias: str, *, job_id: Optional[str] = None) -> Dict[str, Any]:
        resolved_alias = str(alias or "").strip()
        aliases = self.get_v1_table_aliases()
        targets = set(aliases.keys()) | set(aliases.values())
        if resolved_alias not in targets:
            raise KeyError(resolved_alias)
        canonical_alias = next((name for name, target in aliases.items() if resolved_alias in {name, target}), resolved_alias)
        rows = self.get_rows_for_alias(canonical_alias)
        if job_id:
            rows = [row for row in rows if self._row_matches_job(row, job_id)]
        expected_fields = self._default_columns_for_alias(canonical_alias)
        observed_fields: set[str] = set()
        observed_types: Dict[str, str] = {}
        schema_versions: Counter[str] = Counter()
        for row in rows:
            for field, value in row.items():
                observed_fields.add(str(field))
                observed_types.setdefault(str(field), self._schema_field_type(value))
            schema_version = str(row.get("schema_version") or "").strip()
            if schema_version:
                schema_versions[schema_version] += 1
        missing_required_fields = sorted(field for field in expected_fields if field not in observed_fields)
        extra_fields = sorted(field for field in observed_fields if field not in expected_fields)
        if not rows:
            compatibility_status = "no_data"
        elif missing_required_fields:
            compatibility_status = "missing_required_fields"
        elif extra_fields:
            compatibility_status = "drifted"
        else:
            compatibility_status = "compatible"
        return {
            "alias": canonical_alias,
            "table_name": aliases.get(canonical_alias, canonical_alias),
            "job_id": job_id,
            "schema_version": schema_versions.most_common(1)[0][0] if schema_versions else "v1",
            "required_fields": expected_fields,
            "observed_fields": sorted(observed_fields),
            "observed_field_types": observed_types,
            "missing_required_fields": missing_required_fields,
            "extra_fields": extra_fields,
            "compatibility_status": compatibility_status,
            "row_count": len(rows),
        }

    def list_schema_contracts(self, *, job_id: Optional[str] = None) -> List[Dict[str, Any]]:
        items = []
        for alias in self.get_v1_table_aliases():
            items.append(self.get_schema_contract(alias, job_id=job_id))
        return items

    def get_rejected_event_explanations(self, *, job_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        items = self.get_pipeline_dead_letters(job_id=job_id, limit=limit)
        explanations: List[Dict[str, Any]] = []
        for item in items:
            normalized_event = dict(item.get("normalized_event") or {})
            flags = list(normalized_event.get("data_quality_flags") or item.get("data_quality_flags") or [])
            reason = item.get("rejection_reason") or ", ".join(flags) or "unknown_rejection"
            explanations.append(
                {
                    "job_id": item.get("job_id") or item.get("job_identifier"),
                    "event_fingerprint": normalized_event.get("event_fingerprint"),
                    "reason": reason,
                    "flags": flags,
                    "normalized_event": normalized_event,
                }
            )
        return explanations

    @staticmethod
    def _row_matches_job(row: Dict[str, Any], job_id: Optional[str]) -> bool:
        if not job_id:
            return True
        match_value = str(job_id)
        return str(row.get("job_id") or row.get("job_identifier") or "") == match_value

    @staticmethod
    def _parse_datetime_value(value: Any) -> datetime | None:
        if value in (None, "", []):
            return None
        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            return None

    def _row_identity_values(self, row: Dict[str, Any]) -> List[str]:
        values: List[str] = []
        for field in ("player_id", "canonical_user_id"):
            normalized = self._normalize_identity_value(row.get(field))
            if normalized and normalized not in values:
                values.append(normalized)
        return values

    def _preferred_player_identity(self, row: Dict[str, Any]) -> str | None:
        values = self._row_identity_values(row)
        return values[0] if values else None

    def get_import_roster_player_ids(self, job_id: str) -> List[Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return []

        if self.mode == "bigquery":
            query = f"""
                SELECT CAST(player_id AS STRING) AS player_id, CAST(canonical_user_id AS STRING) AS canonical_user_id
                FROM `{self._curated_table_id}`
                WHERE (
                    CAST(job_id AS STRING) = @job_id
                    OR CAST(job_identifier AS STRING) = @job_id
                )
                UNION ALL
                SELECT CAST(player_id AS STRING) AS player_id, CAST(canonical_user_id AS STRING) AS canonical_user_id
                FROM `{self._table_id}`
                WHERE (
                    CAST(job_id AS STRING) = @job_id
                    OR CAST(job_identifier AS STRING) = @job_id
                )
            """
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("job_id", "STRING", normalized_job_id)
                ]
            )
            try:
                rows = [dict(row.items()) for row in self._client.query(query, job_config=job_config).result()]
            except Exception:
                rows = []
        elif self.mode == "redshift":
            rows = [
                row
                for row in (self._load_all_rows_from_target("events_curated") + self._load_all_rows_from_target("events_staging"))
                if str(row.get("job_id") or row.get("job_identifier") or "") == normalized_job_id
            ]
        else:
            rows = []
            for table in (self._curated_table, self._table):
                filtered_table = self._filter_table_by_job(table, job_id=normalized_job_id)
                if filtered_table.empty:
                    continue
                rows.extend(filtered_table.to_dict(orient="records"))

        deduped: List[str] = []
        seen_identity_keys: set[str] = set()
        for row in rows:
            identity_key = self._normalize_identity_value(row.get("canonical_user_id")) or self._normalize_identity_value(row.get("player_id"))
            if identity_key is None or identity_key in seen_identity_keys:
                continue
            preferred_identity = self._preferred_player_identity(row)
            if preferred_identity is None:
                continue
            seen_identity_keys.add(identity_key)
            deduped.append(preferred_identity)
        return deduped

    def get_pipeline_lag_summary(self, *, job_id: Optional[str] = None) -> Dict[str, Any]:
        standardized_rows = [row for row in self.get_rows_for_alias("standardized") if self._row_matches_job(row, job_id)]
        curated_rows = [row for row in self.get_rows_for_alias("fact_events_unified") if self._row_matches_job(row, job_id)]
        latest_state_rows = self.get_rows_for_alias("mart_user_daily")
        if job_id:
            roster_ids = set(self.get_import_roster_player_ids(job_id))
            latest_state_rows = [
                row
                for row in latest_state_rows
                if any(identity in roster_ids for identity in self._row_identity_values(row))
            ]
        dead_letter_rows = [row for row in self.get_pipeline_dead_letters(job_id=job_id, limit=5000)]

        def _latest(rows: List[Dict[str, Any]], *fields: str) -> str | None:
            latest_value: datetime | None = None
            for row in rows:
                for field in fields:
                    parsed = self._parse_datetime_value(row.get(field))
                    if parsed is not None and (latest_value is None or parsed > latest_value):
                        latest_value = parsed
            return latest_value.isoformat() if latest_value is not None else None

        latest_standardized_event_time = _latest(standardized_rows, "event_time")
        latest_curated_event_time = _latest(curated_rows, "event_time")
        latest_player_state_time = _latest(latest_state_rows, "last_seen_at", "updated_at", "event_time")
        latest_dead_letter_time = _latest(dead_letter_rows, "event_time", "ingested_at", "created_at")

        standardized_dt = self._parse_datetime_value(latest_standardized_event_time)
        curated_dt = self._parse_datetime_value(latest_curated_event_time)
        latest_state_dt = self._parse_datetime_value(latest_player_state_time)

        staging_to_curated_lag_seconds = 0
        if standardized_dt is not None and curated_dt is not None and standardized_dt > curated_dt:
            staging_to_curated_lag_seconds = int((standardized_dt - curated_dt).total_seconds())

        curated_to_latest_state_lag_seconds = 0
        if curated_dt is not None and latest_state_dt is not None and curated_dt > latest_state_dt:
            curated_to_latest_state_lag_seconds = int((curated_dt - latest_state_dt).total_seconds())

        return {
            "job_id": job_id,
            "table_counts": {
                "standardized_rows": len(standardized_rows),
                "curated_rows": len(curated_rows),
                "player_latest_state_rows": len(latest_state_rows),
                "dead_letter_rows": len(dead_letter_rows),
            },
            "freshness": {
                "latest_standardized_event_time": latest_standardized_event_time,
                "latest_curated_event_time": latest_curated_event_time,
                "latest_player_latest_state_at": latest_player_state_time,
                "latest_dead_letter_at": latest_dead_letter_time,
                "staging_to_curated_lag_seconds": staging_to_curated_lag_seconds,
                "curated_to_latest_state_lag_seconds": curated_to_latest_state_lag_seconds,
            },
        }

    def get_dead_letter_summary(self, *, job_id: Optional[str] = None) -> Dict[str, Any]:
        explanations = self.get_rejected_event_explanations(job_id=job_id, limit=5000)
        reason_counts = Counter(str(item.get("reason") or "unknown_rejection") for item in explanations)
        top_reasons = [
            {"reason": reason, "count": count}
            for reason, count in reason_counts.most_common(5)
        ]
        return {
            "job_id": job_id,
            "count": len(explanations),
            "top_reasons": top_reasons,
            "examples": explanations[:10],
        }

    def get_identity_links(self, *, job_id: Optional[str] = None, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self.get_rows_for_alias("fact_events_unified")
        if job_id:
            rows = [row for row in rows if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)]
        links: List[Dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for row in rows:
            canonical, method = self._resolve_canonical_user_id(row)
            if canonical is None:
                continue
            source = str(row.get("source") or "unknown")
            source_user_id = self._normalize_identity_value(
                self._lookup_row_value(row, "source_user_id")
                or self._lookup_row_value(row, "player_id")
                or self._lookup_row_value(row, "login_user_id")
                or self._lookup_row_value(row, "anonymous_id")
            )
            if source_user_id is None:
                continue
            key = (source, source_user_id, canonical)
            if key in seen:
                continue
            seen.add(key)
            event_time = row.get("event_time")
            links.append(
                {
                    "source": source,
                    "source_user_id": source_user_id,
                    "canonical_user_id": canonical,
                    "method": method,
                    "confidence": "high" if method in {"canonical_user_id", "internal_account_id", "game_uid", "login_user_id", "player_id"} else "medium",
                    "first_seen_at": event_time,
                    "last_seen_at": event_time,
                }
            )
            if len(links) >= max(1, int(limit)):
                break
        return links

    def get_field_conflicts(self, *, job_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self.get_rows_for_alias("fact_events_unified")
        if job_id:
            rows = [row for row in rows if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)]
        grouped: Dict[str, Dict[str, Dict[str, set[str]]]] = {}
        for row in rows:
            canonical, _ = self._resolve_canonical_user_id(row)
            identity_key = canonical or self._normalize_identity_value(self._lookup_row_value(row, "player_id")) or f"row:{len(grouped)+1}"
            bucket = grouped.setdefault(identity_key, {})
            for field in ("campaign", "adset", "media_source", "channel"):
                value = self._normalize_identity_value(self._lookup_row_value(row, field))
                if value is None:
                    continue
                field_bucket = bucket.setdefault(field, {})
                field_bucket.setdefault(value, set()).add(str(row.get("source") or "unknown"))

        conflicts: List[Dict[str, Any]] = []
        for identity_key, field_map in grouped.items():
            for field, value_map in field_map.items():
                if len(value_map) <= 1:
                    continue
                selected_value = sorted(
                    value_map.items(),
                    key=lambda item: (min(self._source_priority(source, field) for source in item[1]), item[0]),
                )[0][0]
                selected_sources = sorted(value_map[selected_value])
                conflicts.append(
                    {
                        "identity_key": identity_key,
                        "field": field,
                        "selected_value": selected_value,
                        "selected_source": selected_sources[0] if selected_sources else None,
                        "rule": "attribution_source_priority",
                        "candidates": {value: sorted(sources) for value, sources in value_map.items()},
                        "explain": f"Selected '{selected_value}' using source priority for {field}.",
                    }
                )
                if len(conflicts) >= max(1, int(limit)):
                    return conflicts
        return conflicts

    def run_readonly_query(
        self,
        sql: str,
        *,
        limit: int = 50,
        timeout_seconds: int = 30,
        max_scan_rows: int = 50000,
        max_concurrency: int = 4,
    ) -> Dict[str, Any]:
        query = str(sql or "").strip()
        if not query:
            raise ValueError("SQL query is required.")
        lowered = query.lower()
        if not (lowered.startswith("select") or lowered.startswith("with")):
            raise ValueError("Only SELECT queries are allowed.")
        forbidden = (" insert ", " update ", " delete ", " drop ", " create ", " alter ", " truncate ", " merge ")
        padded = f" {lowered} "
        if any(token in padded for token in forbidden):
            raise ValueError("Only read-only SQL is allowed.")
        if ";" in query.rstrip(";"):
            raise ValueError("Multiple SQL statements are not allowed.")

        estimated_scan_rows = self._estimate_scan_rows(query)
        if estimated_scan_rows > max(1, int(max_scan_rows)):
            raise ValueError(f"Estimated scan rows {estimated_scan_rows} exceed limit {int(max_scan_rows)}.")

        with self._query_state_lock:
            if self._active_queries >= max(1, int(max_concurrency)):
                raise ValueError("Concurrent query limit exceeded.")
            self._active_queries += 1

        try:
            resolved_query = query
            aliases = self.get_v1_table_aliases()
            if self.mode == "bigquery":
                for alias, target in aliases.items():
                    table_id = self._target_meta(target)["table_id"]
                    resolved_query = re.sub(rf"\b{re.escape(alias)}\b", f"`{table_id}`", resolved_query)
                job_config = self._bigquery.QueryJobConfig()
                job = self._client.query(resolved_query, job_config=job_config, timeout=max(1, int(timeout_seconds)))
                rows = [dict(row.items()) for row in job.result(max_results=max(1, int(limit)))]
                return {
                    "sql": query,
                    "resolved_sql": resolved_query,
                    "aliases": aliases,
                    "rows": rows,
                    "row_count": len(rows),
                    "truncated": len(rows) >= max(1, int(limit)),
                    "estimated_scan_rows": estimated_scan_rows,
                    "timeout_seconds": int(timeout_seconds),
                    "scan_limit_rows": int(max_scan_rows),
                }
            if self.mode == "redshift":
                for alias, target in aliases.items():
                    table_id = self._target_meta(target)["table_id"]
                    resolved_query = re.sub(rf"\b{re.escape(alias)}\b", table_id, resolved_query)
                preview_query = resolved_query
                if " limit " not in lowered:
                    preview_query = f"{resolved_query.rstrip(';')} LIMIT {max(1, int(limit))}"
                rows = self._redshift._run_statement(preview_query, timeout_seconds=max(1, int(timeout_seconds)), fetch=True)
                return {
                    "sql": query,
                    "resolved_sql": preview_query,
                    "aliases": aliases,
                    "rows": rows,
                    "row_count": len(rows),
                    "truncated": len(rows) >= max(1, int(limit)),
                    "estimated_scan_rows": estimated_scan_rows,
                    "timeout_seconds": int(timeout_seconds),
                    "scan_limit_rows": int(max_scan_rows),
                }

            connection = sqlite3.connect(":memory:")
            try:
                for alias, target in aliases.items():
                    rows = self._load_all_rows_from_target(target)
                    frame = pd.DataFrame(rows)
                    if frame.empty:
                        frame = pd.DataFrame(columns=self._default_columns_for_alias(alias))
                    else:
                        for column in frame.columns:
                            if frame[column].dtype != "object":
                                continue
                            frame[column] = frame[column].map(
                                lambda value: json.dumps(value) if isinstance(value, (dict, list)) else value
                            )
                    frame.to_sql(alias, connection, if_exists="replace", index=False)
                preview_query = resolved_query
                if " limit " not in lowered:
                    preview_query = f"{resolved_query.rstrip(';')} LIMIT {max(1, int(limit))}"
                rows = pd.read_sql_query(preview_query, connection).to_dict(orient="records")
                return {
                    "sql": query,
                    "resolved_sql": preview_query,
                    "aliases": aliases,
                    "rows": rows,
                    "row_count": len(rows),
                    "truncated": len(rows) >= max(1, int(limit)),
                    "estimated_scan_rows": estimated_scan_rows,
                    "timeout_seconds": int(timeout_seconds),
                    "scan_limit_rows": int(max_scan_rows),
                }
            finally:
                connection.close()
        finally:
            with self._query_state_lock:
                self._active_queries = max(0, self._active_queries - 1)

    @staticmethod
    def _is_missing_key_part(value: Any) -> bool:
        if value is None:
            return True
        try:
            if pd.isna(value):
                return True
        except Exception:
            pass
        if isinstance(value, str) and value.strip() in {"", "None", "nan", "NaN"}:
            return True
        return False

    def _dedupe_events(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        dedupe_map: Dict[tuple, Dict[str, Any]] = {}
        for row in rows:
            event = dict(row)
            source = str(event.get("source", "unknown"))
            job_scope = str(event.get("job_identifier") or event.get("job_id") or "unknown_job")
            source_event_id = event.get("source_event_id")
            event_fingerprint = event.get("event_fingerprint")
            key = (
                "srcid",
                job_scope,
                source,
                str(source_event_id),
            ) if not self._is_missing_key_part(source_event_id) else (
                "fingerprint",
                job_scope,
                str(
                    event_fingerprint
                    if not self._is_missing_key_part(event_fingerprint)
                    else f"{source}:{event.get('canonical_user_id')}:{event.get('event_type')}:{event.get('event_time')}"
                ),
            )
            dedupe_map[key] = event
        return list(dedupe_map.values())

    def _build_latest_state_from_events(
        self,
        player_id: Any,
        player_df: pd.DataFrame,
        job_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if player_df is None or player_df.empty:
            return None

        df = player_df.copy()
        if "event_time" not in df.columns:
            return None

        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
        df = df[df["event_time"].notna()].copy()
        if df.empty:
            return None

        df = df.sort_values(by="event_time")
        first_seen = df["event_time"].iloc[0]
        last_seen = df["event_time"].iloc[-1]
        now = pd.Timestamp.now(tz="UTC")

        def _count_sessions(frame: pd.DataFrame) -> int:
            if frame.empty:
                return 0
            ordered = frame.sort_values(by="event_time")
            new_sessions = ordered["event_time"].diff() > pd.Timedelta(minutes=15)
            return int(1 + new_sessions.fillna(False).sum())

        total_revenue = 0.0
        if "event_properties" in df.columns and "event_type" in df.columns:
            purchases = df[df["event_type"] == "item_purchased"]
            if not purchases.empty:
                total_revenue = float(
                    purchases["event_properties"].apply(
                        lambda value: (
                            value.get("revenue_usd", 0)
                            if isinstance(value, dict)
                            else 0
                        )
                    ).sum()
                )

        last_campaign = None
        last_media_source = None
        email = None
        if "event_properties" in df.columns or "user_properties" in df.columns:
            for _, row in df.iloc[::-1].iterrows():
                props = row["event_properties"] if isinstance(row.get("event_properties"), dict) else {}
                user_props = row["user_properties"] if isinstance(row.get("user_properties"), dict) else {}
                if last_campaign is None and props.get("campaign") is not None:
                    last_campaign = props.get("campaign")
                if last_media_source is None and props.get("media_source") is not None:
                    last_media_source = props.get("media_source")
                if email is None:
                    email = user_props.get("email") or props.get("email") or props.get("user_email")
                if last_campaign is not None and last_media_source is not None and email is not None:
                    break

        window_7d = df[df["event_time"] >= (now - pd.Timedelta(days=7))]
        window_30d = df[df["event_time"] >= (now - pd.Timedelta(days=30))]
        canonical_user_id = None
        if "canonical_user_id" in df.columns:
            non_null_canon = df["canonical_user_id"].dropna()
            if not non_null_canon.empty:
                canonical_user_id = str(non_null_canon.iloc[-1])

        player_id_value = None
        if "player_id" in df.columns:
            non_null_player_ids = df["player_id"].dropna()
            if not non_null_player_ids.empty:
                player_id_value = str(non_null_player_ids.iloc[-1])

        last_job_id = None
        for column in ("job_id", "job_identifier"):
            if column in df.columns:
                non_null_values = df[column].dropna()
                if not non_null_values.empty:
                    last_job_id = str(non_null_values.iloc[-1])
                    break
        resolved_job_id = str(job_id or last_job_id or "unknown_job")

        total_sessions = _count_sessions(df)
        lifetime_events = int(len(df))
        return {
            "player_id": player_id_value or str(player_id),
            "canonical_user_id": canonical_user_id,
            "email": email,
            "first_seen_at": first_seen.isoformat(),
            "last_seen_at": last_seen.isoformat(),
            "lifetime_events": lifetime_events,
            "total_events": lifetime_events,
            "lifetime_revenue_usd": total_revenue,
            "total_revenue": total_revenue,
            "total_sessions": total_sessions,
            "sessions_7d": _count_sessions(window_7d),
            "sessions_30d": _count_sessions(window_30d),
            "days_since_last_seen": int((now - last_seen).days),
            "last_campaign": last_campaign,
            "last_media_source": last_media_source,
            "last_job_id": resolved_job_id,
            "job_id": resolved_job_id,
            "job_identifier": resolved_job_id,
        }

    def write_events_staging(self, rows: List[Dict[str, Any]], job_id: Optional[str] = None):
        if not rows:
            return

        resolved_job_id = job_id
        prepared_rows = []
        for row in rows:
            row_copy = dict(row)
            if resolved_job_id is None:
                resolved_job_id = row_copy.get("job_id") or row_copy.get("job_identifier")
            prepared_rows.append(row_copy)

        resolved_job_id = str(resolved_job_id or "unknown_job")
        for row in prepared_rows:
            row.setdefault("job_id", resolved_job_id)
            row.setdefault("job_identifier", resolved_job_id)

        self._append_rows(prepared_rows, target="events_staging")

    def write_pipeline_dead_letters(self, rows: List[Dict[str, Any]], job_id: Optional[str] = None):
        if not rows:
            return

        resolved_job_id = job_id
        prepared_rows = []
        for row in rows:
            row_copy = dict(row)
            if resolved_job_id is None:
                resolved_job_id = row_copy.get("job_id") or row_copy.get("job_identifier")
            prepared_rows.append(row_copy)

        resolved_job_id = str(resolved_job_id or "unknown_job")
        for row in prepared_rows:
            row.setdefault("job_id", resolved_job_id)
            row.setdefault("job_identifier", resolved_job_id)

        self._append_rows(prepared_rows, target="pipeline_dead_letters")

    def write_processed_events(self, events: List[Dict[str, Any]], job_identifier: str):
        if not events:
            return

        prepared_events = []
        for event in events:
            event_copy = dict(event)
            event_copy["job_identifier"] = job_identifier
            event_copy.setdefault("job_id", job_identifier)
            prepared_events.append(event_copy)

        self.write_events_staging(prepared_events, job_id=job_identifier)

    def run_events_curation(self, job_id: Optional[str] = None, event_date: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            staging_rows = self._load_all_rows_from_target("events_staging")
            normalized_rows = [self._normalize_event_for_curation(row) for row in staging_rows]
            deduped_rows = self._dedupe_events(normalized_rows)
            self._replace_rows_unlocked(deduped_rows, target="events_curated")
            return {
                "job_id": job_id,
                "event_date": event_date,
                "full_recompute": True,
                "staging_rows": len(staging_rows),
                "normalized_rows": len(normalized_rows),
                "curated_rows": len(deduped_rows),
                "duplicates_removed": max(0, len(staging_rows) - len(deduped_rows)),
            }

    def get_player_events_curated(self, player_id: Any, limit: int = 1000, job_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if self.mode == "bigquery":
            rows = self._query_rows_by_identity_gcp(
                self._curated_table_id,
                player_id,
                limit=limit,
                job_id=job_id,
            )
            return rows
        if self.mode == "redshift":
            rows = [
                row
                for row in self._load_all_rows_from_target("events_curated")
                if self._preferred_player_identity(row) == str(player_id)
                or str(row.get("canonical_user_id") or "") == str(player_id)
            ]
            if job_id:
                rows = [row for row in rows if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)]
            return rows[: max(1, int(limit))]

        player_df = self._get_local_events_for_identity(player_id, job_id=job_id, target="events_curated")
        if player_df is None or player_df.empty:
            return []
        return player_df.head(max(1, int(limit))).to_dict(orient="records")

    def get_events_for_player(self, player_id: Any, job_id: Optional[str] = None) -> Optional[pd.DataFrame]:
        if self.mode == "bigquery":
            curated_rows = self._query_rows_by_identity_gcp(self._curated_table_id, player_id, job_id=job_id)
            if curated_rows:
                return pd.DataFrame(curated_rows)
            staging_rows = self._query_rows_by_identity_gcp(self._table_id, player_id, job_id=job_id)
            if staging_rows:
                return pd.DataFrame(staging_rows)
            return None
        if self.mode == "redshift":
            curated_rows = self.get_player_events_curated(player_id, limit=5000, job_id=job_id)
            if curated_rows:
                return pd.DataFrame(curated_rows)
            staging_rows = [
                row
                for row in self._load_all_rows_from_target("events_staging")
                if self._preferred_player_identity(row) == str(player_id)
                or str(row.get("canonical_user_id") or "") == str(player_id)
            ]
            if job_id:
                staging_rows = [row for row in staging_rows if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)]
            if staging_rows:
                return pd.DataFrame(staging_rows)
            return None

        player_df = self._get_local_events_for_identity(player_id, job_id=job_id, target="events_curated")
        if player_df is not None and not player_df.empty:
            return player_df
        return self._get_local_events_for_identity(player_id, job_id=job_id, target="events_staging")

    def get_all_player_ids(self, job_id: Optional[str] = None) -> List[Any]:
        if job_id:
            return self.get_import_roster_player_ids(job_id)

        if self.mode == "bigquery":
            queries = [
                f"""
                    SELECT DISTINCT COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) AS player_id
                    FROM `{self._player_latest_state_table_id}`
                    WHERE COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) IS NOT NULL
                """,
                f"""
                    SELECT DISTINCT COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) AS player_id
                    FROM `{self._curated_table_id}`
                    WHERE COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) IS NOT NULL
                """,
                f"""
                    SELECT DISTINCT COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) AS player_id
                    FROM `{self._table_id}`
                    WHERE COALESCE(CAST(player_id AS STRING), CAST(canonical_user_id AS STRING)) IS NOT NULL
                """,
            ]
            for query in queries:
                try:
                    rows = []
                    for row in self._client.query(query).result():
                        value = row["player_id"]
                        if value is not None:
                            rows.append(value)
                    if rows:
                        return rows
                except Exception:
                    continue
            return []
        if self.mode == "redshift":
            seen: List[str] = []
            for target in ("player_latest_state", "events_curated", "events_staging"):
                for row in self._load_all_rows_from_target(target):
                    value = self._preferred_player_identity(row)
                    if value is not None:
                        seen.append(value)
                if seen:
                    return list(dict.fromkeys(seen))
            return []

        for target in ("player_latest_state", "events_curated", "events_staging"):
            table = self._get_mock_table(target)
            filtered_table = self._filter_table_by_job(table, job_id=job_id)
            if filtered_table.empty:
                continue
            ids: List[str] = []
            for row in filtered_table.to_dict(orient="records"):
                value = self._preferred_player_identity(row)
                if value is not None:
                    ids.append(value)
            if ids:
                return list(dict.fromkeys(ids))
        return []

    def refresh_player_latest_state(self, job_id: Optional[str] = None, event_date: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            curated_rows = self._load_all_rows_from_target("events_curated")
            if not curated_rows:
                self._replace_rows_unlocked([], target="player_latest_state")
                return {
                    "job_id": job_id,
                    "event_date": event_date,
                    "full_recompute": True,
                    "players_aggregated": 0,
                    "source_curated_rows": 0,
                }

            curated_df = pd.DataFrame(curated_rows)
            identity_keys = []
            for row in curated_df.to_dict(orient="records"):
                canonical = row.get("canonical_user_id")
                player = row.get("player_id")
                identity_keys.append(str(canonical or player or "unknown_user"))
            curated_df["_identity_key"] = identity_keys
            latest_state_rows: List[Dict[str, Any]] = []
            for identity_key, group_df in curated_df.groupby("_identity_key", sort=False):
                latest_state = self._build_latest_state_from_events(identity_key, group_df.copy(), job_id=None)
                if latest_state:
                    latest_state_rows.append(latest_state)

            self._replace_rows_unlocked(latest_state_rows, target="player_latest_state")
            return {
                "job_id": job_id,
                "event_date": event_date,
                "full_recompute": True,
                "players_aggregated": len(latest_state_rows),
                "source_curated_rows": len(curated_rows),
            }

    def get_player_latest_state(self, player_id: Any, job_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if self.mode == "bigquery":
            rows = self._query_rows_by_identity_gcp(self._player_latest_state_table_id, player_id, limit=1, job_id=job_id)
            if rows:
                return rows[0]
        elif self.mode == "redshift":
            rows = [
                row
                for row in self._load_all_rows_from_target("player_latest_state")
                if self._preferred_player_identity(row) == str(player_id)
                or str(row.get("canonical_user_id") or "") == str(player_id)
            ]
            if job_id:
                rows = [row for row in rows if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)]
            if rows:
                return rows[0]
        else:
            latest_state_df = self._get_local_events_for_identity(player_id, job_id=job_id, target="player_latest_state")
            if latest_state_df is not None and not latest_state_df.empty:
                return latest_state_df.iloc[0].to_dict()

        player_df = self.get_events_for_player(player_id, job_id=job_id)
        if player_df is None or player_df.empty:
            return None
        return self._build_latest_state_from_events(player_id, player_df, job_id=job_id)

    def get_pipeline_dead_letters(self, job_id: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        if self.mode == "bigquery":
            query = f"SELECT * FROM `{self._dead_letter_table_id}`"
            if job_id:
                query += " WHERE CAST(job_id AS STRING) = @job_id OR CAST(job_identifier AS STRING) = @job_id"
            query += f" LIMIT {max(1, int(limit))}"

            job_config = None
            if job_id:
                job_config = self._bigquery.QueryJobConfig(
                    query_parameters=[
                        self._bigquery.ScalarQueryParameter("job_id", "STRING", str(job_id))
                    ]
                )
            rows = [dict(row.items()) for row in self._client.query(query, job_config=job_config).result()]
            return rows
        if self.mode == "redshift":
            rows = self._load_all_rows_from_target("pipeline_dead_letters")
            if job_id:
                rows = [
                    row
                    for row in rows
                    if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)
                ]
            return rows[: max(1, int(limit))]

        table = self._get_mock_table("pipeline_dead_letters")
        if table.empty:
            return []
        if job_id:
            match_value = str(job_id)
            job_id_mask = table["job_id"].map(lambda value: str(value) == match_value if pd.notna(value) else False) if "job_id" in table.columns else False
            job_identifier_mask = table["job_identifier"].map(lambda value: str(value) == match_value if pd.notna(value) else False) if "job_identifier" in table.columns else False
            if isinstance(job_id_mask, bool):
                mask = job_identifier_mask
            elif isinstance(job_identifier_mask, bool):
                mask = job_id_mask
            else:
                mask = job_id_mask | job_identifier_mask
            table = table[mask]

        if table.empty:
            return []

        rows = table.head(max(1, int(limit))).to_dict(orient="records")
        return rows

    def _identity_hint(self, row: Dict[str, Any], field: str) -> Any:
        if field in row and row.get(field) not in (None, ""):
            return row.get(field)
        for container_name in ("user_properties", "event_properties"):
            container = row.get(container_name)
            if isinstance(container, dict) and container.get(field) not in (None, ""):
                return container.get(field)
        return None

    @staticmethod
    def _normalize_identity_value(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def build_identity_summary(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        rows = self.get_rows_for_alias("fact_events_unified")
        if job_id:
            rows = [
                row
                for row in rows
                if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)
            ]
        if not rows:
            rows = self.get_rows_for_alias("standardized")
            if job_id:
                rows = [
                    row
                    for row in rows
                    if str(row.get("job_id") or row.get("job_identifier") or "") == str(job_id)
                ]

        if not rows:
            return {
                "job_id": job_id,
                "rows_evaluated": 0,
                "profiles": 0,
                "stitched_rows": 0,
                "canonical_user_id_coverage": 0.0,
                "source_of_truth_matrix": {},
                "source_of_truth_decisions": [],
                "conflict_count": 0,
                "conflict_logs": [],
                "identity_links": [],
            }

        profiles: Dict[str, Dict[str, Any]] = {}
        source_matrix: Dict[str, Dict[str, int]] = {
            "canonical_user_id": {},
            "player_id": {},
            "email": {},
        }
        for row in rows:
            source = str(row.get("source") or "unknown")
            canonical = self._normalize_identity_value(row.get("canonical_user_id"))
            player_id = self._normalize_identity_value(row.get("player_id"))
            email = self._normalize_identity_value(self._identity_hint(row, "email"))
            if email is not None:
                email = email.lower()
            group_key = email or canonical or player_id or f"row:{len(profiles) + 1}"
            profile = profiles.setdefault(
                group_key,
                {
                    "key": group_key,
                    "rows": 0,
                    "sources": set(),
                    "canonicals": {},
                    "player_ids": {},
                    "emails": {},
                },
            )
            profile["rows"] += 1
            profile["sources"].add(source)
            if canonical:
                profile["canonicals"][canonical] = profile["canonicals"].get(canonical, 0) + 1
                source_matrix["canonical_user_id"][source] = source_matrix["canonical_user_id"].get(source, 0) + 1
            if player_id:
                profile["player_ids"][player_id] = profile["player_ids"].get(player_id, 0) + 1
                source_matrix["player_id"][source] = source_matrix["player_id"].get(source, 0) + 1
            if email:
                profile["emails"][email] = profile["emails"].get(email, 0) + 1
                source_matrix["email"][source] = source_matrix["email"].get(source, 0) + 1

        stitched_rows = 0
        covered_rows = 0
        conflict_logs: List[Dict[str, Any]] = []
        identity_links = self.get_identity_links(job_id=job_id, limit=500)
        source_of_truth_decisions = self.get_field_conflicts(job_id=job_id, limit=200)
        for profile in profiles.values():
            resolved_canonical = None
            canonical_candidates = profile["canonicals"]
            if canonical_candidates:
                resolved_canonical = sorted(
                    canonical_candidates.items(),
                    key=lambda item: (-int(item[1]), item[0]),
                )[0][0]
            elif profile["player_ids"]:
                resolved_canonical = sorted(profile["player_ids"])[0]
                stitched_rows += int(profile["rows"])
            elif profile["emails"]:
                resolved_canonical = f"email:{sorted(profile['emails'])[0]}"
                stitched_rows += int(profile["rows"])
            if resolved_canonical:
                covered_rows += int(profile["rows"])
            if len(profile["canonicals"]) > 1:
                conflict_logs.append(
                    {
                        "identity_key": profile["key"],
                        "type": "canonical_conflict",
                        "sources": sorted(profile["sources"]),
                        "candidates": profile["canonicals"],
                        "rows": profile["rows"],
                    }
                )

        def _field_matrix(counts: Dict[str, int]) -> Dict[str, Any]:
            winner = None
            if counts:
                winner = sorted(counts.items(), key=lambda item: (-int(item[1]), item[0]))[0][0]
            return {
                "winner": winner,
                "sources": counts,
            }

        return {
            "job_id": job_id,
            "rows_evaluated": len(rows),
            "profiles": len(profiles),
            "stitched_rows": stitched_rows,
            "canonical_user_id_coverage": round((covered_rows / len(rows) * 100.0), 2) if rows else 0.0,
            "source_of_truth_matrix": {
                field: _field_matrix(counts)
                for field, counts in source_matrix.items()
            },
            "source_of_truth_decisions": source_of_truth_decisions,
            "conflict_count": len(conflict_logs) + len(source_of_truth_decisions),
            "conflict_logs": (conflict_logs + source_of_truth_decisions)[:25],
            "identity_links": identity_links,
        }

    def replace_prediction_results(self, job_id: str, rows: List[Dict[str, Any]]):
        resolved_job_id = str(job_id)
        prepared_rows = []
        for row in rows:
            row_copy = dict(row)
            row_copy.setdefault("prediction_job_id", resolved_job_id)
            prepared_rows.append(row_copy)

        if self.mode == "bigquery":
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("job_id", "STRING", resolved_job_id)
                ]
            )
            try:
                self._client.query(
                    f"DELETE FROM `{self._prediction_results_table_id}` WHERE CAST(prediction_job_id AS STRING) = @job_id",
                    job_config=job_config,
                ).result()
            except Exception:
                pass
            self._append_rows(prepared_rows, target="prediction_results")
            return
        if self.mode == "redshift":
            self._redshift.delete_rows_for_job("prediction_results", resolved_job_id, prediction_job=True)
            self._append_rows(prepared_rows, target="prediction_results")
            return

        with self._lock:
            table = self._get_mock_table("prediction_results")
            if not table.empty and "prediction_job_id" in table.columns:
                table = table[
                    table["prediction_job_id"].map(
                        lambda value: str(value) != resolved_job_id if pd.notna(value) else True
                    )
                ].copy()
            if prepared_rows:
                new_rows = pd.DataFrame(prepared_rows)
                if table.empty:
                    table = new_rows
                else:
                    table = pd.concat([table, new_rows], ignore_index=True)
            self._replace_rows_unlocked(table.to_dict(orient="records"), target="prediction_results")

    def append_prediction_results(self, job_id: str, rows: List[Dict[str, Any]]):
        resolved_job_id = str(job_id)
        prepared_rows = []
        for row in rows:
            row_copy = dict(row)
            row_copy.setdefault("prediction_job_id", resolved_job_id)
            prepared_rows.append(row_copy)
        if not prepared_rows:
            return
        self._append_rows(prepared_rows, target="prediction_results")

    def delete_prediction_results(self, job_id: str) -> None:
        self.replace_prediction_results(job_id=job_id, rows=[])

    def get_local_cache_stats(self) -> Dict[str, Any]:
        if self.mode != "mock":
            return {}

        def _table_stats(table: pd.DataFrame, cache_path: str) -> Dict[str, Any]:
            path = Path(cache_path)
            return {
                "rows": int(len(table.index)) if table is not None else 0,
                "cache_path": str(path),
                "size_bytes": int(path.stat().st_size) if path.exists() else 0,
            }

        if self._uses_database_mock_storage():
            persistent = self.is_mock_state_persistent()
            return {
                "retention_days": max(1, int(os.getenv("JOB_RETENTION_DAYS", "7"))),
                "storage_backend": "database",
                "persistent": persistent,
                "tables": {
                    "events_staging": {"rows": len(self._get_local_rows("events_staging")), "cache_path": "", "size_bytes": 0},
                    "events_curated": {"rows": len(self._get_local_rows("events_curated")), "cache_path": "", "size_bytes": 0},
                    "player_latest_state": {"rows": len(self._get_local_rows("player_latest_state")), "cache_path": "", "size_bytes": 0},
                    "pipeline_dead_letters": {"rows": len(self._get_local_rows("pipeline_dead_letters")), "cache_path": "", "size_bytes": 0},
                    "prediction_results": {"rows": len(self._get_local_rows("prediction_results")), "cache_path": "", "size_bytes": 0},
                },
            }

        with self._lock:
            return {
                "retention_days": max(1, int(os.getenv("JOB_RETENTION_DAYS", "7"))),
                "storage_backend": "local_files",
                "persistent": False,
                "tables": {
                    "events_staging": _table_stats(self._table, self._cache_path),
                    "events_curated": _table_stats(self._curated_table, self._curated_cache_path),
                    "player_latest_state": _table_stats(self._player_latest_state_table, self._player_latest_state_cache_path),
                    "pipeline_dead_letters": _table_stats(self._dead_letter_table, self._dead_letter_cache_path),
                    "prediction_results": _table_stats(self._prediction_results_table, self._prediction_results_cache_path),
                },
            }

    def _sort_prediction_result_dicts(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def _sort_key(item: Dict[str, Any]):
            raw_completed_at = item.get("completed_at")
            try:
                completed_at = datetime.fromisoformat(str(raw_completed_at))
            except (TypeError, ValueError):
                completed_at = datetime.min
            return (completed_at, str(item.get("user_id") or ""))

        return sorted(items, key=_sort_key, reverse=True)

    def list_prediction_results(self, job_id: str, page: int = 1, page_size: int = 100) -> Dict[str, Any]:
        page = max(1, int(page))
        page_size = max(1, int(page_size))
        offset = (page - 1) * page_size

        if self.mode == "bigquery":
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("job_id", "STRING", str(job_id))
                ]
            )
            count_query = (
                f"SELECT COUNT(*) AS total FROM `{self._prediction_results_table_id}` "
                "WHERE CAST(prediction_job_id AS STRING) = @job_id"
            )
            row_query = (
                f"SELECT * FROM `{self._prediction_results_table_id}` "
                "WHERE CAST(prediction_job_id AS STRING) = @job_id"
            )
            total = int(next(iter(self._client.query(count_query, job_config=job_config).result()))["total"])
            items = [self._deserialize_prediction_row(dict(row.items())) for row in self._client.query(row_query, job_config=job_config).result()]
            items = self._sort_prediction_result_dicts(items)[offset: offset + page_size]
            return {"page": page, "page_size": page_size, "total": total, "items": items}
        if self.mode == "redshift":
            items = [
                self._deserialize_prediction_row(row)
                for row in self._load_all_rows_from_target("prediction_results")
                if str(row.get("prediction_job_id") or "") == str(job_id)
            ]
            total = len(items)
            items = self._sort_prediction_result_dicts(items)[offset: offset + page_size]
            return {"page": page, "page_size": page_size, "total": total, "items": items}

        with self._lock:
            table = self._get_mock_table("prediction_results")
            if table.empty:
                return {"page": page, "page_size": page_size, "total": 0, "items": []}
            if "prediction_job_id" not in table.columns:
                return {"page": page, "page_size": page_size, "total": 0, "items": []}
            table = table[
                table["prediction_job_id"].map(lambda value: str(value) == str(job_id) if pd.notna(value) else False)
            ].copy()
            if table.empty:
                return {"page": page, "page_size": page_size, "total": 0, "items": []}
            total = len(table)
            items = [self._deserialize_prediction_row(row) for row in table.to_dict(orient="records")]
            items = self._sort_prediction_result_dicts(items)[offset: offset + page_size]
            return {"page": page, "page_size": page_size, "total": total, "items": items}

    def _deserialize_prediction_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        parsed = dict(row)
        for key, value in list(parsed.items()):
            try:
                if value is None or bool(pd.isna(value)):
                    parsed[key] = None
                    continue
            except (TypeError, ValueError):
                pass
            if not isinstance(value, str):
                continue
            if value.strip().lower() == "nan":
                parsed[key] = None
                continue
            if not value.startswith("{") and not value.startswith("["):
                continue
            try:
                parsed[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
        return parsed

    def delete_data_for_job(self, job_identifier: str):
        if self.mode == "bigquery":
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("job_identifier", "STRING", job_identifier)
                ]
            )
            queries = [
                f"""
                    DELETE FROM `{self._table_id}`
                    WHERE job_identifier = @job_identifier OR job_id = @job_identifier
                """,
                f"""
                    DELETE FROM `{self._dead_letter_table_id}`
                    WHERE job_identifier = @job_identifier OR job_id = @job_identifier
                """,
            ]
            for query in queries:
                self._client.query(query, job_config=job_config).result()
            self.run_events_curation()
            self.refresh_player_latest_state()
            print(f"Deleted rows from BigQuery for job '{job_identifier}'.")
            return
        if self.mode == "redshift":
            self._redshift.delete_rows_for_job("events_staging", job_identifier)
            self._redshift.delete_rows_for_job("pipeline_dead_letters", job_identifier)
            self.run_events_curation()
            self.refresh_player_latest_state()
            print(f"Deleted rows from Redshift for job '{job_identifier}'.")
            return

        with self._lock:
            for target_name in ("events_staging", "pipeline_dead_letters"):
                table = self._get_mock_table(target_name)
                if table.empty:
                    continue

                masks = []
                if "job_identifier" in table.columns:
                    masks.append(table["job_identifier"].map(lambda value: str(value) == str(job_identifier) if pd.notna(value) else False))
                if "job_id" in table.columns:
                    masks.append(table["job_id"].map(lambda value: str(value) == str(job_identifier) if pd.notna(value) else False))
                if not masks:
                    continue

                combined_mask = masks[0]
                for mask in masks[1:]:
                    combined_mask = combined_mask | mask

                initial_rows = len(table)
                filtered = table[~combined_mask].copy()
                rows_deleted = initial_rows - len(filtered)
                if rows_deleted > 0:
                    print(f"Deleted {rows_deleted} rows from mock target '{target_name}' for job '{job_identifier}'.")
                    self._replace_rows_unlocked(filtered.to_dict(orient='records'), target=target_name)

            self.run_events_curation()
            self.refresh_player_latest_state()
