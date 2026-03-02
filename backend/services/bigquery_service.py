# bigquery_service.py

import os
import json
import pandas as pd
from typing import List, Dict, Any, Optional

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


class BigQueryService:
    """
    BigQuery service with dual backend support:
    - mock: local pandas/parquet cache (dev/qa)
    - gcp: real Google BigQuery client (prod)
    """

    def __init__(self):
        self.mode = os.getenv("DATA_BACKEND_MODE", "mock").strip().lower()
        if self.mode not in {"mock", "gcp"}:
            raise ValueError("DATA_BACKEND_MODE must be 'mock' or 'gcp'.")

        if self.mode == "gcp":
            self._init_gcp_backend()
            print(f"BigQueryService initialized in GCP mode (table: {self._table_id}).")
        else:
            self._init_mock_backend()
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

        dataset_id = os.getenv("BIGQUERY_DATASET_ID", "kairyx")
        table_name = os.getenv("BIGQUERY_TABLE_NAME", "processed_events")
        self._table_id = os.getenv("BIGQUERY_TABLE_ID", f"{project_id}.{dataset_id}.{table_name}")

        self._bigquery = bigquery
        self._client = bigquery.Client(project=project_id)

    def _init_mock_backend(self):
        self._cache_path = ".cache/bigquery_table.parquet"
        if os.path.exists(self._cache_path):
            print(f"Loading BigQuery cache from {self._cache_path}")
            self._table = pd.read_parquet(self._cache_path)
            self._restore_complex_columns_from_parquet()
        else:
            self._table = pd.DataFrame()
            os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)

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

    def _restore_complex_columns_from_parquet(self):
        """
        Restores JSON-serialized dict/list values after loading Parquet data.
        """
        for column in self._table.columns:
            series = self._table[column]
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

            self._table[column] = series.map(_maybe_parse_json)

    def _coerce_oversized_integer_columns(self):
        """
        Parquet cannot store integers outside int64 range.
        If a scalar-integer column contains oversized values, cast the full
        column to string to keep a consistent, writable schema.
        """
        for column in self._table.columns:
            series = self._table[column]
            non_null = series[series.notna()]
            if non_null.empty:
                continue

            if not non_null.map(_is_int_like_scalar).all():
                continue

            if non_null.map(_is_oversized_int).any():
                self._table[column] = series.map(
                    lambda value: str(value) if _is_int_like_scalar(value) else value
                )

    def write_processed_events(self, events: List[Dict[str, Any]], job_identifier: str):
        if not events:
            return

        prepared_events = []
        for event in events:
            event_copy = dict(event)
            event_copy["job_identifier"] = job_identifier
            prepared_events.append(_sanitize_for_storage(event_copy))

        if self.mode == "gcp":
            job_config = self._bigquery.LoadJobConfig(
                source_format=self._bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=self._bigquery.WriteDisposition.WRITE_APPEND,
                create_disposition=self._bigquery.CreateDisposition.CREATE_IF_NEEDED,
                autodetect=True,
                ignore_unknown_values=True,
            )
            load_job = self._client.load_table_from_json(prepared_events, self._table_id, job_config=job_config)
            load_job.result()
            print(f"Wrote {len(prepared_events)} events to BigQuery table {self._table_id}.")
            return

        new_data_df = pd.DataFrame(prepared_events)
        if self._table.empty:
            self._table = new_data_df
        else:
            self._table = pd.concat([self._table, new_data_df], ignore_index=True)

        print("Sanitizing DataFrame for Parquet compatibility...")
        if hasattr(self._table, "map"):
            self._table = self._table.map(_sanitize_for_storage)
        else:
            self._table = self._table.applymap(_sanitize_for_storage)
        self._coerce_oversized_integer_columns()

        print(f"Wrote {len(new_data_df)} events to local BigQuery mock cache. Table now has {len(self._table)} total events.")
        table_to_persist = self._prepare_for_parquet(self._table)
        table_to_persist.to_parquet(self._cache_path)

    def get_events_for_player(self, player_id: Any) -> Optional[pd.DataFrame]:
        if self.mode == "gcp":
            query = f"""
                SELECT *
                FROM `{self._table_id}`
                WHERE CAST(player_id AS STRING) = @player_id
            """
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("player_id", "STRING", str(player_id))
                ]
            )
            rows = [dict(row.items()) for row in self._client.query(query, job_config=job_config).result()]
            if not rows:
                return None
            return pd.DataFrame(rows)

        if self._table.empty or "player_id" not in self._table.columns:
            return None

        player_df = self._table[self._table["player_id"] == player_id].copy()
        return player_df if not player_df.empty else None

    def get_all_player_ids(self) -> List[Any]:
        if self.mode == "gcp":
            query = f"SELECT DISTINCT CAST(player_id AS STRING) AS player_id FROM `{self._table_id}`"
            rows = [row["player_id"] for row in self._client.query(query).result()]
            return rows

        if self._table.empty or "player_id" not in self._table.columns:
            return []
        return self._table["player_id"].unique().tolist()

    def delete_data_for_job(self, job_identifier: str):
        if self.mode == "gcp":
            query = f"""
                DELETE FROM `{self._table_id}`
                WHERE job_identifier = @job_identifier
            """
            job_config = self._bigquery.QueryJobConfig(
                query_parameters=[
                    self._bigquery.ScalarQueryParameter("job_identifier", "STRING", job_identifier)
                ]
            )
            self._client.query(query, job_config=job_config).result()
            print(f"Deleted rows from BigQuery for job '{job_identifier}'.")
            return

        if self._table.empty or "job_identifier" not in self._table.columns:
            return

        initial_rows = len(self._table)
        self._table = self._table[self._table["job_identifier"] != job_identifier]
        rows_deleted = initial_rows - len(self._table)
        if rows_deleted > 0:
            print(f"Deleted {rows_deleted} rows from local BigQuery mock for job '{job_identifier}'.")
            table_to_persist = self._prepare_for_parquet(self._table)
            table_to_persist.to_parquet(self._cache_path)

