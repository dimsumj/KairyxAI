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


def _sanitize_for_parquet(data: Any) -> Any:
    """
    Recursively traverses data structures (lists, dicts) and replaces
    unsupported values:
    - empty dictionaries -> None
    - oversized integers (outside int64 range) -> str
    """
    if isinstance(data, dict):
        if not data:  # If dictionary is empty
            return None
        return {k: _sanitize_for_parquet(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_sanitize_for_parquet(item) for item in data]
    if _is_oversized_int(data):
        return str(data)
    return data


class BigQueryService:
    """
    Simulates a service for interacting with Google BigQuery.
    In a real-world application, this would use the google-cloud-bigquery library.
    """

    def __init__(self):
        """
        Initializes the service. In a real app, this would set up the BigQuery client
        with credentials and project information.
        
        For this simulation, we'll use a pandas DataFrame as an in-memory "BigQuery table".
        """
        self._cache_path = ".cache/bigquery_table.parquet"
        if os.path.exists(self._cache_path):
            print(f"Loading BigQuery cache from {self._cache_path}")
            self._table = pd.read_parquet(self._cache_path)
            self._restore_complex_columns_from_parquet()
        else:
            self._table = pd.DataFrame()
            os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
        print("BigQueryService initialized (simulating in-memory BigQuery).")

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

            # Heuristic: only attempt JSON parse for likely serialized objects/arrays.
            if not (sample.startswith("{") or sample.startswith("[")):
                continue

            def _maybe_parse_json(value: Any) -> Any:
                if not isinstance(value, str):
                    return value
                if not (value.startswith("{") or value.startswith("[")):
                    return value
                try:
                    parsed = json.loads(value)
                    return parsed
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
        """
        Simulates writing a batch of processed events to a BigQuery table.

        Args:
            events: A list of processed and normalized event dictionaries.
            job_identifier: The identifier for the current job (e.g., 'YYYYMMDD_to_YYYYMMDD').
        """
        if not events:
            return

        for event in events:
            event['job_identifier'] = job_identifier

        new_data_df = pd.DataFrame(events)
        if self._table.empty:
            self._table = new_data_df
        else:
            self._table = pd.concat([self._table, new_data_df], ignore_index=True)
        
        # Deep sanitize the entire DataFrame to replace all empty dicts with None.
        # This is the most robust way to prevent the pyarrow "empty struct" error.
        print("Sanitizing DataFrame for Parquet compatibility...")
        if hasattr(self._table, "map"):
            self._table = self._table.map(_sanitize_for_parquet)
        else:
            self._table = self._table.applymap(_sanitize_for_parquet)
        self._coerce_oversized_integer_columns()

        print(f"Wrote {len(new_data_df)} events to BigQuery. Table now has {len(self._table)} total events.")
        table_to_persist = self._prepare_for_parquet(self._table)
        table_to_persist.to_parquet(self._cache_path)

    def get_events_for_player(self, player_id: Any) -> Optional[pd.DataFrame]:
        """
        Simulates querying BigQuery for all events belonging to a specific player.

        Args:
            player_id: The ID of the player to retrieve events for.

        Returns:
            A pandas DataFrame containing the player's events, or None if not found.
        """
        if self._table.empty or 'player_id' not in self._table.columns:
            return None
        
        player_df = self._table[self._table['player_id'] == player_id].copy()
        return player_df if not player_df.empty else None

    def get_all_player_ids(self) -> List[Any]:
        """
        Simulates querying BigQuery for all unique player IDs.

        Returns:
            A list of unique player IDs.
        """
        if self._table.empty or 'player_id' not in self._table.columns:
            return []
        
        return self._table['player_id'].unique().tolist()

    def delete_data_for_job(self, job_identifier: str):
        """
        Simulates deleting rows from the BigQuery table that are associated
        with a specific job identifier.
        """
        if self._table.empty or 'job_identifier' not in self._table.columns:
            return

        initial_rows = len(self._table)
        self._table = self._table[self._table['job_identifier'] != job_identifier]
        rows_deleted = initial_rows - len(self._table)
        if rows_deleted > 0:
            print(f"Deleted {rows_deleted} rows from BigQuery for job '{job_identifier}'.")
            table_to_persist = self._prepare_for_parquet(self._table)
            table_to_persist.to_parquet(self._cache_path)
