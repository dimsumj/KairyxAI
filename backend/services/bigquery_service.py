# bigquery_service.py

import os
import pandas as pd
from typing import List, Dict, Any, Optional

def _sanitize_for_parquet(data: Any) -> Any:
    """
    Recursively traverses data structures (lists, dicts) and replaces
    empty dictionaries with None, as pyarrow cannot serialize them to Parquet.
    """
    if isinstance(data, dict):
        if not data:  # If dictionary is empty
            return None
        return {k: _sanitize_for_parquet(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_sanitize_for_parquet(item) for item in data]
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
        else:
            self._table = pd.DataFrame()
            os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
        print("BigQueryService initialized (simulating in-memory BigQuery).")

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
        self._table = self._table.applymap(_sanitize_for_parquet)

        print(f"Wrote {len(new_data_df)} events to BigQuery. Table now has {len(self._table)} total events.")
        self._table.to_parquet(self._cache_path)

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
            self._table.to_parquet(self._cache_path)