# bigquery_service.py

import pandas as pd
from typing import List, Dict, Any, Optional

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
        self._table = pd.DataFrame()
        print("BigQueryService initialized (simulating in-memory BigQuery).")

    def write_processed_events(self, events: List[Dict[str, Any]]):
        """
        Simulates writing a batch of processed events to a BigQuery table.

        Args:
            events: A list of processed and normalized event dictionaries.
        """
        if not events:
            return

        new_data_df = pd.DataFrame(events)
        if self._table.empty:
            self._table = new_data_df
        else:
            self._table = pd.concat([self._table, new_data_df], ignore_index=True)
        
        print(f"Wrote {len(new_data_df)} events to BigQuery. Table now has {len(self._table)} total events.")

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

    def get_all_player_ids(self) -> List[Any]:
        """
        Simulates querying BigQuery for all unique player IDs.

        Returns:
            A list of unique player IDs.
        """
        if self._table.empty or 'player_id' not in self._table.columns:
            return []
        
        return self._table['player_id'].unique().tolist()