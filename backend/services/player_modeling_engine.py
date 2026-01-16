# player_modeling_engine.py

from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import pandas as pd
from gemini_client import GeminiClient
from json_encoder import NpEncoder

class PlayerModelingEngine:
    """
    Analyzes player event data to build intelligence profiles, including
    summaries, churn risk, and engagement patterns.
    """

    def __init__(self, normalized_events: List[Dict[str, Any]], gemini_client: GeminiClient):
        """
        Initializes the engine with normalized event data.

        Args:
            normalized_events: A list of cleaned event dictionaries.
        """
        if not normalized_events:
            self.player_df = pd.DataFrame()
        else:
            # Convert to DataFrame for efficient analysis
            self.player_df = self._preprocess_events(normalized_events)
        self.ai_client = gemini_client

    def _preprocess_events(self, events: List[Dict[str, Any]]) -> pd.DataFrame:
        """Converts event list to a DataFrame and enriches it."""
        df = pd.DataFrame(events)

        # Standardize the user identifier column if 'userId' (camelCase) exists
        if 'userId' in df.columns and 'user_id' not in df.columns:
            df.rename(columns={'userId': 'user_id'}, inplace=True)

        # The 'event_time' from Amplitude is a string like '2024-01-01 12:34:56.123456'
        # We need to parse it into a datetime object.
        df['event_time'] = pd.to_datetime(df['event_time'])
        
        # Use 'user_id' as the primary player identifier. Events without a user_id will be dropped.
        df['player_id'] = df['user_id']
        
        # Drop events where a player identifier could not be found
        df.dropna(subset=['player_id'], inplace=True)
        
        print(f"Preprocessing complete. DataFrame created with {len(df)} events.")
        return df

    def get_all_player_ids(self) -> List[Any]:
        """Returns a list of all unique player IDs."""
        if self.player_df.empty:
            return []
        return self.player_df['player_id'].unique().tolist()

    def build_player_profile(self, player_id: Any) -> Optional[Dict[str, Any]]:
        """
        Builds a summary profile for a single player.

        Args:
            player_id: The unique identifier for the player.

        Returns:
            A dictionary containing the player's profile, or None if not found.
        """
        player_events = self.player_df[self.player_df['player_id'] == player_id]

        if player_events.empty:
            return None

        # Sort events by time to correctly calculate sessions and identify first/last seen
        player_events = player_events.sort_values(by='event_time')

        first_seen = player_events['event_time'].iloc[0]
        last_seen = player_events['event_time'].iloc[-1]
        
        # Calculate sessions based on inactivity.
        # A new session starts with the first event, or if the time gap between
        # consecutive events is greater than 15 minutes.
        time_diffs = player_events['event_time'].diff()
        is_new_session = time_diffs > pd.Timedelta(minutes=15)
        # The total number of sessions is 1 (for the very first event) + the number of times a new session was started.
        total_sessions = 1 + is_new_session.sum()
        
        # Calculate total revenue from 'item_purchased' events
        purchases = player_events[player_events['event_type'] == 'item_purchased']
        total_revenue = 0
        if not purchases.empty:
            # Sum up revenue from the 'revenue_usd' field in event_properties
            total_revenue = purchases['event_properties'].apply(lambda x: x.get('revenue_usd', 0)).sum()

        profile = {
            "player_id": player_id,
            "first_seen_date": first_seen.isoformat(),
            "last_seen_date": last_seen.isoformat(),
            "total_sessions": total_sessions,
            "total_events": len(player_events),
            "total_revenue": total_revenue,
            "days_since_last_seen": (datetime.utcnow() - last_seen.replace(tzinfo=None)).days,
        }
        return profile

    def estimate_churn_risk(self, player_id: Any) -> Optional[Dict[str, Any]]:
        """
        Estimates the churn risk for a single player based on activity.

        Args:
            player_id: The unique identifier for the player.

        Returns:
            A dictionary with the player's ID, churn risk, and reason, or None.
        """
        player_profile = self.build_player_profile(player_id)

        if not player_profile:
            return None

        prompt = f"""
        As a world-class mobile game analyst, analyze the following player profile and estimate their churn risk.
        Provide your response as a JSON object with two keys: "churn_risk" (string: "low", "medium", or "high") and "reason" (string: a brief justification for your analysis).

        Player Profile:
        {json.dumps(player_profile, indent=2, cls=NpEncoder)}
        """

        try:
            print("\nAsking Gemini to estimate churn risk...")
            ai_response_text = self.ai_client.get_ai_response(prompt)
            
            # Clean the response to ensure it's valid JSON
            cleaned_json_text = ai_response_text.strip().replace("```json", "").replace("```", "")
            ai_analysis = json.loads(cleaned_json_text)
            
            return {
                "player_id": player_id,
                "churn_risk": ai_analysis.get("churn_risk", "unknown"),
                "reason": ai_analysis.get("reason", "AI analysis failed.")
            }
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error processing AI response for churn risk: {e}")
            return {
                "player_id": player_id,
                "churn_risk": "unknown",
                "reason": "Failed to get a valid analysis from the AI model."
            }

    def get_player_engagement_patterns(self, player_id: Any) -> Optional[pd.Series]:
        """
        Analyzes a player's engagement patterns, like top events.

        Args:
            player_id: The unique identifier for the player.

        Returns:
            A pandas Series with event counts, or None if player not found.
        """
        player_events = self.player_df[self.player_df['player_id'] == player_id]
        if player_events.empty:
            return None
        
        # Return a count of each event type for this player
        return player_events['event_type'].value_counts()