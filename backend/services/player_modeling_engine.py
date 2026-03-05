# player_modeling_engine.py

from typing import List, Dict, Any, Optional
from datetime import datetime
import json
import pandas as pd
from gemini_client import GeminiClient
from json_encoder import NpEncoder
from bigquery_service import BigQueryService

class PlayerModelingEngine:
    """
    Analyzes player event data to build intelligence profiles, including
    summaries, churn state, and churn risk.
    """
    def __init__(self, gemini_client: Optional[GeminiClient], bigquery_service: BigQueryService, churn_inactive_days: int = 14):
        """
        Initializes the engine with AI and data warehouse clients.

        Args:
            gemini_client: Optional client for generative AI models.
            bigquery_service: Client for querying processed player data.
        """
        self.ai_client = gemini_client
        self.db_client = bigquery_service
        self.churn_inactive_days = max(1, int(churn_inactive_days))

    def _get_and_preprocess_player_data(self, player_id: Any) -> Optional[pd.DataFrame]:
        """Fetches player data from the warehouse and performs preprocessing."""
        df = self.db_client.get_events_for_player(player_id)
        if df is None or df.empty:
            return None
        
        # The 'event_time' from Amplitude is a string like '2024-01-01 12:34:56.123456'
        # We need to parse it into a datetime object.
        df['event_time'] = pd.to_datetime(df['event_time'])
        
        print(f"Preprocessing complete for player {player_id}. DataFrame created with {len(df)} events.")
        return df


    def build_player_profile(self, player_id: Any) -> Optional[Dict[str, Any]]:
        """
        Builds a summary profile for a single player.

        Args:
            player_id: The unique identifier for the player.

        Returns:
            A dictionary containing the player's profile, or None if not found.
        """
        player_events = self._get_and_preprocess_player_data(player_id)

        if player_events is None or player_events.empty:
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
        total_sessions = int(1 + is_new_session.sum()) # Ensure total_sessions is a standard Python int
        
        # Calculate total revenue from 'item_purchased' events
        purchases = player_events[player_events['event_type'] == 'item_purchased']
        total_revenue = 0
        if not purchases.empty:
            # Sum up revenue from the 'revenue_usd' field in event_properties
            total_revenue = float(purchases['event_properties'].apply(lambda x: x.get('revenue_usd', 0)).sum()) # Ensure total_revenue is a standard Python float

        days_since_last_seen = (datetime.utcnow() - last_seen.replace(tzinfo=None)).days
        churn_state = "churned" if days_since_last_seen >= self.churn_inactive_days else "active"

        profile = {
            "player_id": player_id,
            "first_seen_date": first_seen.isoformat(),
            "last_seen_date": last_seen.isoformat(),
            "total_sessions": total_sessions,
            "total_events": len(player_events),
            "total_revenue": total_revenue,
            "days_since_last_seen": days_since_last_seen,
            "churn_state": churn_state,
            "churn_inactive_days": self.churn_inactive_days,
        }
        return profile

    def _estimate_churn_risk_heuristic(self, player_id: Any, player_profile: Dict[str, Any]) -> Dict[str, Any]:
        """Deterministic fallback when LLM is unavailable."""
        days_since_last_seen = float(player_profile.get("days_since_last_seen", 0) or 0)
        total_sessions = float(player_profile.get("total_sessions", 0) or 0)
        total_revenue = float(player_profile.get("total_revenue", 0.0) or 0.0)
        churn_state = player_profile.get("churn_state", "active")

        if churn_state == "churned":
            return {
                "player_id": player_id,
                "churn_state": "churned",
                "churn_risk": "already_churned",
                "reason": f"No activity for {int(days_since_last_seen)} days (threshold={self.churn_inactive_days}).",
                "top_signals": [
                    {"signal": "inactive_days", "value": days_since_last_seen},
                    {"signal": "threshold_days", "value": self.churn_inactive_days},
                ],
            }

        score = 0.0
        signals = []
        if days_since_last_seen >= 7:
            score += 45
            signals.append({"signal": "inactive_7d_plus", "value": days_since_last_seen})
        elif days_since_last_seen >= 3:
            score += 20
            signals.append({"signal": "inactive_3d_plus", "value": days_since_last_seen})

        if total_sessions <= 2:
            score += 20
            signals.append({"signal": "very_low_sessions", "value": total_sessions})
        elif total_sessions <= 5:
            score += 10
            signals.append({"signal": "low_sessions", "value": total_sessions})

        if total_revenue >= 100:
            score -= 10
            signals.append({"signal": "high_value_user", "value": total_revenue})

        if score >= 70:
            churn_risk = "high"
        elif score >= 35:
            churn_risk = "medium"
        else:
            churn_risk = "low"

        return {
            "player_id": player_id,
            "churn_state": "active",
            "churn_risk": churn_risk,
            "reason": (
                f"Heuristic fallback: days_since_last_seen={days_since_last_seen}, "
                f"sessions={total_sessions}, revenue={total_revenue}."
            ),
            "top_signals": signals,
        }

    async def estimate_churn_risk(self, player_id: Any, player_profile: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Estimates the churn risk for a single player based on their profile.
        If a player_profile is provided, it reuses that rather than rebuilding it.

        Args:
            player_id: The unique identifier for the player.
            player_profile: (Optional) A pre-built player profile to avoid redundant processing.

        Returns:
            A dictionary with the player's ID, churn risk, and reason, or None.
        """
        if player_profile is None:
            player_profile = self.build_player_profile(player_id)

        if not player_profile:
            return None

        if player_profile.get("churn_state") == "churned":
            return self._estimate_churn_risk_heuristic(player_id, player_profile)

        if self.ai_client is None:
            return self._estimate_churn_risk_heuristic(player_id, player_profile)

        prompt = f"""
        As a world-class mobile game analyst, analyze the following ACTIVE player profile and estimate churn risk.
        Provide JSON with keys:
        - churn_risk: "low" | "medium" | "high"
        - reason: short plain explanation
        - top_signals: array of up to 3 objects {"signal": string, "value": number|string}

        Player Profile:
        {json.dumps(player_profile, indent=2, cls=NpEncoder)}
        """

        try:
            print("\nAsking Gemini to estimate churn risk...")
            ai_response_text = self.ai_client.get_ai_response(prompt)
            cleaned_json_text = ai_response_text.strip().replace("```json", "").replace("```", "")
            ai_analysis = json.loads(cleaned_json_text)
            return {
                "player_id": player_id,
                "churn_state": "active",
                "churn_risk": ai_analysis.get("churn_risk", "unknown"),
                "reason": ai_analysis.get("reason", "AI analysis failed."),
                "top_signals": ai_analysis.get("top_signals", []),
            }
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error processing AI response for churn risk: {e}")
            return self._estimate_churn_risk_heuristic(player_id, player_profile)

    def get_player_engagement_patterns(self, player_id: Any) -> Optional[pd.Series]:
        """
        Analyzes a player's engagement patterns, like top events.

        Args:
            player_id: The unique identifier for the player.

        Returns:
            A pandas Series with event counts, or None if player not found.
        """
        player_events = self._get_and_preprocess_player_data(player_id)
        if player_events is None or player_events.empty:
            return None
        
        # Return a count of each event type for this player
        return player_events['event_type'].value_counts()

    def get_all_player_ids(self) -> List[Any]:
        """
        Retrieves a list of all unique player IDs from the data source.

        Returns:
            A list of unique player IDs.
        """
        print("Fetching all unique player IDs from the data warehouse...")
        return self.db_client.get_all_player_ids()
