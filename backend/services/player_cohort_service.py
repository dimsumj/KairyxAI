# player_cohort_service.py

import asyncio
from typing import List, Dict, Any
from player_modeling_engine import PlayerModelingEngine

class PlayerCohortService:
    """
    Analyzes player data to create distinct player cohorts based on behavior.
    """

    def __init__(self, modeling_engine: PlayerModelingEngine):
        """
        Initializes the service with a player modeling engine.

        Args:
            modeling_engine: An instance of PlayerModelingEngine with loaded player data.
        """
        self.modeling_engine = modeling_engine
        print("PlayerCohortService initialized.")

    async def create_player_cohorts(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Segments all players into cohorts based on their profiles.

        Returns:
            A dictionary where keys are cohort names and values are lists of player profiles.
        """
        COHORT_DEFINITIONS = {
            "new_players": lambda p: p["days_since_last_seen"] <= 3 and p["total_sessions"] <= 5,
            "active_spenders": lambda p: p["total_revenue"] > 0 and p["days_since_last_seen"] <= 14,
            "at_risk_of_churn": lambda p: 14 < p["days_since_last_seen"] <= 30,
            "dormant_players": lambda p: p["days_since_last_seen"] > 30,
        }

        player_ids = self.modeling_engine.get_all_player_ids()
        if not player_ids:
            return {}

        cohorts = {name: [] for name in COHORT_DEFINITIONS.keys()}

        # Build profiles and estimate churn risk concurrently for better performance
        async def process_player(player_id):
            profile = self.modeling_engine.build_player_profile(player_id)
            if not profile:
                return None
            
            churn_estimate = await self.modeling_engine.estimate_churn_risk(player_id)
            profile["predicted_churn_risk"] = churn_estimate.get("churn_risk", "unknown") if churn_estimate else "unknown"
            return profile

        tasks = [process_player(player_id) for player_id in player_ids]
        all_profiles = await asyncio.gather(*tasks)

        for profile in all_profiles:
            if not profile:
                continue
            
            # Assign player to the first matching cohort
            for name, condition in COHORT_DEFINITIONS.items():
                if condition(profile):
                    cohorts[name].append(profile)
                    break

        print("Player cohorts created successfully.")
        return cohorts