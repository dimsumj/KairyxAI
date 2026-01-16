# churn_reporter.py

import pandas as pd
from typing import List, Any
from player_modeling_engine import PlayerModelingEngine
from growth_decision_engine import GrowthDecisionEngine

class ChurnReporter:
    """
    Generates a CSV report of players predicted to churn, including the
    reason for the prediction and the AI-suggested engagement action.
    """

    def __init__(self, modeling_engine: PlayerModelingEngine, decision_engine: GrowthDecisionEngine):
        """
        Initializes the reporter with the necessary engine dependencies.
        """
        self.modeling_engine = modeling_engine
        self.decision_engine = decision_engine
        print("ChurnReporter initialized.")

    def generate_report(self, player_ids: List[Any], output_filepath: str):
        """
        Analyzes a list of players and generates a CSV report for those at risk of churning.

        Args:
            player_ids: A list of player IDs to analyze.
            output_filepath: The path to save the generated CSV file (e.g., 'churn_report.csv').
        """
        report_data = []
        print(f"\nGenerating churn report for {len(player_ids)} players...")

        players_reported = 0
        for player_id in player_ids:
            # For the demo, we will stop after finding 5 at-risk players.
            if players_reported >= 5:
                print("\nDemo limit of 5 players reached for the report. Halting analysis.")
                break

            churn_estimate = self.modeling_engine.estimate_churn_risk(player_id)

            # Only include players with a medium or high churn risk in the report
            if churn_estimate and churn_estimate.get("churn_risk") in ["medium", "high"]:
                player_profile = self.modeling_engine.build_player_profile(player_id)
                next_action = self.decision_engine.decide_next_action(
                    player_profile, churn_estimate, objective="reduce_churn"
                )

                report_data.append({
                    "player_id": player_id,
                    "churn_risk": churn_estimate.get("churn_risk"),
                    "reason": churn_estimate.get("reason"),
                    "suggested_action": next_action.get("content") if next_action else "N/A",
                })
                players_reported += 1

        if not report_data:
            print("No players with medium or high churn risk found.")
            return

        report_df = pd.DataFrame(report_data)
        report_df.to_csv(output_filepath, index=False)
        print(f"Churn report successfully generated and saved to '{output_filepath}'")