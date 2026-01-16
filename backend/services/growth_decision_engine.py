# growth_decision_engine.py

from typing import Dict, Any, Optional
import json
from gemini_client import GeminiClient
from json_encoder import NpEncoder

class GrowthDecisionEngine:
    """
    Decides the next best action for a player based on their profile
    and a specific growth objective.
    """

    def __init__(self, gemini_client: GeminiClient):
        self.ai_client = gemini_client

    def decide_next_action(
        self,
        player_profile: Dict[str, Any],
        churn_estimate: Dict[str, Any],
        objective: str
    ) -> Optional[Dict[str, Any]]:
        """
        Determines whether to act, on which channel, with what content, and when.

        Args:
            player_profile: A dictionary summarizing the player's activity.
            churn_estimate: A dictionary with the player's churn risk.
            objective: The strategic goal (e.g., 'reduce_churn').

        Returns:
            A dictionary describing the action to take, or None if no action is needed.
        """
        if not player_profile or not churn_estimate:
            return None

        if objective == 'reduce_churn':
            return self._decide_churn_reduction_action(player_profile, churn_estimate)
        
        # Future objectives can be added here
        # if objective == 'increase_monetization':
        #     return self._decide_monetization_action(player_profile)

        print(f"Warning: Objective '{objective}' not recognized. No action taken.")
        return None

    def _decide_churn_reduction_action(
        self,
        player_profile: Dict[str, Any],
        churn_estimate: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Rule-based decisions for the 'reduce_churn' objective."""
        
        churn_risk = churn_estimate.get("churn_risk")
        player_id = player_profile.get("player_id")

        if churn_risk == "low":
            return {
                "player_id": player_id,
                "decision": "NO_ACTION",
                "reason": "AI analysis indicates player is already engaged and has low churn risk."
            }

        prompt = f"""
        As a world-class AI Growth Operator for a mobile game, your goal is to reduce player churn.
        Based on the player's profile and churn analysis, devise the best engagement action.

        Generate a personalized, concise, and engaging message for a push notification. The message should be friendly and enticing.

        Provide your response as a JSON object with three keys: "decision" (string: "ACT"), "channel" (string: "push_notification"), and "content" (string: the message you generated).

        Player Profile:
        {json.dumps(player_profile, indent=2, cls=NpEncoder)}

        Churn Analysis:
        {json.dumps(churn_estimate, indent=2, cls=NpEncoder)}
        """

        try:
            print("\nAsking Gemini to decide the next best action and generate content...")
            ai_response_text = self.ai_client.get_ai_response(prompt)
            cleaned_json_text = ai_response_text.strip().replace("```json", "").replace("```", "")
            action = json.loads(cleaned_json_text)
            action["player_id"] = player_id
            action["timing"] = "immediate" # Add timing
            return action
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error processing AI response for next action: {e}")
            return {
                "player_id": player_id,
                "decision": "NO_ACTION",
                "reason": "Failed to generate a valid action from the AI model."
            }