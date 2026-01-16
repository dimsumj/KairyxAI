# engagement_executor.py

from typing import Dict, Any
import logging
import uuid

# Configure a dedicated logger for engagement actions.
# This will write to a file named 'engagement_actions.log'.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='engagement_actions.log',
    filemode='a'
)

class EngagementExecutor:
    """
    Executes engagement actions, such as sending notifications,
    and logs the actions taken.
    """

    def execute_action(self, action: Dict[str, Any]) -> str | None:
        """
        Routes the action to the appropriate channel executor if a decision to act was made.

        Args:
            action: A dictionary describing the action to take from the GrowthDecisionEngine.
        
        Returns:
            The unique action_id if an action was executed, otherwise None.
        """
        if not action or action.get("decision") != "ACT":
            print(f"\nDecision is '{action.get('decision', 'NONE')}'. No action executed.")
            return None

        channel = action.get("channel")
        player_id = action.get("player_id")
        action_id = str(uuid.uuid4())

        if channel == "push_notification":
            message = action.get("content")
            self.send_push(player_id, message, action_id)
        elif channel == "email":
            subject = action.get("subject", "A message from your game")
            body = action.get("content")
            self.send_email(player_id, subject, body, action_id)
        else:
            print(f"Warning: Channel '{channel}' is not supported. No action taken.")
            return None
        
        return action_id

    def _log_action(self, player_id: Any, channel: str, content: str, action_id: str):
        """Logs the executed action to a file and prints to the console."""
        log_message = f"Action Sent - ActionID: {action_id}, PlayerID: {player_id}, Channel: {channel}, Content: '{content}'"
        logging.info(log_message)
        print(f"LOGGED: {log_message}")

    def send_push(self, player_id: Any, message: str, action_id: str):
        """
        Simulates sending a push notification to a player.
        """
        print("\n--- SIMULATING PUSH NOTIFICATION ---")
        print(f"TO: Player {player_id}")
        print(f"MESSAGE: {message}")
        print("------------------------------------")
        self._log_action(player_id, "push_notification", message, action_id)

    def send_email(self, player_id: Any, subject: str, body: str, action_id: str):
        """
        Simulates sending an email to a player.
        """
        print("\n--- SIMULATING EMAIL ---")
        print(f"TO: Player {player_id}")
        print(f"SUBJECT: {subject}")
        print(f"BODY: {body}")
        print("------------------------")
        self._log_action(player_id, "email", f"Subject: {subject} | Body: {body}", action_id)