# engagement_executor.py

from typing import Dict, Any, Optional
import logging
import uuid

from engagement_channels import (
    PushSimulatorAdapter,
    SendGridEmailAdapter,
    BrazeAdapter,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='engagement_actions.log',
    filemode='a'
)


class EngagementExecutor:
    """
    Executes engagement actions using pluggable channel adapters.
    """

    def __init__(self):
        self.adapters = {
            "push_notification": PushSimulatorAdapter(),
            "email": SendGridEmailAdapter(),  # auto-fallback to simulator
            "braze": BrazeAdapter(),          # auto-fallback to simulator
        }

    def execute_action(self, action: Dict[str, Any]) -> Optional[str]:
        if not action or action.get("decision") != "ACT":
            print(f"\nDecision is '{action.get('decision', 'NONE')}'. No action executed.")
            return None

        channel = action.get("channel", "push_notification")
        player_id = action.get("player_id")
        action_id = str(uuid.uuid4())

        adapter = self.adapters.get(channel)
        if not adapter:
            print(f"Warning: Channel '{channel}' is not supported. No action taken.")
            return None

        result = adapter.send(player_id, action, action_id)
        self._log_action(
            player_id=player_id,
            channel=result.get("channel", channel),
            provider=result.get("provider", "unknown"),
            content=result.get("content", action.get("content", "")),
            action_id=action_id,
            ok=result.get("ok", False),
            error=result.get("error"),
        )

        if not result.get("ok", False):
            print(f"Action delivery failed for {action_id}: {result.get('error', 'unknown error')}")
            return None

        return action_id

    def _log_action(self, player_id: Any, channel: str, provider: str, content: str, action_id: str, ok: bool, error: Optional[str]):
        status = "SENT" if ok else "FAILED"
        log_message = (
            f"Action {status} - ActionID: {action_id}, PlayerID: {player_id}, "
            f"Channel: {channel}, Provider: {provider}, Content: '{content}', Error: '{error or ''}'"
        )
        logging.info(log_message)
        print(f"LOGGED: {log_message}")
