# engagement_executor.py

from typing import Dict, Any, Optional
import logging
import uuid

from engagement_channels import (
    PushNotificationAdapter,
    SendGridEmailAdapter,
    BrazeAdapter,
    WebhookAdapter,
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
            "push_notification": PushNotificationAdapter(),
            "email": SendGridEmailAdapter(),
            "braze": BrazeAdapter(),
            "webhook": WebhookAdapter(),
        }

    def execute_action(self, action: Dict[str, Any]) -> Optional[str]:
        result = self.execute_action_detailed(action)
        if not result.get("ok", False):
            return None
        return str(result.get("action_id"))

    def execute_action_detailed(self, action: Dict[str, Any]) -> Dict[str, Any]:
        if not action or action.get("decision") != "ACT":
            print(f"\nDecision is '{action.get('decision', 'NONE')}'. No action executed.")
            return {
                "ok": False,
                "action_id": None,
                "provider": "none",
                "provider_mode": "none",
                "provider_backend": "none",
                "simulated": False,
                "channel": str(action.get("channel", "unknown")) if action else "unknown",
                "content": str(action.get("content", "")) if action else "",
                "error": "decision_not_act",
            }

        channel = action.get("channel", "push_notification")
        player_id = action.get("player_id")
        action_id = str(uuid.uuid4())

        adapter = self.adapters.get(channel)
        if not adapter:
            print(f"Warning: Channel '{channel}' is not supported. No action taken.")
            return {
                "ok": False,
                "action_id": action_id,
                "provider": "unsupported",
                "provider_mode": "none",
                "provider_backend": "unsupported",
                "simulated": False,
                "channel": str(channel),
                "content": str(action.get("content", "")),
                "error": f"unsupported_channel:{channel}",
            }

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
            return {
                "action_id": action_id,
                "provider": result.get("provider", "unknown"),
                "provider_mode": result.get("provider_mode", "live"),
                "provider_backend": result.get("provider_backend", result.get("provider", "unknown")),
                "fallback_reason": result.get("fallback_reason"),
                "simulated": bool(result.get("simulated")),
                "channel": result.get("channel", channel),
                "content": result.get("content", action.get("content", "")),
                "status_code": result.get("status_code"),
                "accepted": result.get("accepted"),
                "duplicate": result.get("duplicate"),
                "provider_campaign_id": result.get("provider_campaign_id"),
                "scheduled_at": result.get("scheduled_at"),
                "provider_response_body": result.get("provider_response_body"),
                "ok": False,
                "error": result.get("error", "unknown_error"),
            }

        return {
            "action_id": action_id,
            "provider": result.get("provider", "unknown"),
            "provider_mode": result.get("provider_mode", "live"),
            "provider_backend": result.get("provider_backend", result.get("provider", "unknown")),
            "fallback_reason": result.get("fallback_reason"),
            "simulated": bool(result.get("simulated")),
            "channel": result.get("channel", channel),
            "content": result.get("content", action.get("content", "")),
            "status_code": result.get("status_code"),
            "accepted": result.get("accepted"),
            "duplicate": result.get("duplicate"),
            "provider_campaign_id": result.get("provider_campaign_id"),
            "scheduled_at": result.get("scheduled_at"),
            "provider_response_body": result.get("provider_response_body"),
            "ok": True,
            "error": None,
        }

    def _log_action(self, player_id: Any, channel: str, provider: str, content: str, action_id: str, ok: bool, error: Optional[str]):
        status = "SENT" if ok else "FAILED"
        log_message = (
            f"Action {status} - ActionID: {action_id}, PlayerID: {player_id}, "
            f"Channel: {channel}, Provider: {provider}, Content: '{content}', Error: '{error or ''}'"
        )
        logging.info(log_message)
        print(f"LOGGED: {log_message}")
