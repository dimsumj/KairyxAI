from __future__ import annotations

import os
import requests
from typing import Any, Dict, Optional


class ChannelAdapter:
    channel_name: str = "unknown"

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def _wrap_result(
        self,
        payload: Dict[str, Any],
        *,
        provider: str,
        provider_mode: str,
        provider_backend: str | None = None,
        fallback_reason: str | None = None,
        simulated: bool = False,
    ) -> Dict[str, Any]:
        return {
            **payload,
            "provider": provider,
            "provider_mode": provider_mode,
            "provider_backend": provider_backend or provider,
            "fallback_reason": fallback_reason,
            "simulated": simulated,
        }


class PushSimulatorAdapter(ChannelAdapter):
    channel_name = "push_notification"

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        msg = action.get("content", "")
        print("\n--- SIMULATING PUSH NOTIFICATION ---")
        print(f"TO: Player {player_id}")
        print(f"MESSAGE: {msg}")
        print("------------------------------------")
        return self._wrap_result(
            {
                "ok": True,
                "channel": self.channel_name,
                "content": msg,
                "status_code": 200,
            },
            provider="simulator",
            provider_mode="simulator",
            simulated=True,
        )


class EmailSimulatorAdapter(ChannelAdapter):
    channel_name = "email"

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        subject = action.get("subject", "A message from your game")
        body = action.get("content", "")
        print("\n--- SIMULATING EMAIL ---")
        print(f"TO: Player {player_id}")
        print(f"SUBJECT: {subject}")
        print(f"BODY: {body}")
        print("------------------------")
        return self._wrap_result(
            {
                "ok": True,
                "channel": self.channel_name,
                "content": f"Subject: {subject} | Body: {body}",
                "status_code": 200,
            },
            provider="simulator",
            provider_mode="simulator",
            simulated=True,
        )


class SendGridEmailAdapter(ChannelAdapter):
    channel_name = "email"

    def __init__(self):
        self.api_key = os.getenv("SENDGRID_API_KEY")
        self.from_email = os.getenv("SENDGRID_FROM_EMAIL", "noreply@example.com")

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        to_email = str(player_id)
        subject = action.get("subject", "A message from your game")
        body = action.get("content", "")

        if "@" not in to_email:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": f"Subject: {subject} | Body: {body}",
                    "status_code": 422,
                    "error": "invalid_target:missing_email",
                },
                provider="sendgrid",
                provider_mode="live",
            )
        if not self.api_key:
            fallback = EmailSimulatorAdapter().send(player_id, action, action_id)
            return self._wrap_result(
                fallback,
                provider="sendgrid",
                provider_mode="fallback_simulator",
                provider_backend="simulator",
                fallback_reason="missing_api_key",
                simulated=True,
            )

        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": self.from_email},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        }
        try:
            resp = requests.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=15,
            )
        except requests.Timeout:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": f"Subject: {subject} | Body: {body}",
                    "status_code": 504,
                    "error": "provider_timeout",
                },
                provider="sendgrid",
                provider_mode="live",
            )
        except requests.RequestException as exc:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": f"Subject: {subject} | Body: {body}",
                    "status_code": 502,
                    "error": f"provider_request_failed:{exc.__class__.__name__.lower()}",
                },
                provider="sendgrid",
                provider_mode="live",
            )
        ok = 200 <= resp.status_code < 300
        return self._wrap_result(
            {
                "ok": ok,
                "channel": self.channel_name,
                "status_code": resp.status_code,
                "content": f"Subject: {subject} | Body: {body}",
                "error": None if ok else resp.text[:400],
            },
            provider="sendgrid",
            provider_mode="live",
        )


class BrazeAdapter(ChannelAdapter):
    channel_name = "braze"

    def __init__(self):
        self.api_key = os.getenv("BRAZE_API_KEY")
        self.rest_endpoint = os.getenv("BRAZE_REST_ENDPOINT", "").rstrip("/")

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        msg = action.get("content", "")
        if not self.api_key or not self.rest_endpoint:
            fallback = PushSimulatorAdapter().send(player_id, {"content": msg}, action_id)
            return self._wrap_result(
                fallback,
                provider="braze",
                provider_mode="fallback_simulator",
                provider_backend="simulator",
                fallback_reason="missing_provider_config",
                simulated=True,
            )

        url = f"{self.rest_endpoint}/users/track"
        payload = {
            "attributes": [],
            "events": [
                {
                    "external_id": str(player_id),
                    "name": "kairyx_ai_engagement",
                    "time": action.get("time") or None,
                    "properties": {"message": msg, "action_id": action_id},
                }
            ],
        }
        try:
            resp = requests.post(
                url,
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=15,
            )
        except requests.Timeout:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "status_code": 504,
                    "content": msg,
                    "error": "provider_timeout",
                },
                provider="braze",
                provider_mode="live",
            )
        except requests.RequestException as exc:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "status_code": 502,
                    "content": msg,
                    "error": f"provider_request_failed:{exc.__class__.__name__.lower()}",
                },
                provider="braze",
                provider_mode="live",
            )
        ok = 200 <= resp.status_code < 300
        return self._wrap_result(
            {
                "ok": ok,
                "channel": self.channel_name,
                "status_code": resp.status_code,
                "content": msg,
                "error": None if ok else resp.text[:400],
            },
            provider="braze",
            provider_mode="live",
        )


class WebhookAdapter(ChannelAdapter):
    channel_name = "webhook"

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        webhook_url = str(action.get("webhook_url") or os.getenv("ENGAGEMENT_WEBHOOK_URL") or "").strip()
        if not webhook_url:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "status_code": 422,
                    "content": str(action.get("content", "")),
                    "error": "invalid_target:missing_webhook_url",
                },
                provider="webhook",
                provider_mode="live",
            )
        body = {
            "action_id": action_id,
            "player_id": player_id,
            "content": action.get("content", ""),
            "subject": action.get("subject"),
            "metadata": dict(action.get("metadata") or {}),
        }
        try:
            resp = requests.post(
                webhook_url,
                headers={"Content-Type": "application/json"},
                json=body,
                timeout=15,
            )
        except requests.Timeout:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "status_code": 504,
                    "content": str(action.get("content", "")),
                    "error": "provider_timeout",
                },
                provider="webhook",
                provider_mode="live",
            )
        except requests.RequestException as exc:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "status_code": 502,
                    "content": str(action.get("content", "")),
                    "error": f"provider_request_failed:{exc.__class__.__name__.lower()}",
                },
                provider="webhook",
                provider_mode="live",
            )
        ok = 200 <= resp.status_code < 300
        return self._wrap_result(
            {
                "ok": ok,
                "channel": self.channel_name,
                "status_code": resp.status_code,
                "content": str(action.get("content", "")),
                "error": None if ok else resp.text[:400],
            },
            provider="webhook",
            provider_mode="live",
        )
