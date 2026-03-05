from __future__ import annotations

import os
import requests
from typing import Any, Dict, Optional


class ChannelAdapter:
    channel_name: str = "unknown"

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        raise NotImplementedError


class PushSimulatorAdapter(ChannelAdapter):
    channel_name = "push_notification"

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        msg = action.get("content", "")
        print("\n--- SIMULATING PUSH NOTIFICATION ---")
        print(f"TO: Player {player_id}")
        print(f"MESSAGE: {msg}")
        print("------------------------------------")
        return {
            "ok": True,
            "provider": "simulator",
            "channel": self.channel_name,
            "content": msg,
        }


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
        return {
            "ok": True,
            "provider": "simulator",
            "channel": self.channel_name,
            "content": f"Subject: {subject} | Body: {body}",
        }


class SendGridEmailAdapter(ChannelAdapter):
    channel_name = "email"

    def __init__(self):
        self.api_key = os.getenv("SENDGRID_API_KEY")
        self.from_email = os.getenv("SENDGRID_FROM_EMAIL", "noreply@example.com")

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        # For demo: player_id can be an email address; if not, fallback to simulator semantics.
        to_email = str(player_id)
        subject = action.get("subject", "A message from your game")
        body = action.get("content", "")

        if not self.api_key or "@" not in to_email:
            return EmailSimulatorAdapter().send(player_id, action, action_id)

        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": self.from_email},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        }
        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )
        ok = 200 <= resp.status_code < 300
        return {
            "ok": ok,
            "provider": "sendgrid",
            "channel": self.channel_name,
            "status_code": resp.status_code,
            "content": f"Subject: {subject} | Body: {body}",
            "error": None if ok else resp.text[:400],
        }


class BrazeAdapter(ChannelAdapter):
    channel_name = "braze"

    def __init__(self):
        self.api_key = os.getenv("BRAZE_API_KEY")
        self.rest_endpoint = os.getenv("BRAZE_REST_ENDPOINT", "").rstrip("/")

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        # Minimal Braze track API use for demo. If not configured, fallback to push simulator.
        msg = action.get("content", "")
        if not self.api_key or not self.rest_endpoint:
            return PushSimulatorAdapter().send(player_id, {"content": msg}, action_id)

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
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )
        ok = 200 <= resp.status_code < 300
        return {
            "ok": ok,
            "provider": "braze",
            "channel": self.channel_name,
            "status_code": resp.status_code,
            "content": msg,
            "error": None if ok else resp.text[:400],
        }
