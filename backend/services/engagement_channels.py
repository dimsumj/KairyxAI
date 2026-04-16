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
        msg = action.get("body") or action.get("content", "")
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


class WynnPushNotifierAdapter(ChannelAdapter):
    channel_name = "push_notification"

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        base_url = str(action.get("base_url") or "").rstrip("/")
        api_token = str(action.get("api_token") or "").strip()
        title = str(action.get("title") or "").strip()
        body = str(action.get("body") or action.get("content") or "").strip()
        provider_request_id = str(action.get("provider_request_id") or action_id).strip() or action_id
        if not base_url:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": body,
                    "status_code": 422,
                    "error": "provider_config_missing:base_url",
                },
                provider="wynn_push_notifier",
                provider_mode="live",
            )
        if not api_token:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": body,
                    "status_code": 422,
                    "error": "provider_config_missing:api_token",
                },
                provider="wynn_push_notifier",
                provider_mode="live",
            )
        if not title:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": body,
                    "status_code": 422,
                    "error": "invalid_target:missing_title",
                },
                provider="wynn_push_notifier",
                provider_mode="live",
            )
        if not body:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": body,
                    "status_code": 422,
                    "error": "invalid_target:missing_body",
                },
                provider="wynn_push_notifier",
                provider_mode="live",
            )

        context = dict(action.get("context") or {})
        context = {
            **context,
            "workflow_id": action.get("workflow_id") or context.get("workflow_id"),
            "execution_id": action.get("execution_id") or context.get("execution_id"),
            "tenant_id": action.get("tenant_id") or context.get("tenant_id"),
            "project_id": action.get("project_id") or context.get("project_id"),
        }
        context = {key: value for key, value in context.items() if value not in (None, "")}
        player_ids = player_id if isinstance(player_id, list) else [player_id]
        player_ids = [str(item).strip() for item in player_ids if str(item).strip()]
        request_payload = {
            "provider_request_id": provider_request_id,
            "campaign_name": str(action.get("campaign_name") or f"kairyx_push_{action_id}").strip(),
            "title": title,
            "body": body,
            "player_ids": player_ids,
            "data": dict(action.get("data") or {}),
            "scheduled_at": action.get("scheduled_at"),
            "deep_link": action.get("deep_link"),
            "deep_link_token": action.get("deep_link_token") or action.get("default_deep_link_token"),
            "provider_options": dict(action.get("provider_options") or {}),
            "context": context,
        }
        try:
            resp = requests.post(
                f"{base_url}/pushNotificationAPI/kairyx/campaigns",
                headers={
                    "Authorization": f"Bearer {api_token}",
                    "Content-Type": "application/json",
                },
                json=request_payload,
                timeout=15,
            )
        except requests.Timeout:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": body,
                    "status_code": 504,
                    "error": "provider_timeout",
                },
                provider="wynn_push_notifier",
                provider_mode="live",
            )
        except requests.RequestException as exc:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": body,
                    "status_code": 502,
                    "error": f"provider_request_failed:{exc.__class__.__name__.lower()}",
                },
                provider="wynn_push_notifier",
                provider_mode="live",
            )

        response_payload: Dict[str, Any]
        try:
            response_payload = dict(resp.json() or {})
        except ValueError:
            response_payload = {}
        accepted = bool(response_payload.get("accepted")) if response_payload else False
        ok = 200 <= resp.status_code < 300 and accepted
        error_message = response_payload.get("error") or response_payload.get("message") or resp.text[:400]
        return self._wrap_result(
            {
                "ok": ok,
                "channel": self.channel_name,
                "status_code": resp.status_code,
                "content": body,
                "error": None if ok else str(error_message or "provider_error"),
                "accepted": accepted,
                "duplicate": bool(response_payload.get("duplicate")),
                "provider_campaign_id": response_payload.get("campaign_id"),
                "scheduled_at": response_payload.get("scheduled_at"),
                "provider_response_body": response_payload,
            },
            provider="wynn_push_notifier",
            provider_mode="live",
        )


class PushNotificationAdapter(ChannelAdapter):
    channel_name = "push_notification"

    def __init__(self):
        self.simulator = PushSimulatorAdapter()
        self.wynn_push_notifier = WynnPushNotifierAdapter()

    def send(self, player_id: Any, action: Dict[str, Any], action_id: str) -> Dict[str, Any]:
        provider = str(action.get("provider") or "").strip().lower()
        has_live_config = bool(
            str(action.get("provider_connection_id") or "").strip()
            or str(action.get("base_url") or "").strip()
            or str(action.get("api_token") or "").strip()
        )
        if not has_live_config and provider not in {"", "simulator"}:
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": str(action.get("body") or action.get("content") or ""),
                    "status_code": 422,
                    "error": f"unsupported_provider_connection:{provider}",
                },
                provider=provider or "unsupported",
                provider_mode="live",
            )
        if provider in {"", "simulator"} and not has_live_config:
            return self.simulator.send(player_id, action, action_id)
        if provider and provider != "wynn_push_notifier":
            return self._wrap_result(
                {
                    "ok": False,
                    "channel": self.channel_name,
                    "content": str(action.get("body") or action.get("content") or ""),
                    "status_code": 422,
                    "error": f"unsupported_provider_connection:{provider}",
                },
                provider=provider,
                provider_mode="live",
            )
        return self.wynn_push_notifier.send(player_id, action, action_id)


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
        api_key = str(action.get("api_key") or self.api_key or "").strip()
        from_email = str(action.get("from_email") or self.from_email or "noreply@example.com").strip() or "noreply@example.com"

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
        if not api_key:
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
            "from": {"email": from_email},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        }
        try:
            resp = requests.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
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
        api_key = str(action.get("api_key") or self.api_key or "").strip()
        rest_endpoint = str(action.get("rest_endpoint") or self.rest_endpoint or "").rstrip("/")
        if not api_key or not rest_endpoint:
            fallback = PushSimulatorAdapter().send(player_id, {"content": msg}, action_id)
            return self._wrap_result(
                fallback,
                provider="braze",
                provider_mode="fallback_simulator",
                provider_backend="simulator",
                fallback_reason="missing_provider_config",
                simulated=True,
            )

        url = f"{rest_endpoint}/users/track"
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
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
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
        webhook_token = str(action.get("webhook_token") or "").strip()
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
            headers = {"Content-Type": "application/json"}
            if webhook_token:
                headers["Authorization"] = f"Bearer {webhook_token}"
            resp = requests.post(
                webhook_url,
                headers=headers,
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
