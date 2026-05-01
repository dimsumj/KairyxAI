from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from app.core import db as db_module
from app.main import create_app


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setenv("SCHEDULER_ENABLED", "false")
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    db_module.clear_runtime_database_fallback()


class _DummyResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text or ""

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


def _headers() -> dict[str, str]:
    return {"x-actor-role": "operator"}


def _create_wynn_provider_connection(client: TestClient) -> str:
    response = client.post(
        "/api/v1/provider-connections",
        headers=_headers(),
        json={
            "name": "Wynn Push",
            "provider": "wynn_push_notifier",
            "config": {
                "base_url": "https://push.example.com",
                "api_token": "push-secret-token",
                "default_deep_link_token": "default-token",
                "callback_url": "https://operator.example.com/api/v1/activation/callbacks/wynn_push_notifier",
                "callback_bearer_token": "callback-bearer-token",
                "callback_signing_secret": "callback-signing-secret",
            },
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["provider_connection_id"]


def test_send_now_single_user_push_uses_wynn_provider_connection(client: TestClient, monkeypatch):
    provider_connection_id = _create_wynn_provider_connection(client)
    captured_requests: list[dict] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        captured_requests.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        return _DummyResponse(
            202,
            {
                "accepted": True,
                "campaign_id": "PUSH_NOTIFICATION.cid_42",
                "duplicate": False,
                "scheduled_at": None,
            },
        )

    monkeypatch.setattr("engagement_channels.requests.post", fake_post)

    response = client.post(
        "/api/v1/push-dispatches/send-now",
        headers=_headers(),
        json={
            "name": "vip_reactivation",
            "user_id": "player_123",
            "provider_connection_id": provider_connection_id,
            "campaign_name": "vip_reactivation_push",
            "title": "We miss you",
            "body": "A reward is waiting.",
            "deep_link": "wynn://promotions/vip",
            "deep_link_token": "vip-token",
            "data": {"reward_id": "vip_pack"},
            "provider_options": {"priority": "high"},
        },
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["status"] == "sent"
    assert payload["provider"] == "wynn_push_notifier"
    assert payload["provider_mode"] == "live"
    assert payload["provider_connection_id"] == provider_connection_id
    assert payload["user_id"] == "player_123"
    assert payload["user_ids"] == ["player_123"]
    assert payload["audience_mode"] == "explicit_user_ids"
    assert payload["campaign_name"] == "vip_reactivation_push"
    assert payload["provider_campaign_id"] == "PUSH_NOTIFICATION.cid_42"
    assert payload["provider_accepted"] is True
    assert payload["simulated"] is False
    assert payload["provider_request_id"] == payload["push_dispatch_id"]

    assert len(captured_requests) == 1
    outbound = captured_requests[0]
    assert outbound["url"] == "https://push.example.com/pushNotificationAPI/kairyx/campaigns"
    assert outbound["headers"]["Authorization"] == "Bearer push-secret-token"
    assert outbound["json"]["player_ids"] == ["player_123"]
    assert outbound["json"]["campaign_name"] == "vip_reactivation_push"
    assert outbound["json"]["title"] == "We miss you"
    assert outbound["json"]["body"] == "A reward is waiting."
    assert outbound["json"]["provider_request_id"] == payload["provider_request_id"]
    assert outbound["json"]["data"] == {"reward_id": "vip_pack"}
    assert outbound["json"]["context"]["kairyx_callback"]["url"] == "https://operator.example.com/api/v1/activation/callbacks/wynn_push_notifier"
    assert outbound["json"]["context"]["kairyx_callback"]["bearer_token"] == "callback-bearer-token"
    assert outbound["json"]["provider_options"] == {"priority": "high"}

    list_response = client.get("/api/v1/push-dispatches", headers=_headers())
    assert list_response.status_code == 200, list_response.text
    assert list_response.json()["items"][0]["push_dispatch_id"] == payload["push_dispatch_id"]

    detail_response = client.get(f"/api/v1/push-dispatches/{payload['push_dispatch_id']}", headers=_headers())
    assert detail_response.status_code == 200, detail_response.text
    assert detail_response.json()["provider_campaign_id"] == "PUSH_NOTIFICATION.cid_42"


def test_send_now_single_user_push_uses_simulator_without_provider_connection(client: TestClient):
    response = client.post(
        "/api/v1/push-dispatches/send-now",
        headers=_headers(),
        json={
            "name": "manual_simulator_send",
            "user_id": "player_sim",
            "body": "Simulator fallback copy.",
            "data": {"reward_id": "simulator_pack"},
        },
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["status"] == "sent"
    assert payload["provider"] == "simulator"
    assert payload["provider_mode"] == "simulator"
    assert payload["simulated"] is True
    assert payload["provider_connection_id"] is None
    assert payload["user_ids"] == ["player_sim"]
    assert payload["audience_mode"] == "explicit_user_ids"
    assert payload["body"] == "Simulator fallback copy."
    assert payload["data"] == {"reward_id": "simulator_pack"}


def test_send_now_live_push_requires_title(client: TestClient):
    provider_connection_id = _create_wynn_provider_connection(client)

    response = client.post(
        "/api/v1/push-dispatches/send-now",
        headers=_headers(),
        json={
            "name": "missing_title",
            "user_id": "player_123",
            "provider_connection_id": provider_connection_id,
            "body": "A reward is waiting.",
        },
    )

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "Live push workflows require title."


def test_send_now_supports_multi_user_provider_campaign_with_wynn_filters(client: TestClient, monkeypatch):
    provider_connection_id = _create_wynn_provider_connection(client)
    captured_requests: list[dict] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        captured_requests.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return _DummyResponse(
            202,
            {
                "accepted": True,
                "campaign_id": "PUSH_NOTIFICATION.cid_multi",
                "duplicate": False,
                "scheduled_at": None,
            },
        )

    monkeypatch.setattr("engagement_channels.requests.post", fake_post)

    response = client.post(
        "/api/v1/push-dispatches/send-now",
        headers=_headers(),
        json={
            "name": "vip_reactivation_multi",
            "user_ids": ["player_123", "player_456", "player_123"],
            "provider_connection_id": provider_connection_id,
            "campaign_name": "vip_multi_push",
            "title": "We miss you",
            "body": "A reward is waiting.",
            "provider_options": {
                "priority": "high",
                "filters": {
                    "minVIPLevel": 3,
                    "platform": "ios",
                },
            },
        },
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["user_id"] is None
    assert payload["user_ids"] == ["player_123", "player_456"]
    assert payload["audience_mode"] == "explicit_user_ids"
    assert payload["provider_request_id"] == payload["push_dispatch_id"]

    assert len(captured_requests) == 1
    outbound = captured_requests[0]
    assert outbound["json"]["player_ids"] == ["player_123", "player_456"]
    assert outbound["json"]["provider_options"] == {
        "priority": "high",
        "filters": {
            "minVIPLevel": 3,
            "platform": "ios",
        },
    }
    assert dict(outbound["json"].get("data") or {}) == {}


def test_send_now_blank_user_ids_broadcasts_to_all_players_for_live_wynn_provider(client: TestClient, monkeypatch):
    provider_connection_id = _create_wynn_provider_connection(client)
    captured_requests: list[dict] = []

    def fake_post(url, headers=None, json=None, timeout=None):
        captured_requests.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return _DummyResponse(
            202,
            {
                "accepted": True,
                "campaign_id": "PUSH_NOTIFICATION.cid_all",
                "duplicate": False,
                "scheduled_at": None,
            },
        )

    monkeypatch.setattr("engagement_channels.requests.post", fake_post)

    response = client.post(
        "/api/v1/push-dispatches/send-now",
        headers=_headers(),
        json={
            "name": "broadcast_all",
            "provider_connection_id": provider_connection_id,
            "campaign_name": "broadcast_push",
            "title": "Weekend event",
            "body": "Rewards are live.",
            "provider_options": {
                "filters": {
                    "minVIPLevel": 5,
                    "daysFromLastLogin": 14,
                }
            },
        },
    )

    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["user_id"] is None
    assert payload["user_ids"] == []
    assert payload["audience_mode"] == "provider_broadcast_all_players"

    assert len(captured_requests) == 1
    assert captured_requests[0]["json"]["player_ids"] == []
    assert captured_requests[0]["json"]["provider_options"]["filters"] == {
        "minVIPLevel": 5,
        "daysFromLastLogin": 14,
    }
    assert captured_requests[0]["json"]["data"] == {}


def test_send_now_blank_user_ids_rejects_simulator_broadcast(client: TestClient):
    response = client.post(
        "/api/v1/push-dispatches/send-now",
        headers=_headers(),
        json={
            "name": "simulator_broadcast",
            "title": "Weekend event",
            "body": "Rewards are live.",
        },
    )

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "Broadcast push sends require a live Wynn PushNotifier provider connection."


def test_send_now_rejects_non_object_push_json(client: TestClient):
    response = client.post(
        "/api/v1/push-dispatches/send-now",
        headers=_headers(),
        json={
            "name": "bad_payload_shape",
            "user_id": "player_123",
            "body": "A reward is waiting.",
            "data": [],
        },
    )

    assert response.status_code == 422, response.text


def test_push_dispatch_callbacks_accept_provider_callback_bearer_token_and_update_summary(client: TestClient, monkeypatch):
    provider_connection_id = _create_wynn_provider_connection(client)

    def fake_post(url, headers=None, json=None, timeout=None):
        return _DummyResponse(
            202,
            {
                "accepted": True,
                "campaign_id": "PUSH_NOTIFICATION.cid_callbacks",
                "duplicate": False,
                "scheduled_at": None,
            },
        )

    monkeypatch.setattr("engagement_channels.requests.post", fake_post)

    dispatch_response = client.post(
        "/api/v1/push-dispatches/send-now",
        headers=_headers(),
        json={
            "name": "callback_dispatch",
            "user_ids": ["player_123", "player_456"],
            "provider_connection_id": provider_connection_id,
            "campaign_name": "callback_dispatch_push",
            "title": "We miss you",
            "body": "A reward is waiting.",
            "data": {"reward_id": "vip_pack"},
        },
    )
    assert dispatch_response.status_code == 201, dispatch_response.text
    dispatch_payload = dispatch_response.json()

    callback_payload = {
        "callbacks": [
            {
                "provider_connection_id": provider_connection_id,
                "provider_request_id": dispatch_payload["provider_request_id"],
                "push_dispatch_id": dispatch_payload["push_dispatch_id"],
                "provider_campaign_id": dispatch_payload["provider_campaign_id"],
                "user_id": "player_123",
                "event_id": "evt_click_1",
                "event_type": "clicked",
                "occurred_at": "2026-04-30T20:10:00Z",
            },
            {
                "provider_connection_id": provider_connection_id,
                "provider_request_id": dispatch_payload["provider_request_id"],
                "push_dispatch_id": dispatch_payload["push_dispatch_id"],
                "provider_campaign_id": dispatch_payload["provider_campaign_id"],
                "user_id": "player_123",
                "event_id": "evt_claim_1",
                "event_type": "claimed",
                "outcome_name": "purchase",
                "occurred_at": "2026-04-30T20:15:00Z",
            },
        ]
    }
    raw_body = json.dumps(callback_payload).encode("utf-8")
    signature = hmac.new(b"callback-signing-secret", raw_body, hashlib.sha256).hexdigest()

    callback_response = client.post(
        "/api/v1/activation/callbacks/wynn_push_notifier",
        headers={
            "Authorization": "Bearer callback-bearer-token",
            "Content-Type": "application/json",
            "X-Kairyx-Signature": signature,
        },
        content=raw_body,
    )

    assert callback_response.status_code == 200, callback_response.text
    assert callback_response.json()["ingested"] == 2

    detail_response = client.get(f"/api/v1/push-dispatches/{dispatch_payload['push_dispatch_id']}", headers=_headers())
    assert detail_response.status_code == 200, detail_response.text
    detail_payload = detail_response.json()
    assert detail_payload["callback_count"] == 2
    assert detail_payload["last_provider_event"] == "claimed"
    assert detail_payload["callback_summary"]["event_counts"]["clicked"] == 1
    assert detail_payload["callback_summary"]["event_counts"]["claimed"] == 1
    assert detail_payload["callback_summary"]["event_counts"]["purchase"] == 1
    assert detail_payload["callback_summary"]["unique_user_counts"]["clicked"] == 1
