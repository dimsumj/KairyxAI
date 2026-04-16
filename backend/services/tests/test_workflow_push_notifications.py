from __future__ import annotations

import requests

import pytest
from fastapi.testclient import TestClient

from app.core import db as db_module
from app.main import create_app
from engagement_channels import PushNotificationAdapter


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


def _create_active_cohort(client: TestClient, *, name: str = "push_cohort") -> str:
    response = client.post(
        "/api/v1/cohorts",
        headers=_headers(),
        json={
            "name": name,
            "description": "Push notification cohort",
            "type": "list",
            "definition": {
                "members": [
                    {
                        "canonical_user_id": "player_1",
                        "email": "player1@example.com",
                        "country": "US",
                        "platform": "ios",
                    }
                ]
            },
            "refresh_mode": "manual",
            "activate": True,
        },
    )
    assert response.status_code == 201
    return response.json()["cohort_id"]


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
            },
        },
    )
    assert response.status_code == 201
    return response.json()["provider_connection_id"]


def _create_push_workflow(
    client: TestClient,
    *,
    cohort_id: str,
    provider_connection_id: str | None = None,
    retry_policy: dict | None = None,
    title: str = "Come back",
    body: str = "Rewards are waiting.",
) -> str:
    channel_config = {
        "channel": "push_notification",
        "campaign_name": "winback_push",
        "title": title,
        "body": body,
        "deep_link": "wynn://promotions/welcome-back",
        "deep_link_token": "custom-token",
        "scheduled_at": "2026-04-16T18:30:00+00:00",
        "data": {"reward_id": "reward_pack"},
        "provider_options": {"priority": "high"},
    }
    if provider_connection_id:
        channel_config["provider_connection_id"] = provider_connection_id
    if retry_policy:
        channel_config["retry_policy"] = retry_policy
    response = client.post(
        "/api/v1/workflows",
        headers=_headers(),
        json={
            "name": "push_delivery_flow",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily"},
            "action": channel_config,
            "channel_config": channel_config,
            "policy": {
                "global_daily_limit": 5,
                "channel_daily_limit": 5,
                "cooldown_hours": 0,
            },
            "budget_policy": {"daily_budget_limit": 5},
        },
    )
    assert response.status_code == 201
    return response.json()["workflow_id"]


def test_provider_connection_create_supports_wynn_push_notifier(client: TestClient):
    response = client.post(
        "/api/v1/provider-connections",
        headers=_headers(),
        json={
            "name": "Wynn Push",
            "provider": "wynn_push_notifier",
            "config": {
                "base_url": "https://push.example.com",
                "api_token": "push-secret-token",
            },
        },
    )
    assert response.status_code == 201
    payload = response.json()
    assert payload["provider"] == "wynn_push_notifier"
    assert payload["config"]["base_url"] == "https://push.example.com"
    assert payload["config"]["api_token"] is None
    assert payload["config"]["api_token_configured"] is True


def test_legacy_push_workflow_without_provider_connection_uses_simulator(client: TestClient):
    cohort_id = _create_active_cohort(client, name="simulator_cohort")
    response = client.post(
        "/api/v1/workflows",
        headers=_headers(),
        json={
            "name": "simulator_push_flow",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily"},
            "action": {"channel": "push_notification", "content": "Fallback simulator copy"},
            "channel_config": {"channel": "push_notification", "content": "Fallback simulator copy"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 0},
        },
    )
    assert response.status_code == 201
    workflow_id = response.json()["workflow_id"]

    run = client.post(
        f"/api/v1/workflows/{workflow_id}/test-run",
        headers=_headers(),
        json={"limit": 1, "confirm": True, "sandbox": False},
    )
    assert run.status_code == 200
    assert run.json()["success"] == 1

    deliveries = client.get(f"/api/v1/workflows/{workflow_id}/deliveries", headers=_headers())
    assert deliveries.status_code == 200
    delivery = deliveries.json()["items"][0]
    assert delivery["provider"] == "simulator"
    assert delivery["provider_request"]["body"] == "Fallback simulator copy"


def test_push_workflow_delivery_records_wynn_campaign_metadata(client: TestClient, monkeypatch):
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
                "scheduled_at": "2026-04-16T18:30:00+00:00",
            },
        )

    monkeypatch.setattr("engagement_channels.requests.post", fake_post)

    provider_connection_id = _create_wynn_provider_connection(client)
    cohort_id = _create_active_cohort(client, name="live_push_cohort")
    workflow_id = _create_push_workflow(client, cohort_id=cohort_id, provider_connection_id=provider_connection_id)

    run = client.post(
        f"/api/v1/workflows/{workflow_id}/test-run",
        headers=_headers(),
        json={"limit": 1, "confirm": True, "sandbox": False},
    )
    assert run.status_code == 200
    assert run.json()["success"] == 1

    assert len(captured_requests) == 1
    outbound = captured_requests[0]
    assert outbound["url"] == "https://push.example.com/pushNotificationAPI/kairyx/campaigns"
    assert outbound["headers"]["Authorization"] == "Bearer push-secret-token"
    assert outbound["json"]["campaign_name"] == "winback_push"
    assert outbound["json"]["title"] == "Come back"
    assert outbound["json"]["body"] == "Rewards are waiting."
    assert outbound["json"]["player_ids"] == ["player_1"]
    assert outbound["json"]["deep_link"] == "wynn://promotions/welcome-back"
    assert outbound["json"]["provider_options"] == {"priority": "high"}

    deliveries = client.get(f"/api/v1/workflows/{workflow_id}/deliveries", headers=_headers())
    assert deliveries.status_code == 200
    delivery = deliveries.json()["items"][0]
    assert delivery["provider"] == "wynn_push_notifier"
    assert delivery["provider_connection_id"] == provider_connection_id
    assert delivery["provider_campaign_id"] == "PUSH_NOTIFICATION.cid_42"
    assert delivery["provider_accepted"] is True
    assert delivery["provider_request"]["campaign_name"] == "winback_push"
    assert delivery["provider_request"]["title"] == "Come back"
    assert delivery["provider_request"]["body"] == "Rewards are waiting."
    assert delivery["provider_response"]["accepted"] is True
    assert delivery["provider_response"]["campaign_id"] == "PUSH_NOTIFICATION.cid_42"


def test_push_notification_adapter_surfaces_auth_failure(monkeypatch):
    def fake_post(url, headers=None, json=None, timeout=None):
        return _DummyResponse(401, {"accepted": False, "error": "unauthorized", "message": "Unauthorized"})

    monkeypatch.setattr("engagement_channels.requests.post", fake_post)
    adapter = PushNotificationAdapter()

    result = adapter.send(
        "player_1",
        {
            "provider": "wynn_push_notifier",
            "base_url": "https://push.example.com",
            "api_token": "push-secret-token",
            "campaign_name": "winback_push",
            "title": "Come back",
            "body": "Rewards are waiting.",
            "provider_request_id": "pr_auth_failure",
        },
        "action_auth_failure",
    )

    assert result["ok"] is False
    assert result["provider"] == "wynn_push_notifier"
    assert result["status_code"] == 401
    assert result["error"] == "unauthorized"


def test_push_notification_adapter_surfaces_timeout(monkeypatch):
    def fake_post(url, headers=None, json=None, timeout=None):
        raise requests.Timeout()

    monkeypatch.setattr("engagement_channels.requests.post", fake_post)
    adapter = PushNotificationAdapter()

    result = adapter.send(
        "player_1",
        {
            "provider": "wynn_push_notifier",
            "base_url": "https://push.example.com",
            "api_token": "push-secret-token",
            "campaign_name": "winback_push",
            "title": "Come back",
            "body": "Rewards are waiting.",
            "provider_request_id": "pr_timeout",
        },
        "action_timeout",
    )

    assert result["ok"] is False
    assert result["status_code"] == 504
    assert result["error"] == "provider_timeout"


def test_push_workflow_retries_5xx_with_stable_provider_request_id(client: TestClient, monkeypatch):
    captured_requests: list[dict] = []
    responses = iter(
        [
            _DummyResponse(500, {"accepted": False, "error": "provider_error"}, text="server error"),
            _DummyResponse(
                202,
                {
                    "accepted": True,
                    "campaign_id": "PUSH_NOTIFICATION.cid_retry",
                    "duplicate": False,
                    "scheduled_at": "2026-04-16T18:30:00+00:00",
                },
            ),
        ]
    )

    def fake_post(url, headers=None, json=None, timeout=None):
        captured_requests.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        return next(responses)

    monkeypatch.setattr("engagement_channels.requests.post", fake_post)

    provider_connection_id = _create_wynn_provider_connection(client)
    cohort_id = _create_active_cohort(client, name="retry_push_cohort")
    workflow_id = _create_push_workflow(
        client,
        cohort_id=cohort_id,
        provider_connection_id=provider_connection_id,
        retry_policy={"max_retries": 1, "base_backoff_seconds": 2},
    )

    run = client.post(
        f"/api/v1/workflows/{workflow_id}/test-run",
        headers=_headers(),
        json={"limit": 1, "confirm": True, "sandbox": False},
    )
    assert run.status_code == 200
    assert run.json()["success"] == 1

    assert len(captured_requests) == 2
    assert captured_requests[0]["json"]["provider_request_id"] == captured_requests[1]["json"]["provider_request_id"]

    deliveries = client.get(f"/api/v1/workflows/{workflow_id}/deliveries", headers=_headers())
    assert deliveries.status_code == 200
    delivery = deliveries.json()["items"][0]
    assert delivery["delivery_diagnostics"]["attempt_count"] == 2
    assert delivery["delivery_diagnostics"]["retry_schedule_seconds"] == [2]
    assert delivery["provider_response"]["campaign_id"] == "PUSH_NOTIFICATION.cid_retry"
