from __future__ import annotations

import json
from urllib.parse import urlparse

from fastapi.testclient import TestClient
import pytest

from app.core import db as db_module
from app.core.db import session_scope
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service


OPERATOR_HEADERS = {"x-actor-role": "operator"}


class _FakeSendGridResponse:
    def __init__(self, status_code: int, payload=None, *, headers: dict | None = None, text: str | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = dict(headers or {})
        self.text = text if text is not None else (json.dumps(payload) if payload is not None else "")

    def json(self):
        return self._payload


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
    clear_shared_bigquery_service_cache()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    clear_shared_bigquery_service_cache()
    db_module.clear_runtime_database_fallback()


def _build_sendgrid_stub(call_log: list[dict], responses_by_key: dict[tuple[str, str], list[_FakeSendGridResponse]]):
    remaining = {key: list(value) for key, value in responses_by_key.items()}

    def _fake_request(method: str, url: str, **kwargs):
        key = (str(method or "GET").upper(), urlparse(url).path)
        call_log.append(
            {
                "method": key[0],
                "url": url,
                "path": key[1],
                "params": kwargs.get("params"),
                "json": kwargs.get("json"),
                "headers": kwargs.get("headers"),
            }
        )
        bucket = remaining.get(key)
        if not bucket:
            raise AssertionError(f"Unexpected SendGrid request: {key}")
        return bucket.pop(0)

    return _fake_request


def _create_sendgrid_provider_connection(client: TestClient, *, name: str = "Lifecycle SendGrid") -> str:
    response = client.post(
        "/api/v1/provider-connections",
        headers=OPERATOR_HEADERS,
        json={
            "name": name,
            "provider": "sendgrid",
            "config": {
                "api_key": "SG.test-key",
                "from_email": "rewards@example.com",
                "from_name": "Rewards Team",
            },
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["config"]["api_key"] is None
    assert payload["config"]["api_key_configured"] is True
    return payload["provider_connection_id"]


def _seed_prediction_job(prediction_job_id: str, rows: list[dict]) -> None:
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        if repository.get_prediction_job(prediction_job_id) is None:
            repository.create_prediction_job(
                {
                    "id": prediction_job_id,
                    "import_job_id": "import_seed",
                    "status": "completed",
                    "spec": {"prediction_mode": "local"},
                    "progress": {"current": len(rows), "total": len(rows), "pct": 100.0, "details": {}},
                }
            )
    get_shared_bigquery_service().append_prediction_results(prediction_job_id, rows)


def _template_detail_payload(template_id: str, *, subject: str = "Come back for a reward") -> dict:
    return {
        "id": template_id,
        "name": "Winback Reward",
        "generation": "dynamic",
        "updated_at": "2026-03-09T10:00:00Z",
        "versions": [
            {
                "id": "ver_active",
                "name": "Active Version",
                "subject": subject,
                "updated_at": "2026-03-09T10:00:00Z",
                "active": 1,
                "editor": "code",
            }
        ],
    }


def test_sendgrid_template_list_endpoint_paginates_dynamic_templates(client: TestClient, monkeypatch):
    provider_connection_id = _create_sendgrid_provider_connection(client)
    call_log: list[dict] = []
    monkeypatch.setattr(
        "app.application.sendgrid_provider.requests.request",
        _build_sendgrid_stub(
            call_log,
            {
                ("GET", "/v3/templates"): [
                    _FakeSendGridResponse(
                        200,
                        {
                            "result": [
                                {
                                    "id": "tmpl_a",
                                    "name": "Winback A",
                                    "generation": "dynamic",
                                    "updated_at": "2026-03-08T10:00:00Z",
                                    "versions": [
                                        {
                                            "id": "ver_a",
                                            "name": "A",
                                            "subject": "Subject A",
                                            "updated_at": "2026-03-08T10:00:00Z",
                                            "active": 1,
                                            "editor": "code",
                                        }
                                    ],
                                }
                            ],
                            "_metadata": {
                                "next": "https://api.sendgrid.com/v3/templates?generations=dynamic&page_token=token-2"
                            },
                        },
                    ),
                    _FakeSendGridResponse(
                        200,
                        {
                            "result": [
                                {
                                    "id": "tmpl_b",
                                    "name": "Winback B",
                                    "generation": "dynamic",
                                    "updated_at": "2026-03-08T11:00:00Z",
                                    "versions": [
                                        {
                                            "id": "ver_b",
                                            "name": "B",
                                            "subject": "Subject B",
                                            "updated_at": "2026-03-08T11:00:00Z",
                                            "active": 1,
                                            "editor": "code",
                                        }
                                    ],
                                }
                            ],
                            "_metadata": {},
                        },
                    ),
                ]
            },
        ),
    )

    response = client.get(
        f"/api/v1/provider-connections/{provider_connection_id}/sendgrid/templates",
        headers=OPERATOR_HEADERS,
    )

    assert response.status_code == 200, response.text
    items = response.json()["items"]
    assert [item["id"] for item in items] == ["tmpl_a", "tmpl_b"]
    assert items[0]["active_version"]["subject"] == "Subject A"
    assert items[1]["active_version"]["id"] == "ver_b"
    assert "page_token" not in (call_log[0]["params"] or {})
    assert call_log[1]["params"]["page_token"] == "token-2"


def test_sendgrid_template_list_endpoint_surfaces_auth_failure(client: TestClient, monkeypatch):
    provider_connection_id = _create_sendgrid_provider_connection(client)
    monkeypatch.setattr(
        "app.application.sendgrid_provider.requests.request",
        _build_sendgrid_stub(
            [],
            {
                ("GET", "/v3/templates"): [
                    _FakeSendGridResponse(
                        401,
                        {"errors": [{"message": "Permission denied, wrong credentials"}]},
                    )
                ]
            },
        ),
    )

    response = client.get(
        f"/api/v1/provider-connections/{provider_connection_id}/sendgrid/templates",
        headers=OPERATOR_HEADERS,
    )

    assert response.status_code == 409
    assert "wrong credentials" in response.json()["detail"].lower()


def test_email_campaign_send_now_uses_deeplink_override_and_template_payload(client: TestClient, monkeypatch):
    provider_connection_id = _create_sendgrid_provider_connection(client)
    prediction_job_id = "pred_sendgrid_now"
    _seed_prediction_job(
        prediction_job_id,
        [
            {
                "prediction_job_id": prediction_job_id,
                "user_id": "u_1",
                "canonical_user_id": "u_1",
                "email": "u1@example.com",
                "first_name": "Ada",
                "reward_id": "rw_100",
                "reward_deeplink_url": "mygame://override/u_1",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "completed_at": "2026-03-09T10:00:00",
            },
            {
                "prediction_job_id": prediction_job_id,
                "user_id": "u_2",
                "canonical_user_id": "u_2",
                "email": "u2@example.com",
                "first_name": "Ben",
                "reward_id": "rw_200",
                "predicted_churn_risk": "medium",
                "churn_state": "active",
                "completed_at": "2026-03-09T10:00:00",
            },
        ],
    )
    call_log: list[dict] = []
    monkeypatch.setattr(
        "app.application.sendgrid_provider.requests.request",
        _build_sendgrid_stub(
            call_log,
            {
                ("GET", "/v3/templates/tmpl_winback"): [
                    _FakeSendGridResponse(200, _template_detail_payload("tmpl_winback"))
                ],
                ("POST", "/v3/mail/send"): [
                    _FakeSendGridResponse(202, {}, headers={"X-Message-Id": "msg-123"})
                ],
            },
        ),
    )

    create_response = client.post(
        "/api/v1/email-campaigns",
        headers=OPERATOR_HEADERS,
        json={
            "name": "spring_winback_reward",
            "provider_connection_id": provider_connection_id,
            "template_id": "tmpl_winback",
            "audience": {
                "prediction_job_id": prediction_job_id,
                "include_risks": ["high", "medium"],
                "include_churned": False,
            },
            "merge_fields": {
                "first_name": {"source": "field", "value": "first_name"},
                "reward_name": {"source": "literal", "value": "Welcome Back Pack"},
            },
            "deeplink_template": "mygame://reward?user_id={user_id}&reward_id={reward_id}&campaign={campaign_id}",
            "deeplink_override_field": "reward_deeplink_url",
        },
    )

    assert create_response.status_code == 201, create_response.text
    campaign = create_response.json()
    send_response = client.post(
        f"/api/v1/email-campaigns/{campaign['email_campaign_id']}/send-now",
        headers=OPERATOR_HEADERS,
    )

    assert send_response.status_code == 200, send_response.text
    payload = send_response.json()
    assert payload["status"] == "sent"
    assert payload["result_summary"]["sent_count"] == 2

    send_request = next(item for item in call_log if item["method"] == "POST")
    assert send_request["json"]["template_id"] == "tmpl_winback"
    personalizations_by_email = {
        item["to"][0]["email"]: item
        for item in send_request["json"]["personalizations"]
    }
    assert personalizations_by_email["u1@example.com"]["dynamic_template_data"]["first_name"] == "Ada"
    assert personalizations_by_email["u1@example.com"]["dynamic_template_data"]["reward_name"] == "Welcome Back Pack"
    assert personalizations_by_email["u1@example.com"]["dynamic_template_data"]["deeplink_url"] == "mygame://override/u_1"
    assert (
        personalizations_by_email["u2@example.com"]["dynamic_template_data"]["deeplink_url"]
        == f"mygame://reward?user_id=u_2&reward_id=rw_200&campaign={campaign['email_campaign_id']}"
    )


def test_email_campaign_scheduler_tick_runs_due_campaigns_and_marks_partial_errors(client: TestClient, monkeypatch):
    provider_connection_id = _create_sendgrid_provider_connection(client)
    prediction_job_id = "pred_sendgrid_due"
    _seed_prediction_job(
        prediction_job_id,
        [
            {
                "prediction_job_id": prediction_job_id,
                "user_id": "u_10",
                "canonical_user_id": "u_10",
                "email": "u10@example.com",
                "first_name": "Dana",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "completed_at": "2026-03-09T10:00:00",
            },
            {
                "prediction_job_id": prediction_job_id,
                "user_id": "u_11",
                "canonical_user_id": "u_11",
                "email": "",
                "first_name": "Evan",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "completed_at": "2026-03-09T10:00:00",
            },
        ],
    )
    call_log: list[dict] = []
    monkeypatch.setattr(
        "app.application.sendgrid_provider.requests.request",
        _build_sendgrid_stub(
            call_log,
            {
                ("GET", "/v3/templates/tmpl_scheduled"): [
                    _FakeSendGridResponse(200, _template_detail_payload("tmpl_scheduled"))
                ],
                ("POST", "/v3/mail/send"): [
                    _FakeSendGridResponse(202, {}, headers={"X-Message-Id": "msg-due-1"})
                ],
            },
        ),
    )

    create_response = client.post(
        "/api/v1/email-campaigns",
        headers=OPERATOR_HEADERS,
        json={
            "name": "scheduled_winback",
            "provider_connection_id": provider_connection_id,
            "template_id": "tmpl_scheduled",
            "audience": {
                "prediction_job_id": prediction_job_id,
                "include_risks": ["high"],
            },
            "merge_fields": {
                "first_name": {"source": "field", "value": "first_name"},
            },
            "schedule_at": "2026-03-10T09:00:00Z",
        },
    )

    assert create_response.status_code == 201, create_response.text
    campaign_id = create_response.json()["email_campaign_id"]

    tick_response = client.post(
        "/api/v1/health/scheduler/tick",
        headers=OPERATOR_HEADERS,
        json={"reference_time": "2026-03-10T10:00:00Z"},
    )

    assert tick_response.status_code == 200, tick_response.text
    job_runs = tick_response.json()["items"]
    due_job = next(item for item in job_runs if item["job_id"] == "due_email_campaign_runner")
    assert due_job["status"] == "completed"
    assert due_job["result_summary"]["campaign_runs"] == 1

    campaign_response = client.get(f"/api/v1/email-campaigns/{campaign_id}", headers=OPERATOR_HEADERS)
    assert campaign_response.status_code == 200
    campaign = campaign_response.json()
    assert campaign["status"] == "sent_with_errors"
    assert campaign["result_summary"]["sent_count"] == 1
    assert campaign["result_summary"]["skipped_missing_email"] == 1

    send_request = next(item for item in call_log if item["method"] == "POST")
    assert len(send_request["json"]["personalizations"]) == 1
    assert send_request["json"]["personalizations"][0]["to"][0]["email"] == "u10@example.com"


def test_email_campaign_edit_cancel_and_delete_rules(client: TestClient, monkeypatch):
    provider_connection_id = _create_sendgrid_provider_connection(client)
    prediction_job_id = "pred_sendgrid_rules"
    _seed_prediction_job(
        prediction_job_id,
        [
            {
                "prediction_job_id": prediction_job_id,
                "user_id": "u_20",
                "canonical_user_id": "u_20",
                "email": "u20@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "completed_at": "2026-03-09T10:00:00",
            }
        ],
    )
    monkeypatch.setattr(
        "app.application.sendgrid_provider.requests.request",
        _build_sendgrid_stub(
            [],
            {
                ("GET", "/v3/templates/tmpl_rules"): [
                    _FakeSendGridResponse(200, _template_detail_payload("tmpl_rules")),
                    _FakeSendGridResponse(200, _template_detail_payload("tmpl_rules")),
                    _FakeSendGridResponse(200, _template_detail_payload("tmpl_rules")),
                ],
            },
        ),
    )

    scheduled_response = client.post(
        "/api/v1/email-campaigns",
        headers=OPERATOR_HEADERS,
        json={
            "name": "rules_scheduled",
            "provider_connection_id": provider_connection_id,
            "template_id": "tmpl_rules",
            "audience": {"prediction_job_id": prediction_job_id, "include_risks": ["high"]},
            "schedule_at": "2026-03-12T10:00:00",
        },
    )
    assert scheduled_response.status_code == 201, scheduled_response.text
    scheduled_id = scheduled_response.json()["email_campaign_id"]

    update_response = client.patch(
        f"/api/v1/email-campaigns/{scheduled_id}",
        headers=OPERATOR_HEADERS,
        json={"name": "rules_scheduled_v2", "schedule_at": "2026-03-13T10:30:00"},
    )
    assert update_response.status_code == 200
    assert update_response.json()["name"] == "rules_scheduled_v2"
    assert update_response.json()["status"] == "scheduled"

    cancel_response = client.post(
        f"/api/v1/email-campaigns/{scheduled_id}/cancel",
        headers=OPERATOR_HEADERS,
    )
    assert cancel_response.status_code == 200
    assert cancel_response.json()["status"] == "cancelled"

    blocked_update = client.patch(
        f"/api/v1/email-campaigns/{scheduled_id}",
        headers=OPERATOR_HEADERS,
        json={"name": "should_fail"},
    )
    assert blocked_update.status_code == 409
    assert "draft or scheduled" in blocked_update.json()["detail"].lower()

    blocked_send = client.post(
        f"/api/v1/email-campaigns/{scheduled_id}/send-now",
        headers=OPERATOR_HEADERS,
    )
    assert blocked_send.status_code == 409

    draft_response = client.post(
        "/api/v1/email-campaigns",
        headers=OPERATOR_HEADERS,
        json={
            "name": "rules_draft",
            "provider_connection_id": provider_connection_id,
            "template_id": "tmpl_rules",
            "audience": {"prediction_job_id": prediction_job_id, "include_risks": ["high"]},
        },
    )
    assert draft_response.status_code == 201, draft_response.text
    draft_id = draft_response.json()["email_campaign_id"]

    delete_response = client.delete(
        f"/api/v1/email-campaigns/{draft_id}",
        headers=OPERATOR_HEADERS,
    )
    assert delete_response.status_code == 204

    missing_response = client.get(f"/api/v1/email-campaigns/{draft_id}", headers=OPERATOR_HEADERS)
    assert missing_response.status_code == 404
