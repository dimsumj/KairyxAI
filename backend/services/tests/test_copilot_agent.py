from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from app.application.cohorts import CohortService
from app.core import db as db_module
from app.core.db import session_scope
from app.main import create_app
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setenv("SCHEDULER_ENABLED", "false")
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    clear_shared_bigquery_service_cache()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def _headers(role: str = "admin", *, actor_id: str | None = None, tenant_id: str = "default", project_id: str = "default") -> dict[str, str]:
    return {
        "x-actor-role": role,
        "x-actor-id": actor_id or f"{role}_user",
        "x-tenant-id": tenant_id,
        "x-project-id": project_id,
    }


def _create_session(client: TestClient, headers: dict[str, str], *, title: str = "Agent Test Session") -> str:
    response = client.post(
        "/api/v1/copilot/agent/sessions",
        headers=headers,
        json={"title": title, "ui_context": {}},
    )
    assert response.status_code == 201
    return response.json()["session_state"]["session_id"]


def _seed_mock_warehouse():
    service = get_shared_bigquery_service()
    service.write_events_staging(
        [
            {
                "job_id": "job_1",
                "source": "adjust",
                "player_id": "u_1",
                "canonical_user_id": "u_1",
                "event_type": "promo_view",
                "event_time": "2026-03-08T10:00:00",
                "event_properties": {"country": "US", "platform": "ios", "campaign": "winback_a"},
                "user_properties": {"email": "u1@example.com"},
            },
            {
                "job_id": "job_1",
                "source": "adjust",
                "player_id": "u_2",
                "canonical_user_id": "u_2",
                "event_type": "item_purchased",
                "event_time": "2026-03-08T11:00:00",
                "event_properties": {"country": "US", "platform": "android", "campaign": "winback_b", "revenue_usd": 9.99},
                "user_properties": {"email": "u2@example.com"},
            },
            {
                "job_id": "job_1",
                "source": "adjust",
                "player_id": "u_3",
                "canonical_user_id": "u_3",
                "event_type": "session_start",
                "event_time": "2026-03-08T12:00:00",
                "event_properties": {"country": "CA", "platform": "ios", "campaign": "winback_a"},
                "user_properties": {"email": "u3@example.com"},
            },
        ],
        job_id="job_1",
    )
    service.run_events_curation(job_id="job_1")
    service.refresh_player_latest_state(job_id="job_1")
    service.append_prediction_results(
        "pred_job_1",
        [
            {
                "prediction_job_id": "pred_job_1",
                "user_id": "u_1",
                "canonical_user_id": "u_1",
                "email": "u1@example.com",
                "churn_state": "active",
                "predicted_churn_risk": "high",
                "prediction_source": "local",
                "suggested_action": "push_notification",
                "completed_at": "2026-03-09T10:00:00",
            },
            {
                "prediction_job_id": "pred_job_1",
                "user_id": "u_2",
                "canonical_user_id": "u_2",
                "email": "u2@example.com",
                "churn_state": "active",
                "predicted_churn_risk": "medium",
                "prediction_source": "local",
                "suggested_action": "email",
                "completed_at": "2026-03-09T10:00:00",
            },
            {
                "prediction_job_id": "pred_job_1",
                "user_id": "u_3",
                "canonical_user_id": "u_3",
                "email": "u3@example.com",
                "churn_state": "churned",
                "predicted_churn_risk": "already_churned",
                "prediction_source": "local",
                "suggested_action": "none",
                "completed_at": "2026-03-09T10:00:00",
            },
        ],
    )


def test_copilot_agent_connection_clarification_loop_and_safe_execution(client):
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers)

    first_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={"message": "Set up a connection.", "ui_context": {}},
    )
    assert first_turn.status_code == 200
    assert first_turn.json()["session_state"]["status"] == "awaiting_input"
    assert first_turn.json()["clarifications"][0]["key"] == "connection_scope"

    second_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "\n".join(
                [
                    "connection_scope: connector",
                    "connection_type: amplitude",
                    "name: agent_amplitude_connector",
                    "api_key: demo_api_key",
                    "secret_key: demo_secret_key",
                ]
            ),
            "ui_context": {},
        },
    )
    assert second_turn.status_code == 200
    payload = second_turn.json()
    assert payload["session_state"]["status"] == "active"
    assert {item["action_type"] for item in payload["completed_actions"]} >= {"upsert_connector", "check_connector_health"}
    connector_action = next(item for item in payload["completed_actions"] if item["action_type"] == "upsert_connector")
    assert connector_action["status"] == "completed"
    assert connector_action["parameters"]["config"]["api_key"] is None
    assert connector_action["parameters"]["config"]["api_key_configured"] is True
    assert connector_action["parameters"]["config"]["secret_key"] is None
    assert connector_action["parameters"]["config"]["secret_key_configured"] is True
    assert any(item["resource_type"] == "connector" for item in payload["artifacts"])

    turns = client.get(f"/api/v1/copilot/agent/sessions/{session_id}/turns", headers=headers)
    assert turns.status_code == 200
    assert len(turns.json()["items"]) == 2


def test_copilot_agent_creates_sql_cohort_and_disabled_experiment(client):
    _seed_mock_warehouse()
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers, title="Agent Cohort Session")

    cohort_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": (
                "Set up a cohort named high_risk_agent_cohort with cohort_type: sql and SQL: "
                "SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'"
            ),
            "ui_context": {},
        },
    )
    assert cohort_turn.status_code == 200
    cohort_payload = cohort_turn.json()
    assert [item["action_type"] for item in cohort_payload["completed_actions"]] == ["preview_sql", "save_query", "create_cohort_sql"]
    cohort_artifact = next(item for item in cohort_payload["artifacts"] if item["resource_type"] == "cohort")
    cohort_id = cohort_artifact["resource_id"]

    cohort_detail = client.get(f"/api/v1/cohorts/{cohort_id}", headers=headers)
    assert cohort_detail.status_code == 200
    assert cohort_detail.json()["status"] == "draft"
    assert cohort_detail.json()["member_count"] == 1

    experiment_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "\n".join(
                [
                    "Set up an A/B test",
                    "experiment id: agent_exp_1",
                    f"cohort_id: {cohort_id}",
                    "primary metric: return_rate",
                    "guardrails: engagement_rate, policy_block_rate",
                    "sample size: 15",
                    "runtime: 12",
                    "holdout: 10",
                    "b variant: 40",
                ]
            ),
            "ui_context": {},
        },
    )
    assert experiment_turn.status_code == 200
    experiment_payload = experiment_turn.json()
    experiment_action = next(item for item in experiment_payload["completed_actions"] if item["action_type"] == "save_experiment_config")
    experiment = experiment_action["result"]["experiment"]
    assert experiment["experiment_id"] == "agent_exp_1"
    assert experiment["enabled"] is False
    assert experiment["cohort_id"] == cohort_id
    assert experiment["holdout_pct"] == 0.1
    assert experiment["b_variant_pct"] == 0.4


def test_copilot_agent_confirmation_gate_for_risky_action(client):
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers, title="Agent Confirmation Session")

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        cohort = CohortService(repository).create_cohort(
            name="agent_confirm_target",
            cohort_type="list",
            definition={"members": [{"canonical_user_id": "u_1", "email": "u1@example.com"}]},
            owner="system",
            activate=False,
        )
        session.commit()
    cohort_id = cohort["cohort_id"]

    pending = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={"message": f"Activate cohort {cohort_id}", "ui_context": {}},
    )
    assert pending.status_code == 200
    pending_payload = pending.json()
    assert pending_payload["session_state"]["status"] == "awaiting_confirmation"
    assert pending_payload["pending_confirmations"][0]["action_type"] == "activate_cohort"
    action_id = pending_payload["pending_confirmations"][0]["action_id"]

    confirmed = client.post(
        f"/api/v1/copilot/agent/actions/{action_id}/confirm",
        headers=headers,
        json={"note": "Approved for activation."},
    )
    assert confirmed.status_code == 200
    confirmed_payload = confirmed.json()
    assert confirmed_payload["completed_actions"][0]["action_type"] == "activate_cohort"
    assert confirmed_payload["completed_actions"][0]["status"] == "completed"

    cohort_detail = client.get(f"/api/v1/cohorts/{cohort_id}", headers=headers)
    assert cohort_detail.status_code == 200
    assert cohort_detail.json()["status"] == "active"


def test_copilot_agent_blocks_write_for_analyst_and_scopes_sessions_by_project(client):
    analyst_headers = _headers("analyst", actor_id="agent_analyst", project_id="default")
    session_id = _create_session(client, analyst_headers, title="Analyst Agent Session")

    summary = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=analyst_headers,
        json={"message": "Summarize the dashboard.", "ui_context": {}},
    )
    assert summary.status_code == 200
    assert summary.json()["completed_actions"][0]["action_type"] == "summarize_dashboard"
    assert summary.json()["completed_actions"][0]["status"] == "completed"

    blocked = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=analyst_headers,
        json={
            "message": "\n".join(
                [
                    "Set up a connection",
                    "connection_scope: connector",
                    "connection_type: amplitude",
                    "name: analyst_blocked_connector",
                    "api_key: demo_api_key",
                    "secret_key: demo_secret_key",
                ]
            ),
            "ui_context": {},
        },
    )
    assert blocked.status_code == 200
    blocked_action = blocked.json()["completed_actions"][0]
    assert blocked_action["action_type"] == "upsert_connector"
    assert blocked_action["status"] == "blocked"
    assert blocked_action["result"]["status_code"] == 403

    alpha_headers = _headers("operator", actor_id="project_operator", project_id="alpha")
    alpha_session_id = _create_session(client, alpha_headers, title="Alpha Project Session")
    alpha_session = client.get(f"/api/v1/copilot/agent/sessions/{alpha_session_id}", headers=alpha_headers)
    assert alpha_session.status_code == 200

    beta_headers = _headers("operator", actor_id="project_operator", project_id="beta")
    missing = client.get(f"/api/v1/copilot/agent/sessions/{alpha_session_id}", headers=beta_headers)
    assert missing.status_code == 404
