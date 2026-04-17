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


def _create_sendgrid_provider_connection(client: TestClient, headers: dict[str, str], *, name: str = "Agent SendGrid") -> str:
    response = client.post(
        "/api/v1/provider-connections",
        headers=headers,
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


def _seed_completed_import_job(job_id: str, *, source_name: str = "Amplitude 1") -> None:
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        if repository.get_import_job(job_id) is None:
            repository.create_import_job(
                {
                    "id": job_id,
                    "source_name": source_name,
                    "status": "completed",
                    "spec": {
                        "job_id": job_id,
                        "source_name": source_name,
                        "display_name": source_name,
                    },
                    "progress": {"current": 1, "total": 1, "pct": 100.0, "details": {}},
                }
            )


def _seed_completed_prediction_job(
    prediction_job_id: str,
    *,
    import_job_id: str = "imp_agentsource1",
    source_name: str = "Amplitude 1",
    prediction_mode: str = "local",
    rows: list[dict] | None = None,
) -> None:
    _seed_completed_import_job(import_job_id, source_name=source_name)
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        if repository.get_prediction_job(prediction_job_id) is None:
            repository.create_prediction_job(
                {
                    "id": prediction_job_id,
                    "import_job_id": import_job_id,
                    "status": "completed",
                    "spec": {
                        "import_job_id": import_job_id,
                        "audience_scope": "source",
                        "source_name": source_name,
                        "prediction_mode": prediction_mode,
                    },
                    "progress": {
                        "current": len(rows or []),
                        "total": len(rows or []),
                        "pct": 100.0,
                        "details": {
                            "prediction_mode": prediction_mode,
                            "audience_scope": "source",
                            "source_name": source_name,
                            "history_scope": "tenant_merged",
                            "stale": False,
                            "stale_reason": "",
                        },
                    },
                }
            )
    if rows:
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


def test_copilot_agent_create_session_returns_empty_ready_state(client):
    headers = _headers("analyst", actor_id="agent_session_reader")
    response = client.post(
        "/api/v1/copilot/agent/sessions",
        headers=headers,
        json={"title": "Fresh Agent Session", "ui_context": {"active_module_id": "data-core"}},
    )
    assert response.status_code == 201
    payload = response.json()
    assert payload["latest_turn"] is None
    assert payload["pending_confirmations"] == []
    assert payload["session_state"]["title"] == "Fresh Agent Session"
    assert payload["session_state"]["ui_context"]["active_module_id"] == "data-core"


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


def test_copilot_agent_support_answers_with_page_context_and_samples(client):
    headers = _headers("analyst", actor_id="agent_analyst")
    session_id = _create_session(client, headers, title="Agent Support Session")

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "How do I create an Amplitude connector here? Give me a sample payload.",
            "ui_context": {"active_module_id": "data-core", "active_page_id": "connectors"},
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["session_state"]["status"] == "active"
    assert payload["session_state"]["current_intent"] == "help_support"
    assert payload["clarifications"] == []
    assert payload["completed_actions"] == []
    assert "Data Core -> Connectors" in payload["assistant_message"]
    assert "demo_api_key" in payload["assistant_message"]
    assert "```json" in payload["assistant_message"]


def test_copilot_agent_unsupported_requests_fall_back_to_grounded_help(client):
    headers = _headers("analyst", actor_id="agent_analyst")
    session_id = _create_session(client, headers, title="Agent Unsupported Fallback Session")

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "What should I do next here?",
            "ui_context": {"active_module_id": "audience-engine", "active_page_id": "audience-engine"},
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["session_state"]["status"] == "active"
    assert payload["session_state"]["current_intent"] == "help_support"
    assert payload["clarifications"] == []
    assert payload["completed_actions"] == []
    assert "Audience Engine" in payload["assistant_message"]
    assert "Set up a cohort" in payload["assistant_message"]


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


def test_copilot_agent_model_profiles_support_default_selection_and_provider_fallback(client, monkeypatch):
    headers = _headers("operator", actor_id="agent_operator")

    gemini_profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "Gemini Drafts",
            "provider": "gemini",
            "model_name": "gemini-2.5-flash",
            "config": {"api_key": "gemini-test-key"},
            "is_default": True,
        },
    )
    assert gemini_profile.status_code == 201, gemini_profile.text
    gemini_payload = gemini_profile.json()
    assert gemini_payload["config"]["api_key"] is None
    assert gemini_payload["config"]["api_key_configured"] is True

    anthropic_profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "Anthropic Drafts",
            "provider": "anthropic",
            "model_name": "claude-3-7-sonnet-latest",
            "config": {
                "api_key": "anthropic-test-key",
                "base_url": "https://api.anthropic.com",
            },
        },
    )
    assert anthropic_profile.status_code == 201, anthropic_profile.text
    anthropic_payload = anthropic_profile.json()

    listed = client.get("/api/v1/copilot/agent/model-profiles", headers=headers)
    assert listed.status_code == 200
    items = listed.json()["items"]
    assert items[0]["model_profile_id"] == gemini_payload["model_profile_id"]
    assert items[0]["is_default"] is True

    default_session = client.post(
        "/api/v1/copilot/agent/sessions",
        headers=headers,
        json={"title": "Default Model Session", "ui_context": {}},
    )
    assert default_session.status_code == 201
    assert default_session.json()["session_state"]["model_profile_id"] == gemini_payload["model_profile_id"]
    assert default_session.json()["session_state"]["effective_provider"] == "gemini"

    selected_session = client.post(
        "/api/v1/copilot/agent/sessions",
        headers=headers,
        json={
            "title": "Anthropic Session",
            "ui_context": {"active_module_id": "audience-engine", "active_page_id": "audience-engine"},
            "model_profile_id": anthropic_payload["model_profile_id"],
        },
    )
    assert selected_session.status_code == 201
    session_id = selected_session.json()["session_state"]["session_id"]
    assert selected_session.json()["session_state"]["effective_provider"] == "anthropic"
    assert selected_session.json()["session_state"]["effective_model_name"] == "claude-3-7-sonnet-latest"

    def _raise_requests(*args, **kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr("app.application.text_model_runtime.requests.post", _raise_requests)

    fallback = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "How do I create a cohort from churn predictions?",
            "ui_context": {"active_module_id": "audience-engine", "active_page_id": "audience-engine"},
        },
    )
    assert fallback.status_code == 200
    payload = fallback.json()
    assert payload["session_state"]["effective_provider"] == "anthropic"
    assert payload["assistant_message"]
    assert payload["session_state"]["status"] == "active"


def test_copilot_agent_model_profiles_allow_local_openai_without_api_key(client, monkeypatch):
    headers = _headers("operator", actor_id="agent_operator")

    profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "LM Studio Local",
            "provider": "openai",
            "model_name": "local-llama-3.1",
            "config": {
                "base_url": "http://127.0.0.1:1234/v1/",
                "runtime_preset": "lmstudio",
            },
            "is_default": True,
        },
    )
    assert profile.status_code == 201, profile.text
    payload = profile.json()
    assert payload["config"]["api_key"] is None
    assert payload["config"]["api_key_configured"] is False
    assert payload["config"]["base_url"] == "http://127.0.0.1:1234/v1"
    assert payload["config"]["runtime_preset"] == "lmstudio"

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": "{\"assistant_message\":\"Use the cohort builder or ask the agent to draft it.\"}",
                        }
                    }
                ]
            }

    captured = {}

    def _fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers or {}
        captured["json"] = json or {}
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr("app.application.text_model_runtime.requests.post", _fake_post)

    session = client.post(
        "/api/v1/copilot/agent/sessions",
        headers=headers,
        json={"title": "Local OpenAI Session", "ui_context": {}},
    )
    assert session.status_code == 201
    session_id = session.json()["session_state"]["session_id"]
    assert session.json()["session_state"]["effective_provider"] == "openai"
    assert session.json()["session_state"]["effective_model_name"] == "local-llama-3.1"

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={"message": "How do I create a cohort?", "ui_context": {}},
    )
    assert response.status_code == 200
    assert response.json()["assistant_message"]
    assert captured["url"] == "http://127.0.0.1:1234/v1/chat/completions"
    assert "Authorization" not in captured["headers"]
    assert captured["json"]["model"] == "local-llama-3.1"


def test_copilot_agent_model_profiles_reject_openai_without_api_key_or_base_url(client):
    headers = _headers("operator", actor_id="agent_operator")

    profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "Broken OpenAI Profile",
            "provider": "openai",
            "model_name": "gpt-4.1-mini",
            "config": {},
        },
    )
    assert profile.status_code == 409
    assert profile.json()["detail"] == "OpenAI agent model profiles require api_key or base_url."


def test_copilot_agent_model_profiles_reject_private_openai_base_url_in_prod(client, monkeypatch):
    headers = _headers("operator", actor_id="agent_operator")
    monkeypatch.setenv("APP_ENV", "prod")

    profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "Hosted LM Studio",
            "provider": "openai",
            "model_name": "local-llama-3.1",
            "config": {
                "base_url": "http://127.0.0.1:1234/v1",
                "runtime_preset": "lmstudio",
            },
        },
    )
    assert profile.status_code == 409
    assert (
        profile.json()["detail"]
        == "Private-network or localhost OpenAI-compatible runtime base_url values are only allowed outside hosted production deployments."
    )


def test_copilot_agent_draft_sql_blocks_when_preview_lacks_canonical_user_id(client, monkeypatch):
    prediction_job_id = "pred_sqlblock1"
    _seed_completed_prediction_job(
        prediction_job_id,
        rows=[
            {
                "prediction_job_id": prediction_job_id,
                "user_id": "u_1",
                "canonical_user_id": "u_1",
                "email": "u1@example.com",
                "churn_state": "active",
                "predicted_churn_risk": "high",
                "prediction_source": "local",
                "suggested_action": "email",
                "completed_at": "2026-03-09T10:00:00",
            }
        ],
    )
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers, title="SQL Draft Guard Session")

    monkeypatch.setattr(
        "app.application.copilot_agent.ConfiguredCopilotAgentModel.draft_sql",
        lambda self, prompt, *, session_state, ui_context, hint: {
            "sql": f"SELECT email FROM prediction_results WHERE prediction_job_id = '{prediction_job_id}'",
            "query_name": "invalid_query",
            "cohort_name": "invalid_cohort",
        },
    )

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": f"Draft SQL for the high-risk audience using prediction_job_id: {prediction_job_id}",
            "ui_context": {},
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["completed_actions"][0]["action_type"] == "draft_sql_from_prompt"
    assert payload["completed_actions"][0]["status"] == "blocked"
    assert payload["completed_actions"][0]["result"]["status_code"] == 409
    assert "canonical_user_id" in payload["completed_actions"][0]["summary"]


def test_copilot_agent_operator_flow_resumes_after_prediction_completion(client, monkeypatch):
    headers = _headers("operator", actor_id="agent_operator")
    provider_connection_id = _create_sendgrid_provider_connection(client, headers, name="Agent Flow SendGrid")
    _seed_completed_import_job("impagentsource1", source_name="Amplitude 1")

    monkeypatch.setattr(
        "app.application.predictions.PredictionService.start_job_async",
        lambda self, job_id: self.get_job(job_id),
    )
    monkeypatch.setattr(
        "app.application.sendgrid_provider.SendGridProviderService.list_dynamic_templates",
        lambda self, provider_connection_id: [{"id": "tmpl_winback", "name": "Winback Template"}],
    )
    monkeypatch.setattr(
        "app.application.sendgrid_provider.SendGridProviderService.get_template_summary",
        lambda self, provider_connection_id, template_id: _template_detail_payload(template_id),
    )

    session_id = _create_session(client, headers, title="Operator Flow Session")
    first_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "\n".join(
                [
                    "Run churn prediction for high-risk players, create a cohort, use SendGrid template tmpl_winback, and set up a draft workflow.",
                    "source_name: Amplitude 1",
                    f"provider_connection_id: {provider_connection_id}",
                    "template_id: tmpl_winback",
                    "campaign_name: agent_winback_campaign",
                    "workflow_name: agent_winback_workflow",
                    "cohort_name: agent_high_risk_cohort",
                    "saved_query_name: agent_high_risk_query",
                    "local model",
                ]
            ),
            "ui_context": {},
        },
    )
    assert first_turn.status_code == 200
    first_payload = first_turn.json()
    assert first_payload["session_state"]["status"] == "waiting_for_prediction"
    assert first_payload["session_state"]["async_status"] == "waiting_for_prediction"
    assert first_payload["completed_actions"][0]["action_type"] == "setup_operator_flow"
    assert first_payload["completed_actions"][0]["status"] == "running"
    prediction_job_id = first_payload["completed_actions"][0]["result"]["prediction_job"]["id"]
    assert prediction_job_id

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.update_prediction_job(
            prediction_job_id,
            {
                "status": "completed",
                "progress": {
                    "current": 1,
                    "total": 1,
                    "pct": 100.0,
                    "details": {
                        "prediction_mode": "local",
                        "audience_scope": "source",
                        "source_name": "Amplitude 1",
                        "history_scope": "tenant_merged",
                        "stale": False,
                        "stale_reason": "",
                    },
                },
            },
        )
    get_shared_bigquery_service().append_prediction_results(
        prediction_job_id,
        [
            {
                "prediction_job_id": prediction_job_id,
                "user_id": "u_1",
                "canonical_user_id": "u_1",
                "email": "u1@example.com",
                "churn_state": "active",
                "predicted_churn_risk": "high",
                "prediction_source": "local",
                "suggested_action": "email",
                "completed_at": "2026-03-09T10:00:00",
            },
            {
                "prediction_job_id": prediction_job_id,
                "user_id": "u_2",
                "canonical_user_id": "u_2",
                "email": "u2@example.com",
                "churn_state": "active",
                "predicted_churn_risk": "medium",
                "prediction_source": "local",
                "suggested_action": "wait",
                "completed_at": "2026-03-09T10:00:00",
            },
        ],
    )

    session_status = client.get(f"/api/v1/copilot/agent/sessions/{session_id}", headers=headers)
    assert session_status.status_code == 200
    assert session_status.json()["session_state"]["status"] == "ready_to_resume"
    assert session_status.json()["session_state"]["async_status"] == "ready_to_resume"
    assert any(item.get("resume_ready") for item in session_status.json()["session_state"]["latest_artifacts"])

    resumed = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={"message": "Continue", "ui_context": {}},
    )
    assert resumed.status_code == 200
    resumed_payload = resumed.json()
    assert resumed_payload["session_state"]["status"] == "active"
    assert resumed_payload["completed_actions"][0]["action_type"] == "setup_operator_flow"
    assert resumed_payload["completed_actions"][0]["status"] == "completed"

    artifact_types = {item["resource_type"] for item in resumed_payload["artifacts"]}
    assert {"prediction_job", "saved_query", "cohort", "email_campaign", "workflow"} <= artifact_types

    cohort_artifact = next(item for item in resumed_payload["artifacts"] if item["resource_type"] == "cohort")
    campaign_artifact = next(item for item in resumed_payload["artifacts"] if item["resource_type"] == "email_campaign")
    workflow_artifact = next(item for item in resumed_payload["artifacts"] if item["resource_type"] == "workflow")

    cohort_detail = client.get(f"/api/v1/cohorts/{cohort_artifact['resource_id']}", headers=headers)
    assert cohort_detail.status_code == 200
    assert cohort_detail.json()["status"] == "draft"

    campaign_detail = client.get(f"/api/v1/email-campaigns/{campaign_artifact['resource_id']}", headers=headers)
    assert campaign_detail.status_code == 200
    assert campaign_detail.json()["status"] == "draft"
    assert campaign_detail.json()["template_id"] == "tmpl_winback"

    workflow_detail = client.get(f"/api/v1/workflows/{workflow_artifact['resource_id']}", headers=headers)
    assert workflow_detail.status_code == 200
    assert workflow_detail.json()["status"] == "draft"
    assert workflow_detail.json()["definition"]["cohort_id"] == cohort_artifact["resource_id"]
