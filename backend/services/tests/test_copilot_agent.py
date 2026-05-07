from __future__ import annotations

from contextlib import contextmanager

from fastapi.testclient import TestClient
import pytest

from app.application.cohorts import CohortService
from app.application.copilot_agent import ACTION_RESOURCE_TYPE, CONFIRMATION_RESOURCE_TYPE, deterministic_agent_parse
from app.core import db as db_module
from app.core.db import session_scope
from app.main import create_app
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service
from secret_manager_service import SecretManagerService


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


@contextmanager
def _client_with_env(monkeypatch, tmp_path, **env):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setenv("SCHEDULER_ENABLED", "false")
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    clear_shared_bigquery_service_cache()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    clear_shared_bigquery_service_cache()


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


def _create_push_provider_connection(client: TestClient, headers: dict[str, str], *, name: str = "Agent Push Provider") -> str:
    response = client.post(
        "/api/v1/provider-connections",
        headers=headers,
        json={
            "name": name,
            "provider": "wynn_push_notifier",
            "config": {
                "api_token": "push-test-token",
                "base_url": "https://push.example.test",
            },
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["config"]["api_token"] is None
    assert payload["config"]["api_token_configured"] is True
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


def test_copilot_agent_secure_inputs_create_bigquery_and_push_provider_without_chat_secrets(client):
    headers = _headers("operator", actor_id="agent_operator")
    bigquery_session_id = _create_session(client, headers, title="Secure BigQuery Session")

    first_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{bigquery_session_id}/messages",
        headers=headers,
        json={
            "message": "Set up a BigQuery connector named agent_bigquery_connector with project_id: analytics-prod",
            "ui_context": {},
        },
    )
    assert first_turn.status_code == 200
    first_payload = first_turn.json()
    assert first_payload["session_state"]["status"] == "awaiting_input"
    clarification_by_key = {item["key"]: item for item in first_payload["clarifications"]}
    assert clarification_by_key["dataset_id"]["input_type"] == "text"
    assert clarification_by_key["service_account_json"]["input_type"] == "secure_multiline"
    assert clarification_by_key["service_account_json"]["metadata"]["secure_input"] is True

    premature_secure_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{bigquery_session_id}/secure-inputs",
        headers=headers,
        json={
            "values": {
                "dataset_id": "game_events",
                "service_account_json": "{}",
            },
            "ui_context": {},
        },
    )
    assert premature_secure_turn.status_code == 409
    assert "non-sensitive clarification fields first" in premature_secure_turn.json()["detail"]

    dataset_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{bigquery_session_id}/messages",
        headers=headers,
        json={
            "message": "dataset_id: game_events",
            "ui_context": {},
        },
    )
    assert dataset_turn.status_code == 200
    assert dataset_turn.json()["session_state"]["status"] == "awaiting_input"
    assert [item["key"] for item in dataset_turn.json()["clarifications"]] == ["service_account_json"]

    service_account_json = (
        '{"type":"service_account","project_id":"analytics-prod",'
        '"client_email":"svc@analytics-prod.iam.gserviceaccount.com",'
        '"private_key":"-----BEGIN PRIVATE KEY-----\\nagent-private-key\\n-----END PRIVATE KEY-----\\n",'
        '"token_uri":"https://oauth2.googleapis.com/token"}'
    )
    invalid_secure_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{bigquery_session_id}/secure-inputs",
        headers=headers,
        json={
            "values": {
                "service_account_json": service_account_json,
                "name": "audit_bypass_attempt",
            },
            "ui_context": {},
        },
    )
    assert invalid_secure_turn.status_code == 400
    assert "pending secure fields" in invalid_secure_turn.json()["detail"]

    secure_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{bigquery_session_id}/secure-inputs",
        headers=headers,
        json={
            "values": {
                "service_account_json": service_account_json,
            },
            "ui_context": {},
        },
    )
    assert secure_turn.status_code == 200, secure_turn.text
    payload = secure_turn.json()
    assert payload["session_state"]["status"] == "active"
    connector_action = next(item for item in payload["completed_actions"] if item["action_type"] == "upsert_connector")
    assert connector_action["parameters"]["config"]["service_account_json"] is None
    assert connector_action["parameters"]["config"]["service_account_json_configured"] is True
    assert connector_action["parameters"]["config"]["dataset_id"] == "game_events"

    turns = client.get(f"/api/v1/copilot/agent/sessions/{bigquery_session_id}/turns", headers=headers)
    assert turns.status_code == 200
    transcript = "\n".join(item["user_message"] for item in turns.json()["items"])
    assert "agent-private-key" not in transcript
    assert "Secure setup details submitted for: service_account_json." in transcript

    rejected_after_completion = client.post(
        f"/api/v1/copilot/agent/sessions/{bigquery_session_id}/secure-inputs",
        headers=headers,
        json={"values": {"service_account_json": service_account_json}, "ui_context": {}},
    )
    assert rejected_after_completion.status_code == 409

    push_provider_session_id = _create_session(client, headers, title="Secure Push Provider Session")
    push_provider_prompt = client.post(
        f"/api/v1/copilot/agent/sessions/{push_provider_session_id}/messages",
        headers=headers,
        json={
            "message": "Set up a provider connection for push provider named agent_push_provider with base_url: https://push.example.com",
            "ui_context": {},
        },
    )
    assert push_provider_prompt.status_code == 200
    assert push_provider_prompt.json()["session_state"]["status"] == "awaiting_input"

    push_provider_secure = client.post(
        f"/api/v1/copilot/agent/sessions/{push_provider_session_id}/secure-inputs",
        headers=headers,
        json={
            "values": {
                "api_token": "push-provider-token",
            },
            "ui_context": {},
        },
    )
    assert push_provider_secure.status_code == 200, push_provider_secure.text
    provider_action = next(item for item in push_provider_secure.json()["completed_actions"] if item["action_type"] == "upsert_provider_connection")
    assert provider_action["parameters"]["provider"] == "wynn_push_notifier"
    assert provider_action["parameters"]["config"]["api_token"] is None
    assert provider_action["parameters"]["config"]["api_token_configured"] is True


@pytest.mark.parametrize(
    ("message", "expected_intent"),
    [
        ("Connect a BigQuery data source and open the secure credential dialog.", "setup_connection"),
        ("Fix the selected import mapping, preview it, and prepare reprocessing for module review.", "remap_import"),
        ("Summarize data health, blocked imports, and mapping diagnostics.", "summarize_dashboard"),
        ("Create a cohort for high-risk winback users from the latest completed prediction runs.", "setup_cohort"),
        ("Draft SQL for high-risk users with canonical_user_id and email, then preview it.", "draft_sql_from_prompt"),
        ("Draft the audience builder state for high-risk churn rescue users and show the preview.", "draft_audience_builder"),
        ("Build a SendGrid campaign for the selected high-risk cohort and leave it in draft.", "setup_email_campaign"),
        ("Create a draft workflow for the selected cohort and leave it in draft.", "setup_workflow"),
        ("Prepare a one-time push notification for module review without sending it from chat.", "send_push_dispatch"),
        ("Configure an A/B experiment for the selected cohort with return_rate as the primary metric.", "setup_experiment"),
        ("Summarize experiment health, guardrails, measurement gaps, and rollout diagnostics.", "summarize_dashboard"),
        ("Ingest experiment outcomes for the selected experiment from an outcomes JSON payload.", "ingest_experiment_outcomes"),
        ("Summarize the dashboard, current risks, blocked imports, active experiments, and recommended next steps.", "summarize_dashboard"),
        ("Inspect diagnostics across data, audiences, workflows, experiments, and recent Copilot reports.", "summarize_dashboard"),
        ("Build the next prediction-to-campaign workflow as drafts and leave live actions for module review.", "setup_operator_flow"),
    ],
)
def test_copilot_agent_primary_starter_prompts_route_to_expected_intents(client, message, expected_intent):
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers, title=f"Starter {expected_intent}")

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": message,
            "ui_context": {
                "active_module_id": "insight-copilot",
                "selected_cohort_id": "cohort_selected",
                "current_experiment_id": "exp_selected",
            },
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["session_state"]["current_intent"] == expected_intent


def test_copilot_agent_primary_starter_prompt_slots_are_safe_for_campaign_and_workflow():
    campaign_prompt = "Build a SendGrid campaign for the selected high-risk cohort and leave it in draft."
    campaign_parse = deterministic_agent_parse(
        campaign_prompt,
        ui_context={"selected_cohort_id": "cohort_selected"},
    )
    assert campaign_parse["intent"] == "setup_email_campaign"
    assert "template_id" not in campaign_parse["slots"]
    assert campaign_parse["slots"].get("messaging_provider") == "sendgrid"

    workflow_prompt = "Build the next prediction-to-campaign workflow as drafts and leave live actions for module review."
    workflow_parse = deterministic_agent_parse(workflow_prompt, ui_context={})
    assert workflow_parse["intent"] == "setup_operator_flow"
    assert workflow_parse["slots"]["wants_email_campaign"] is True
    assert workflow_parse["slots"]["wants_workflow"] is True


@pytest.mark.parametrize(
    "message",
    [
        "What is the prediction-to-campaign workflow?",
        "Explain the prediction-to-campaign workflow.",
    ],
)
def test_copilot_agent_prediction_to_campaign_questions_stay_read_only(client, message):
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers, title="Read Only Flow Question")

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": message,
            "ui_context": {
                "selected_cohort_id": "cohort_selected",
                "current_prediction_job_id": "pred_selected",
            },
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["session_state"]["current_intent"] == "help_support"
    assert payload["completed_actions"] == []
    assert payload["pending_confirmations"] == []


def test_copilot_agent_prompt_actions_cover_manual_operator_flows_as_guidance_handoffs(client):
    headers = _headers("operator", actor_id="agent_operator")
    push_provider_connection_id = _create_push_provider_connection(client, headers, name="Agent Manual Push")
    session_id = _create_session(client, headers, title="Manual Flow Agent Session")

    def assert_guidance_handoff(payload, expected_action):
        assert payload["session_state"]["status"] == "active"
        assert payload["pending_confirmations"] == []
        action = next(item for item in payload["completed_actions"] if item["action_type"] == expected_action)
        assert action["status"] == "prepared"
        assert action["requires_confirmation"] is False
        assert action["result"]["manual_handoff"] is True
        assert action["result"]["next_steps"]
        assert action["artifacts"][0]["resource_type"] == "action_handoff"
        return action

    mapping_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": 'Fix mapping for imp_agentmanual1 with ```json\n{"mapping":{"user_id":"player_id","event_name":"event_type"}}\n```',
            "ui_context": {},
        },
    )
    assert mapping_turn.status_code == 200
    mapping_payload = mapping_turn.json()
    assert_guidance_handoff(mapping_payload, "remap_import")

    push_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "Send push notification user_id: u_1 title: Winback body: Return today for a bonus",
            "ui_context": {},
        },
    )
    assert push_turn.status_code == 200
    push_payload = push_turn.json()
    assert_guidance_handoff(push_payload, "send_push_dispatch")

    draft_push_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "Schedule a single push through the connected push channel to all users in half an hour from now. Help me design the copy to call players back to the game.",
            "ui_context": {},
        },
    )
    assert draft_push_turn.status_code == 200
    draft_push_payload = draft_push_turn.json()
    draft_push_action = assert_guidance_handoff(draft_push_payload, "send_push_dispatch")
    assert draft_push_payload["clarifications"] == []
    assert draft_push_action["parameters"]["title"]
    assert "back" in draft_push_action["parameters"]["body"].lower() or "game" in draft_push_action["parameters"]["body"].lower()
    assert draft_push_action["parameters"]["schedule_at"]
    assert draft_push_action["parameters"]["schedule_at"].endswith("Z")
    assert draft_push_action["parameters"]["provider_connection_id"] == push_provider_connection_id
    assert draft_push_action["parameters"]["copy_draft"]["channel"] == "push"

    for message, expected_action in [
        ("Schedule email campaign ec_agentmanual1 schedule_at: 2026-05-05T10:00:00Z", "schedule_email_campaign"),
        ("Send email campaign ec_agentmanual1", "send_email_campaign"),
        ("Cancel email campaign ec_agentmanual1", "cancel_email_campaign"),
        ("Delete email campaign ec_agentmanual1", "delete_email_campaign"),
        ("Publish workflow wf_agentmanual1", "publish_workflow"),
        ("Pause workflow wf_agentmanual1", "pause_workflow"),
        ("Resume workflow wf_agentmanual1", "resume_workflow"),
        ("Test run workflow wf_agentmanual1", "test_run_workflow"),
        ("Archive workflow wf_agentmanual1", "archive_workflow"),
        ("Delete workflow wf_agentmanual1", "delete_workflow"),
    ]:
        response = client.post(
            f"/api/v1/copilot/agent/sessions/{session_id}/messages",
            headers=headers,
            json={"message": message, "ui_context": {}},
        )
        assert response.status_code == 200
        payload = response.json()
        action = assert_guidance_handoff(payload, expected_action)
        if expected_action in {"schedule_email_campaign", "send_email_campaign"}:
            assert "copy_draft" not in action["parameters"]
            assert "subject" not in action["parameters"]
            assert "body" not in action["parameters"]

    outcomes_turn = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": (
                "Ingest experiment outcomes for experiment id: agent_outcome_exp "
                '```json\n{"outcomes":[{"workflow_id":"wf_agentmanual1","cohort_id":"cohort_agentmanual1",'
                '"experiment_id":"agent_outcome_exp","user_id":"u_1","occurred_at":"2026-05-04T12:00:00Z",'
                '"outcome_name":"returned"}]}\n```'
            ),
            "ui_context": {},
        },
    )
    assert outcomes_turn.status_code == 200
    outcomes_payload = outcomes_turn.json()
    assert_guidance_handoff(outcomes_payload, "ingest_experiment_outcomes")


def test_copilot_agent_grounds_push_copy_handoff_with_knowledge_citations(client):
    headers = _headers("operator", actor_id="agent_grounded_push")
    push_provider_connection_id = _create_push_provider_connection(client, headers, name="Agent Grounded Push")
    document = client.post(
        "/api/v1/knowledge/documents",
        headers=headers,
        json={
            "title": "VIP Winback Playbook",
            "source_type": "playbook",
            "source_name": "Lifecycle Marketing",
            "tags": ["vip", "winback", "push"],
            "content": (
                "VIP players respond best to status-benefit push copy. "
                "Use concise reminders about returning to the game, premium access, "
                "and weekend evening windows without inventing rewards."
            ),
        },
    )
    assert document.status_code == 201, document.text
    feedback = client.post(
        "/api/v1/experiments/ai-feedback",
        headers=headers,
        json={
            "feedback_type": "operator_approval",
            "target_type": "push_copy_draft",
            "target_id": "vip_saved_checkpoint_copy",
            "weight": 0.7,
            "comments": "Favor concise saved checkpoint language for VIP winback pushes.",
        },
    )
    assert feedback.status_code == 201, feedback.text
    session_id = _create_session(client, headers, title="Grounded Push Copy Session")

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": (
                "Prepare a single push through the connected push channel for VIP players. "
                "Draft the title and body from our VIP winback playbook to call them back to the game."
            ),
            "ui_context": {},
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    action = next(item for item in payload["completed_actions"] if item["action_type"] == "send_push_dispatch")
    evidence_artifact = next(item for item in payload["artifacts"] if item["resource_type"] == "knowledge_retrieval")

    assert action["status"] == "prepared"
    assert action["parameters"]["provider_connection_id"] == push_provider_connection_id
    assert action["parameters"]["copy_draft"]["channel"] == "push"
    assert action["parameters"]["copy_draft"]["citations"][0]["citation_id"] == "C1"
    assert action["parameters"]["copy_draft"]["feedback_learning"]["profile_id"].startswith("aiflearn_")
    assert "saved checkpoint" in action["parameters"]["feedback_learning"]["prompt_context"]
    assert action["parameters"]["knowledge_context"]["retrieval_id"] == evidence_artifact["resource_id"]
    assert action["parameters"]["knowledge_context"]["retrieval_mode"] == "hybrid_v1"
    assert evidence_artifact["focus"]["citation_count"] >= 1
    assert evidence_artifact["focus"]["retrieval_mode"] == "hybrid_v1"
    assert evidence_artifact["focus"]["citations"][0]["document_title"] == "VIP Winback Playbook"
    assert "Evidence: [C1]" in payload["assistant_message"]


def test_copilot_agent_knowledge_grounding_uses_configured_vector_index(monkeypatch, tmp_path):
    with _client_with_env(
        monkeypatch,
        tmp_path,
        KNOWLEDGE_EMBEDDING_PROVIDER="openai",
        KNOWLEDGE_EMBEDDING_MODEL="text-embedding-3-small",
        KNOWLEDGE_VECTOR_STORE="pgvector",
        KNOWLEDGE_VECTOR_INDEX="agent_playbooks",
        KNOWLEDGE_VECTOR_NAMESPACE="lifecycle",
        KNOWLEDGE_VECTOR_SECRET_REF="secret://knowledge/vector",
    ) as client:
        headers = _headers("operator", actor_id="agent_configured_grounding")
        push_provider_connection_id = _create_push_provider_connection(client, headers, name="Configured Grounding Push")
        document = client.post(
            "/api/v1/knowledge/documents",
            headers=headers,
            json={
                "title": "Configured Vector Playbook",
                "source_type": "playbook",
                "source_name": "Lifecycle Marketing",
                "tags": ["push", "winback"],
                "content": (
                    "Configured vector retrieval should draft winback push copy with saved progress, "
                    "premium access, and a calm return-to-game reminder."
                ),
            },
        )
        assert document.status_code == 201, document.text
        session_id = _create_session(client, headers, title="Configured Vector Grounding")

        response = client.post(
            f"/api/v1/copilot/agent/sessions/{session_id}/messages",
            headers=headers,
            json={
                "message": "Draft a push title and body for saved progress winback from the configured vector playbook.",
                "ui_context": {},
            },
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        action = next(item for item in payload["completed_actions"] if item["action_type"] == "send_push_dispatch")
        evidence_artifact = next(item for item in payload["artifacts"] if item["resource_type"] == "knowledge_retrieval")
        assert action["parameters"]["provider_connection_id"] == push_provider_connection_id

        retrieval = client.get(
            f"/api/v1/knowledge/retrievals/{evidence_artifact['resource_id']}",
            headers=headers,
        )
        assert retrieval.status_code == 200, retrieval.text
        retrieval_payload = retrieval.json()
        assert retrieval_payload["vector_index"]["index_id"] == "agent_playbooks"
        assert retrieval_payload["vector_index"]["embedding_provider"] == "openai"
        assert retrieval_payload["citations"][0]["ranking_signals"]["vector_status"] == "ready"
        assert retrieval_payload["citations"][0]["ranking_signals"]["embedding_provider"] == "openai"


def test_copilot_agent_drafts_email_copy_into_campaign_for_approval(client, monkeypatch):
    headers = _headers("operator", actor_id="agent_operator")
    provider_connection_id = _create_sendgrid_provider_connection(client, headers, name="Agent Copy SendGrid")
    email_feedback = client.post(
        "/api/v1/experiments/ai-feedback",
        headers=headers,
        json={
            "feedback_type": "operator_approval",
            "target_type": "email_copy_draft",
            "target_id": "saved_progress_email",
            "weight": 0.7,
            "comments": "Prefer saved progress lifecycle email copy. Customer note: player@example.com.",
        },
    )
    assert email_feedback.status_code == 201, email_feedback.text
    push_feedback = client.post(
        "/api/v1/experiments/ai-feedback",
        headers=headers,
        json={
            "feedback_type": "operator_approval",
            "target_type": "push_copy_draft",
            "target_id": "push_only_checkpoint_copy",
            "weight": 0.9,
            "comments": "Push-only checkpoint copy should not guide email drafts.",
        },
    )
    assert push_feedback.status_code == 201, push_feedback.text
    prediction_job_id = "pred_email_copy"
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
    monkeypatch.setattr(
        "app.application.sendgrid_provider.SendGridProviderService.list_dynamic_templates",
        lambda self, provider_connection_id: [{"id": "tmpl_winback", "name": "Winback Template"}],
    )
    monkeypatch.setattr(
        "app.application.sendgrid_provider.SendGridProviderService.get_template_summary",
        lambda self, provider_connection_id, template_id: _template_detail_payload(template_id),
    )
    compose_payloads = []

    def _capture_compose_message(self, payload):
        compose_payloads.append(payload)
        return "Prepared draft for operator review."

    monkeypatch.setattr(
        "app.application.copilot_agent.ConfiguredCopilotAgentModel.compose_message",
        _capture_compose_message,
    )

    session_id = _create_session(client, headers, title="Agent Email Copy Session")
    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "\n".join(
                [
                    "Build a SendGrid campaign and draft email copy to call players back to the game.",
                    f"provider_connection_id: {provider_connection_id}",
                    "template_id: tmpl_winback",
                    f"prediction_job_id: {prediction_job_id}",
                    "schedule_at: 2026-05-05T10:00:00Z",
                    "campaign_name: agent_email_copy",
                ]
            ),
            "ui_context": {},
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    action = payload["completed_actions"][0]
    assert action["action_type"] == "setup_email_campaign"
    campaign = action["result"]["email_campaign"]
    assert campaign["subject"]
    assert "game" in campaign["body"].lower() or "back" in campaign["body"].lower()
    assert campaign["status"] == "draft"
    assert campaign["schedule_at"] is None
    assert action["result"]["requested_schedule_at"] == "2026-05-05T10:00:00Z"
    email_artifact = next(item for item in payload["artifacts"] if item["resource_type"] == "email_campaign")
    assert email_artifact["focus"]["schedule_at"] == "2026-05-05T10:00:00Z"
    assert action["parameters"]["copy_draft"]["channel"] == "email"
    assert action["parameters"]["copy_draft"]["feedback_learning"]["target_type"] == "email_copy_draft"
    assert "saved progress" in action["parameters"]["feedback_learning"]["prompt_context"]
    assert "player@example.com" not in action["parameters"]["feedback_learning"]["prompt_context"]
    assert "push_only_checkpoint_copy" not in action["parameters"]["feedback_learning"]["prompt_context"]
    assert compose_payloads
    assert all("feedback_learning" not in str(payload) for payload in compose_payloads)


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


@pytest.mark.parametrize(
    "message",
    [
        "How do I write an email subject/body here?",
        "How do I write a push title/body here?",
    ],
)
def test_copilot_agent_copy_help_prompts_stay_support_only(client, message):
    headers = _headers("analyst", actor_id="agent_copy_help")
    session_id = _create_session(client, headers, title="Copy Help Session")

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": message,
            "ui_context": {"active_module_id": "action-orchestrator", "active_page_id": "action-orchestrator"},
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["session_state"]["current_intent"] == "help_support"
    assert payload["completed_actions"] == []
    assert payload["pending_confirmations"] == []


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


def test_copilot_agent_retires_legacy_pending_confirmation_as_handoff(client):
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers, title="Legacy Confirmation Session")
    action_id = "cpaa_legacy_pending"
    confirmation_id = "cpac_legacy_pending"

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        action_payload = {
            "action_id": action_id,
            "session_id": session_id,
            "action_type": "send_push_dispatch",
            "title": "Send one-time push",
            "status": "awaiting_confirmation",
            "requires_confirmation": True,
            "risk_level": "high",
            "parameters": {
                "user_id": "legacy_user",
                "title": "Legacy",
                "body": "Review this message",
            },
            "result": {},
            "summary": "Waiting for confirmation.",
            "artifacts": [],
            "confirmation_id": confirmation_id,
            "confirmation_note": "",
            "is_async": False,
            "status_detail": "",
            "created_at": "2026-05-05T10:00:00",
            "updated_at": "2026-05-05T10:00:00",
        }
        repository.upsert_resource(ACTION_RESOURCE_TYPE, action_id, status="awaiting_confirmation", name="Send one-time push", payload=action_payload)
        repository.upsert_resource(
            CONFIRMATION_RESOURCE_TYPE,
            confirmation_id,
            status="pending",
            name="Send one-time push",
            payload={"confirmation_id": confirmation_id, "session_id": session_id, "action_id": action_id, "status": "pending"},
        )
        session.commit()

    session_payload = client.get(f"/api/v1/copilot/agent/sessions/{session_id}", headers=headers)
    assert session_payload.status_code == 200
    assert session_payload.json()["pending_confirmations"] == []
    assert session_payload.json()["session_state"]["pending_confirmation_count"] == 0
    assert session_payload.json()["session_state"]["latest_artifacts"][0]["resource_type"] == "action_handoff"

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        action = repository.get_resource(ACTION_RESOURCE_TYPE, action_id)["payload"]
        confirmation = repository.get_resource(CONFIRMATION_RESOURCE_TYPE, confirmation_id)["payload"]
    assert action["status"] == "prepared"
    assert action["requires_confirmation"] is False
    assert action["result"]["manual_handoff"] is True
    assert action["artifacts"][0]["resource_type"] == "action_handoff"
    assert confirmation["status"] == "retired"

    confirm_response = client.post(
        f"/api/v1/copilot/agent/actions/{action_id}/confirm",
        headers=headers,
        json={"note": "Try old confirm path."},
    )
    assert confirm_response.status_code == 409
    assert "module handoff" in confirm_response.json()["detail"]


def test_copilot_agent_confirm_path_retires_legacy_handoff_with_session_artifact(client):
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers, title="Legacy Confirm Path Session")
    action_id = "cpaa_legacy_confirm_path"
    confirmation_id = "cpac_legacy_confirm_path"

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        action_payload = {
            "action_id": action_id,
            "session_id": session_id,
            "action_type": "send_push_dispatch",
            "title": "Send one-time push",
            "status": "awaiting_confirmation",
            "requires_confirmation": True,
            "risk_level": "high",
            "parameters": {"user_id": "legacy_user", "title": "Legacy", "body": "Review this message"},
            "result": {},
            "summary": "Waiting for confirmation.",
            "artifacts": [],
            "confirmation_id": confirmation_id,
            "confirmation_note": "",
            "is_async": False,
            "status_detail": "",
            "created_at": "2026-05-05T10:00:00",
            "updated_at": "2026-05-05T10:00:00",
        }
        repository.upsert_resource(ACTION_RESOURCE_TYPE, action_id, status="awaiting_confirmation", name="Send one-time push", payload=action_payload)
        repository.upsert_resource(
            CONFIRMATION_RESOURCE_TYPE,
            confirmation_id,
            status="pending",
            name="Send one-time push",
            payload={"confirmation_id": confirmation_id, "session_id": session_id, "action_id": action_id, "status": "pending"},
        )
        session.commit()

    confirm_response = client.post(
        f"/api/v1/copilot/agent/actions/{action_id}/confirm",
        headers=headers,
        json={"note": "Try old confirm path first."},
    )
    assert confirm_response.status_code == 409

    session_payload = client.get(f"/api/v1/copilot/agent/sessions/{session_id}", headers=headers)
    assert session_payload.status_code == 200
    assert session_payload.json()["pending_confirmations"] == []
    assert session_payload.json()["session_state"]["latest_artifacts"][0]["resource_type"] == "action_handoff"
    assert session_payload.json()["session_state"]["latest_artifacts"][0]["focus"]["parameters"]["body"] == "Review this message"


def test_copilot_agent_drafts_audience_builder_state_artifact(client):
    headers = _headers("operator", actor_id="agent_builder_operator")
    _seed_completed_prediction_job(
        "pred_builder_a",
        source_name="Amplitude 1",
        rows=[
            {
                "prediction_job_id": "pred_builder_a",
                "user_id": "u_1",
                "canonical_user_id": "u_1",
                "email": "u1@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "local",
                "suggested_action": "email",
                "completed_at": "2026-03-09T10:00:00+00:00",
            }
        ],
    )
    _seed_completed_prediction_job(
        "pred_builder_b",
        import_job_id="imp_agentsource2",
        source_name="Adjust Source",
        rows=[
            {
                "prediction_job_id": "pred_builder_b",
                "user_id": "u_2",
                "canonical_user_id": "u_2",
                "email": "u2@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "cloud",
                "suggested_action": "push_notification",
                "completed_at": "2026-03-10T10:00:00+00:00",
            }
        ],
    )
    session_id = _create_session(client, headers, title="Agent Builder Session")

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "Draft a guided audience builder for high risk winback users from Amplitude 1 and Adjust Source.",
            "ui_context": {"active_module_id": "audience-engine", "active_page_id": "audience-engine"},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["completed_actions"][0]["action_type"] == "draft_audience_builder"
    builder_artifact = next(item for item in payload["artifacts"] if item["resource_type"] == "audience_builder_state")
    builder_state = builder_artifact["focus"]["builder_state"]
    assert builder_state["audience_basis"] == "prediction"
    assert sorted(builder_state["source_names"]) == ["Adjust Source", "Amplitude 1"]
    assert builder_state["conditions"][0]["field"] == "predicted_churn_risk"
    assert builder_state["preview"]["member_count"] == 2


def test_copilot_agent_guidance_handoff_for_risky_action(client):
    headers = _headers("operator", actor_id="agent_operator")
    session_id = _create_session(client, headers, title="Agent Handoff Session")

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

    handoff = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={"message": f"Activate cohort {cohort_id}", "ui_context": {}},
    )
    assert handoff.status_code == 200
    handoff_payload = handoff.json()
    assert handoff_payload["session_state"]["status"] == "active"
    assert handoff_payload["pending_confirmations"] == []
    action = handoff_payload["completed_actions"][0]
    assert action["action_type"] == "activate_cohort"
    assert action["status"] == "prepared"
    assert action["requires_confirmation"] is False
    assert action["result"]["manual_handoff"] is True
    assert "did not change the cohort" in handoff_payload["assistant_message"]

    cohort_detail = client.get(f"/api/v1/cohorts/{cohort_id}", headers=headers)
    assert cohort_detail.status_code == 200
    assert cohort_detail.json()["status"] == "draft"


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


def test_copilot_agent_model_profiles_allow_secret_refs_in_prod_without_inline_storage(client, monkeypatch):
    headers = _headers("operator", actor_id="agent_operator")
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.delenv("CONTROL_PLANE_SECRET_KEY", raising=False)
    SecretManagerService._get_control_plane_cipher.cache_clear()
    SecretManagerService._get_invalid_token_error.cache_clear()

    ref_profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "Gemini Secret Ref",
            "provider": "gemini",
            "model_name": "gemini-2.5-flash",
            "config": {"api_key_ref": "secret://ask-ai/gemini-api-key"},
        },
    )
    assert ref_profile.status_code == 201, ref_profile.text
    ref_payload = ref_profile.json()
    assert ref_payload["config"]["api_key"] is None
    assert ref_payload["config"]["api_key_configured"] is True

    inline_profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "Gemini Inline Secret",
            "provider": "gemini",
            "model_name": "gemini-2.5-flash",
            "config": {"api_key": "inline-prod-key"},
        },
    )
    assert inline_profile.status_code == 409
    assert (
        inline_profile.json()["detail"]
        == "Inline agent model secrets are not allowed in production; configure CONTROL_PLANE_SECRET_KEY or use *_ref fields."
    )


def test_copilot_agent_model_profiles_encrypt_inline_prod_secret_and_replace_it_with_ref(client, monkeypatch):
    headers = _headers("operator", actor_id="agent_operator")
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("CONTROL_PLANE_SECRET_KEY", "test-control-plane-secret-key")
    SecretManagerService._get_control_plane_cipher.cache_clear()
    SecretManagerService._get_invalid_token_error.cache_clear()

    inline_profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "Gemini Inline Storage",
            "provider": "gemini",
            "model_name": "gemini-2.5-flash",
            "config": {"api_key": "inline-prod-key"},
        },
    )
    assert inline_profile.status_code == 201, inline_profile.text
    model_profile_id = inline_profile.json()["model_profile_id"]
    assert inline_profile.json()["config"]["api_key"] is None
    assert inline_profile.json()["config"]["api_key_configured"] is True

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        stored = repository.get_resource("agent_model_profile", model_profile_id)
    stored_config = stored["payload"]["config"]
    assert "api_key" not in stored_config
    assert "api_key_encrypted" in stored_config

    updated_profile = client.patch(
        f"/api/v1/copilot/agent/model-profiles/{model_profile_id}",
        headers=headers,
        json={"config": {"api_key_ref": "secret://ask-ai/gemini-api-key"}},
    )
    assert updated_profile.status_code == 200, updated_profile.text
    assert updated_profile.json()["config"]["api_key"] is None
    assert updated_profile.json()["config"]["api_key_configured"] is True

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        replaced = repository.get_resource("agent_model_profile", model_profile_id)
    replaced_config = replaced["payload"]["config"]
    assert replaced_config["api_key_ref"] == "secret://ask-ai/gemini-api-key"
    assert "api_key" not in replaced_config
    assert "api_key_encrypted" not in replaced_config


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
