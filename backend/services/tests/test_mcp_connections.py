from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient

from app.application.mcp_connections import McpConnectionService
from app.core import db as db_module
from app.core.db import session_scope
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
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


def _headers(
    role: str = "operator",
    *,
    actor_id: str | None = None,
    tenant_id: str = "default",
    project_id: str = "default",
) -> dict[str, str]:
    return {
        "x-actor-role": role,
        "x-actor-id": actor_id or f"{role}_user",
        "x-tenant-id": tenant_id,
        "x-project-id": project_id,
    }


def _create_mcp_connection(client: TestClient, headers: dict[str, str], *, name: str = "Amplitude MCP") -> dict:
    response = client.post(
        "/api/v1/mcp-connections",
        headers=headers,
        json={
            "name": name,
            "preset_key": "amplitude_us",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def _create_session(client: TestClient, headers: dict[str, str], *, title: str = "MCP Agent Session") -> str:
    response = client.post(
        "/api/v1/copilot/agent/sessions",
        headers=headers,
        json={"title": title, "ui_context": {}},
    )
    assert response.status_code == 201, response.text
    return response.json()["session_state"]["session_id"]


@pytest.mark.parametrize(
    "endpoint_url",
    [
        "http://mcp.amplitude.com/mcp",
        "https://localhost:9000/mcp",
        "https://10.0.0.8/mcp",
        "https://analytics.internal/mcp",
    ],
)
def test_mcp_connection_validate_rejects_non_public_endpoints(client: TestClient, endpoint_url: str):
    response = client.post(
        "/api/v1/mcp-connections/validate",
        headers=_headers(),
        json={"preset_key": "custom", "endpoint_url": endpoint_url},
    )
    assert response.status_code == 409
    assert "MCP" in response.json()["detail"]


def test_mcp_connection_oauth_callback_preserves_project_scope_and_actor_isolation(client: TestClient, monkeypatch):
    async def fake_prepare(self, connection, *, callback_url):
        return {
            "authorization_url": "https://auth.example.test/authorize?state=oauth-state-demo",
            "client_id": "client-demo",
            "state": "oauth-state-demo",
            "code_verifier": "verifier-demo",
            "token_endpoint": "https://auth.example.test/token",
            "resource": "https://mcp.amplitude.com",
        }

    async def fake_exchange(self, connection, *, state_payload, code):
        assert code == "demo-code"
        return {
            "status": "authorized",
            "authorized_at": "2026-04-17T12:00:00Z",
            "expires_at": "2026-04-17T13:00:00Z",
            "last_error": "",
            "tokens": {
                "access_token": "access-token-demo",
                "refresh_token": "refresh-token-demo",
                "token_type": "Bearer",
            },
        }

    async def fake_list_tools(self, connection, *, auth_payload):
        return [
            {
                "name": "search",
                "description": "Search analytics entities",
                "allowed": True,
                "classification": "read_only",
                "input_schema": {"type": "object"},
            },
            {
                "name": "delete_dashboard",
                "description": "Mutates remote state",
                "allowed": False,
                "classification": "blocked",
                "input_schema": {"type": "object"},
            },
        ]

    monkeypatch.setattr(McpConnectionService, "_prepare_oauth_context", fake_prepare)
    monkeypatch.setattr(McpConnectionService, "_exchange_oauth_code", fake_exchange)
    monkeypatch.setattr(McpConnectionService, "_list_remote_tools", fake_list_tools)

    alpha_headers = _headers(actor_id="alpha_operator", project_id="alpha")
    created = _create_mcp_connection(client, alpha_headers, name="Alpha Amplitude MCP")
    connection_id = created["mcp_connection_id"]

    start = client.post(
        f"/api/v1/mcp-connections/{connection_id}/connect/start",
        headers=alpha_headers,
    )
    assert start.status_code == 200, start.text
    state = parse_qs(urlparse(start.json()["authorization_url"]).query)["state"][0]

    callback = client.get(
        "/api/v1/mcp-connections/connect/callback",
        params={"state": state, "code": "demo-code"},
    )
    assert callback.status_code == 200
    assert "Connected" in callback.text

    alpha_connection = client.get(f"/api/v1/mcp-connections/{connection_id}", headers=alpha_headers)
    assert alpha_connection.status_code == 200, alpha_connection.text
    alpha_payload = alpha_connection.json()
    assert alpha_payload["project_id"] == "alpha"
    assert alpha_payload["authorization"]["status"] == "authorized"
    assert alpha_payload["authorization"]["has_refresh_token"] is True
    assert alpha_payload["allowed_tools"] == ["search"]
    assert {item["name"]: item["classification"] for item in alpha_payload["discovered_tools"]} == {
        "search": "read_only",
        "delete_dashboard": "blocked",
    }

    other_actor_headers = _headers(actor_id="beta_operator", project_id="alpha")
    other_actor_connection = client.get(f"/api/v1/mcp-connections/{connection_id}", headers=other_actor_headers)
    assert other_actor_connection.status_code == 200
    assert other_actor_connection.json()["authorization"]["status"] == "not_authorized"

    blocked_refresh = client.post(
        f"/api/v1/mcp-connections/{connection_id}/refresh-tools",
        headers=other_actor_headers,
    )
    assert blocked_refresh.status_code == 409
    assert "authorize" in blocked_refresh.json()["detail"].lower()

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        auth_record = repository.get_resource(
            "mcp_connection_authorization",
            f"{connection_id}:alpha_operator",
            tenant_id="default",
            project_id="alpha",
        )
    assert auth_record is not None
    tokens = dict((auth_record.get("payload") or {}).get("tokens") or {})
    assert "access_token" not in tokens
    assert "refresh_token" not in tokens
    assert tokens.get("access_token_encrypted")
    assert tokens.get("refresh_token_encrypted")


def test_mcp_snapshot_import_and_cohort_creation_normalize_identifier_fields(client: TestClient):
    headers = _headers(actor_id="snapshot_operator")
    connection_id = _create_mcp_connection(client, headers, name="Snapshot MCP")["mcp_connection_id"]

    snapshot = client.post(
        f"/api/v1/mcp-connections/{connection_id}/snapshots",
        headers=headers,
        json={
            "name": "Top Risk Snapshot",
            "query_result": {
                "question": "Who should we target next?",
                "answer": "Two importable rows found.",
                "rows": [
                    {"player_id": "player-1", "country": "US"},
                    {"user_id": "user-2", "country": "CA"},
                ],
                "tool_calls": [],
                "result": {},
            },
        },
    )
    assert snapshot.status_code == 201, snapshot.text
    snapshot_payload = snapshot.json()
    assert snapshot_payload["identifier_fields"] == ["player_id", "user_id"]

    cohort_response = client.post(
        f"/api/v1/mcp-connections/snapshots/{snapshot_payload['snapshot_id']}/cohorts",
        headers=headers,
        json={"name": "Top Risk MCP Cohort"},
    )
    assert cohort_response.status_code == 201, cohort_response.text
    cohort = cohort_response.json()["cohort"]
    members = list((cohort.get("definition") or {}).get("members") or [])
    assert [item["canonical_user_id"] for item in members] == ["player-1", "user-2"]


def test_copilot_agent_runs_selected_mcp_connection(client: TestClient, monkeypatch):
    def fake_run_prompt(self, mcp_connection_id, *, actor_id, question, model_adapter, session_state, ui_context):
        assert mcp_connection_id == ui_context["selected_mcp_connection_id"]
        return {
            "query_id": "mcpq_demo",
            "mcp_connection_id": mcp_connection_id,
            "question": question,
            "answer": "Queried the MCP connection successfully.",
            "rows": [{"canonical_user_id": "user-1", "event_name": "purchase"}],
            "tool_calls": [
                {
                    "thought": "Use the read-only search tool first.",
                    "tool_name": "search",
                    "arguments": {"query": question},
                    "result": {"rows": [{"canonical_user_id": "user-1", "event_name": "purchase"}]},
                }
            ],
            "result": {"tool_calls": [{"tool_name": "search"}]},
        }

    monkeypatch.setattr(McpConnectionService, "run_prompt", fake_run_prompt)

    headers = _headers(actor_id="copilot_operator")
    connection_id = _create_mcp_connection(client, headers, name="Copilot MCP")["mcp_connection_id"]
    session_id = _create_session(client, headers)

    response = client.post(
        f"/api/v1/copilot/agent/sessions/{session_id}/messages",
        headers=headers,
        json={
            "message": "What are the top conversion events this week?",
            "ui_context": {"selected_mcp_connection_id": connection_id},
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["completed_actions"][0]["action_type"] == "query_mcp_connection"
    assert payload["completed_actions"][0]["result"]["query_result"]["query_id"] == "mcpq_demo"
    assert {artifact["resource_type"] for artifact in payload["artifacts"]} == {"mcp_connection", "mcp_query_result"}
    assert payload["session_state"]["status"] == "active"
