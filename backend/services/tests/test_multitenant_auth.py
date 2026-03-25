from __future__ import annotations

import hashlib
import hmac
import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
from fastapi.testclient import TestClient

from app.core import db as db_module
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache


OIDC_ISSUER = "https://issuer.example.com"
OIDC_AUDIENCE = "kairyx-tests"
OIDC_SECRET = "test-signing-secret"


def _make_token(subject: str, *, platform_admin: bool = False, extra_claims: dict | None = None) -> str:
    now = datetime.now(timezone.utc)
    claims = {
        "sub": subject,
        "iss": OIDC_ISSUER,
        "aud": OIDC_AUDIENCE,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=1)).timestamp()),
        "email": f"{subject}@example.com",
        "name": subject,
    }
    if platform_admin:
        claims["roles"] = ["platform_admin"]
        claims["kairyx_platform_admin"] = True
    if extra_claims:
        claims.update(extra_claims)
    return jwt.encode(claims, OIDC_SECRET, algorithm="HS256")


def _auth_headers(token: str, tenant_id: str | None = None, project_id: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
    }
    if tenant_id is not None:
        headers["X-Kairyx-Tenant"] = tenant_id
    if project_id is not None:
        headers["X-Kairyx-Project"] = project_id
    return headers


@contextmanager
def _client(monkeypatch, tmp_path: Path, **env_overrides: str):
    monkeypatch.chdir(tmp_path)
    env = {
        "APP_ENV": "local",
        "DATA_BACKEND_MODE": "mock",
        "CONTROL_PLANE_DATABASE_URL": f"sqlite:///{tmp_path / 'control_plane.db'}",
        "KAIRYX_LOCAL_DB_PATH": str(tmp_path / "local_jobs.db"),
        "SCHEDULER_ENABLED": "false",
        "OIDC_ISSUER": OIDC_ISSUER,
        "OIDC_AUDIENCE": OIDC_AUDIENCE,
        "OIDC_JWT_SIGNING_SECRET": OIDC_SECRET,
        "LEGACY_HEADER_AUTH_ENABLED": "false",
    }
    env.update({key: str(value) for key, value in env_overrides.items()})
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    clear_shared_bigquery_service_cache()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def test_jwt_auth_bootstraps_membership_and_rejects_wrong_tenant(monkeypatch, tmp_path):
    admin_token = _make_token("platform-admin", platform_admin=True)
    user_token = _make_token("studio-operator")
    with _client(monkeypatch, tmp_path) as client:
        tenant = client.post(
            "/api/v1/tenants",
            headers=_auth_headers(admin_token, "default"),
            json={"tenant_id": "studio-a", "name": "Studio A"},
        )
        assert tenant.status_code == 201

        membership = client.put(
            "/api/v1/tenants/studio-a/memberships/studio-operator",
            headers=_auth_headers(admin_token, "default"),
            json={
                "user_id": "studio-operator",
                "role": "operator",
                "status": "active",
                "email": "studio-operator@example.com",
                "display_name": "Studio Operator",
            },
        )
        assert membership.status_code == 200

        me = client.get("/studio-a/v1/auth/me", headers=_auth_headers(user_token))
        assert me.status_code == 200
        assert me.json()["display_name"] == "studio-operator"
        assert me.json()["email"] == "studio-operator@example.com"
        assert me.json()["organization_id"] == "studio-a"
        assert me.json()["organization"]["organization_id"] == "studio-a"
        assert me.json()["auth_mode"] == "jwt"
        assert "actor_id" not in me.json()
        assert "tenant_id" not in me.json()

        wrong_tenant = client.get("/studio-b/v1/auth/me", headers=_auth_headers(user_token))
        assert wrong_tenant.status_code == 403


def test_jwt_user_without_membership_can_onboard_but_cannot_access_product_routes(monkeypatch, tmp_path):
    user_token = _make_token("founder")
    with _client(monkeypatch, tmp_path) as client:
        me = client.get("/api/v1/auth/me", headers=_auth_headers(user_token))
        assert me.status_code == 200
        assert me.json()["needs_onboarding"] is True
        assert me.json()["organization_id"] is None
        assert me.json()["project_id"] is None

        blocked = client.get("/api/v1/connectors", headers=_auth_headers(user_token))
        assert blocked.status_code == 403


def test_org_space_onboarding_project_creation_and_invite_redemption(monkeypatch, tmp_path):
    founder_token = _make_token("founder")
    teammate_token = _make_token(
        "studio-analyst",
        extra_claims={
            "email": "studio-analyst@example.com",
            "name": "Studio Analyst",
        },
    )
    with _client(monkeypatch, tmp_path) as client:
        onboard = client.post(
            "/api/v1/onboarding/organization-space",
            headers=_auth_headers(founder_token),
            json={
                "organization_id": "northstar",
                "organization_name": "North Star Games",
                "project_id": "liveops",
                "project_name": "Live Ops",
                "project_description": "Primary production project",
            },
        )
        assert onboard.status_code == 201
        assert onboard.json()["organization_space"]["tenant_id"] == "northstar"
        assert onboard.json()["project"]["project_id"] == "liveops"

        founder_me = client.get("/northstar/v1/auth/me", headers=_auth_headers(founder_token, project_id="liveops"))
        assert founder_me.status_code == 200
        assert founder_me.json()["organization_id"] == "northstar"
        assert founder_me.json()["project_id"] == "liveops"
        assert founder_me.json()["organization_role"] == "owner"
        assert founder_me.json()["project_role"] == "admin"
        assert founder_me.json()["needs_onboarding"] is False

        project = client.post(
            "/northstar/v1/projects",
            headers=_auth_headers(founder_token, project_id="liveops"),
            json={
                "project_id": "sandbox",
                "name": "Sandbox",
                "description": "Experiment workspace",
            },
        )
        assert project.status_code == 201
        assert project.json()["project"]["project_id"] == "sandbox"

        founder_sandbox = client.get("/northstar/v1/auth/me", headers=_auth_headers(founder_token, project_id="sandbox"))
        assert founder_sandbox.status_code == 200
        assert founder_sandbox.json()["project_role"] == "admin"

        invite = client.post(
            "/northstar/v1/projects/liveops/invites",
            headers=_auth_headers(founder_token, project_id="liveops"),
            json={
                "email": "studio-analyst@example.com",
                "display_name": "Studio Analyst",
                "org_role": "member",
                "project_role": "analyst",
            },
        )
        assert invite.status_code == 201
        invite_code = invite.json()["invite"]["invite_code"]

        redeem = client.post(
            "/api/v1/project-invites/redeem",
            headers=_auth_headers(teammate_token),
            json={"invite_code": invite_code},
        )
        assert redeem.status_code == 200
        assert redeem.json()["project"]["project_id"] == "liveops"

        teammate_me = client.get("/northstar/v1/auth/me", headers=_auth_headers(teammate_token, project_id="liveops"))
        assert teammate_me.status_code == 200
        assert teammate_me.json()["organization_role"] == "member"
        assert teammate_me.json()["project_role"] == "analyst"
        assert [item["project_id"] for item in teammate_me.json()["accessible_projects"]] == ["liveops"]

        wrong_project = client.get("/northstar/v1/auth/me", headers=_auth_headers(teammate_token, project_id="sandbox"))
        assert wrong_project.status_code == 403


def test_org_scoped_v1_path_selects_membership_without_tenant_header(monkeypatch, tmp_path):
    founder_token = _make_token("founder")
    with _client(monkeypatch, tmp_path) as client:
        onboard = client.post(
            "/api/v1/onboarding/organization-space",
            headers=_auth_headers(founder_token),
            json={
                "organization_id": "northstar",
                "organization_name": "North Star Games",
                "project_id": "liveops",
                "project_name": "Live Ops",
                "project_description": "Primary production project",
            },
        )
        assert onboard.status_code == 201

        me = client.get(
            "/northstar/v1/auth/me",
            headers={
                "Authorization": f"Bearer {founder_token}",
                "X-Kairyx-Project": "liveops",
            },
        )
        assert me.status_code == 200
        assert me.json()["organization_id"] == "northstar"
        assert me.json()["project_id"] == "liveops"

        mismatch = client.get(
            "/northstar/v1/auth/me",
            headers=_auth_headers(founder_token, "other-org", "liveops"),
        )
        assert mismatch.status_code == 409


def test_secret_bearing_responses_are_redacted(monkeypatch, tmp_path):
    admin_token = _make_token("platform-admin", platform_admin=True)
    headers = _auth_headers(admin_token, "default")
    with _client(monkeypatch, tmp_path) as client:
        connector = client.post(
            "/api/v1/connectors",
            headers=headers,
            json={"name": "Adjust Source", "type": "adjust", "config": {"api_token": "adjust-token"}},
        )
        assert connector.status_code == 201
        assert connector.json()["config"]["api_token"] is None
        assert connector.json()["config"]["api_token_configured"] is True

        provider_connection = client.post(
            "/api/v1/provider-connections",
            headers=headers,
            json={
                "name": "Webhook Signing",
                "provider": "webhook",
                "config": {
                    "webhook_url": "https://example.invalid/webhook",
                    "webhook_token": "secret-token",
                    "callback_signing_secret": "callback-secret",
                },
            },
        )
        assert provider_connection.status_code == 201
        assert provider_connection.json()["config"]["webhook_token"] is None
        assert provider_connection.json()["config"]["callback_signing_secret"] is None

        cohort = client.post(
            "/api/v1/cohorts",
            headers=headers,
            json={
                "name": "list_cohort",
                "type": "list",
                "definition": {"members": [{"canonical_user_id": "u_1", "email": "u1@example.com"}]},
                "activate": True,
            },
        )
        assert cohort.status_code == 201
        cohort_id = cohort.json()["cohort_id"]

        experiment = client.post(
            "/api/v1/experiments/config?experiment_id=wf_exp",
            headers=headers,
            json={"enabled": True, "status": "active", "primary_metric": "return_rate", "min_sample_size": 1, "min_runtime_hours": 0},
        )
        assert experiment.status_code == 200

        workflow = client.post(
            "/api/v1/workflows",
            headers=headers,
            json={
                "name": "redacted_workflow",
                "cohort_id": cohort_id,
                "schedule": {"type": "daily"},
                "action": {
                    "channel": "webhook",
                    "content": "hello",
                    "webhook_url": "https://example.invalid/webhook",
                    "webhook_token": "inline-token",
                },
                "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
                "experiment_id": "wf_exp",
            },
        )
        assert workflow.status_code == 201
        assert workflow.json()["channel_config"]["webhook_token"] is None
        assert workflow.json()["definition"]["action"]["webhook_token"] is None


def test_provider_callback_signature_verification(monkeypatch, tmp_path):
    admin_token = _make_token("platform-admin", platform_admin=True)
    headers = _auth_headers(admin_token, "default")
    with _client(monkeypatch, tmp_path) as client:
        provider_connection = client.post(
            "/api/v1/provider-connections",
            headers=headers,
            json={
                "name": "Signed Webhook",
                "provider": "webhook",
                "config": {
                    "webhook_url": "http://127.0.0.1:9/unreachable",
                    "callback_signing_secret": "signed-secret",
                },
            },
        )
        assert provider_connection.status_code == 201
        provider_connection_id = provider_connection.json()["provider_connection_id"]

        cohort = client.post(
            "/api/v1/cohorts",
            headers=headers,
            json={
                "name": "callback_cohort",
                "type": "list",
                "definition": {"members": [{"canonical_user_id": "u_1", "email": "u1@example.com"}]},
                "activate": True,
            },
        )
        assert cohort.status_code == 201
        cohort_id = cohort.json()["cohort_id"]

        experiment = client.post(
            "/api/v1/experiments/config?experiment_id=callback_exp",
            headers=headers,
            json={"enabled": True, "status": "active", "primary_metric": "return_rate", "min_sample_size": 1, "min_runtime_hours": 0},
        )
        assert experiment.status_code == 200

        workflow = client.post(
            "/api/v1/workflows",
            headers=headers,
            json={
                "name": "callback_workflow",
                "cohort_id": cohort_id,
                "schedule": {"type": "daily"},
                "action": {
                    "channel": "webhook",
                    "content": "hello",
                    "provider_connection_id": provider_connection_id,
                },
                "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
                "experiment_id": "callback_exp",
            },
        )
        assert workflow.status_code == 201
        workflow_id = workflow.json()["workflow_id"]

        publish = client.post(f"/api/v1/workflows/{workflow_id}/publish", headers=headers)
        assert publish.status_code == 200

        run = client.post(f"/api/v1/workflows/{workflow_id}/test-run", headers=headers, json={"limit": 1, "sandbox": False})
        assert run.status_code == 200
        assert run.json()["failures"] >= 1

        deliveries = client.get(f"/api/v1/workflows/{workflow_id}/deliveries", headers=headers)
        assert deliveries.status_code == 200
        delivery = deliveries.json()["items"][0]

        callback_payload = {
            "callbacks": [
                {
                    "delivery_id": delivery["delivery_id"],
                    "action_execution_id": delivery["action_execution_id"],
                    "workflow_id": workflow_id,
                    "user_id": delivery["user_id"],
                    "event_id": "evt_1",
                    "event_type": "returned",
                    "occurred_at": "2026-03-22T12:00:00",
                }
            ]
        }
        raw_body = json.dumps(callback_payload).encode("utf-8")
        valid_signature = hmac.new(b"signed-secret", raw_body, hashlib.sha256).hexdigest()

        callback = client.post(
            "/api/v1/activation/callbacks/webhook",
            headers={**headers, "Content-Type": "application/json", "X-Kairyx-Signature": valid_signature},
            content=raw_body,
        )
        assert callback.status_code == 200
        assert callback.json()["ingested"] == 1

        invalid = client.post(
            "/api/v1/activation/callbacks/webhook",
            headers={**headers, "Content-Type": "application/json", "X-Kairyx-Signature": "bad-signature"},
            content=raw_body,
        )
        assert invalid.status_code == 409


def test_sql_preview_and_copilot_limits(monkeypatch, tmp_path):
    admin_token = _make_token("platform-admin", platform_admin=True)
    headers = _auth_headers(admin_token, "default")
    with _client(
        monkeypatch,
        tmp_path,
        MAX_SQL_PREVIEW_ROWS_PER_TENANT="5",
        MAX_COPILOT_REPORTS_PER_TENANT="1",
    ) as client:
        preview = client.post(
            "/api/v1/sql-workspace/preview",
            headers=headers,
            json={"sql": "SELECT 1 AS value", "limit": 10},
        )
        assert preview.status_code == 400
        assert "tenant cap" in preview.json()["detail"]

        first_report = client.post(
            "/api/v1/copilot/report",
            headers=headers,
            json={"report_type": "daily", "time_window": "7d"},
        )
        assert first_report.status_code == 200

        second_report = client.post(
            "/api/v1/copilot/report",
            headers=headers,
            json={"report_type": "daily", "time_window": "7d"},
        )
        assert second_report.status_code == 409
