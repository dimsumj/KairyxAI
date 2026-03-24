from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import threading
import time

from alembic import command
from alembic.config import Config
import pytest
from fastapi.testclient import TestClient

from app.application.imports import ImportService
from app.core import db as db_module
from app.core.deps import get_settings_dependency
from app.core.runtime import clear_shutdown_requested, mark_shutdown_requested
from app.core.settings import get_settings
from app.infrastructure.db_models import ImportJobModel, PredictionJobModel
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from app.main import create_app
from bigquery_service import BigQueryService
from gcs_service import GcsService


def _alembic_config(tmp_path: Path) -> Config:
    services_dir = Path(__file__).resolve().parents[1]
    config = Config(str(services_dir / "alembic.ini"))
    config.set_main_option("script_location", str(services_dir / "alembic"))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{tmp_path / 'control_plane.db'}")
    return config


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def test_v1_connectors_and_mappings_persist(client):
    resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert resp.status_code == 201
    assert resp.json()["name"] == "Adjust Source"

    health = client.get("/api/v1/connectors/Adjust%20Source/health")
    assert health.status_code == 200
    assert health.json()["ok"] is True

    mapping = client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={"mapping": {"canonical_user_id": "event_properties.player_id"}},
    )
    assert mapping.status_code == 200
    assert mapping.json()["mapping"]["canonical_user_id"] == "event_properties.player_id"

    listed = client.get("/api/v1/connectors")
    assert listed.status_code == 200
    assert len(listed.json()) == 1


def test_root_serves_frontend_shell(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "/static/operator-console.css" in resp.text
    assert "/static/operator-console.js" in resp.text


def test_root_serves_frontend_static_assets(client):
    css_resp = client.get("/static/operator-console.css")
    assert css_resp.status_code == 200
    assert "text/css" in css_resp.headers["content-type"]
    assert "--bg-color" in css_resp.text

    js_resp = client.get("/static/operator-console.js")
    assert js_resp.status_code == 200
    assert "javascript" in js_resp.headers["content-type"]
    assert "document.addEventListener('DOMContentLoaded'" in js_resp.text
    assert "/api/v1" in js_resp.text

def test_root_health_alias(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["service"] == "KairyxAI Operator API"


def test_health_live_aliases_return_lightweight_payload(client):
    root_resp = client.get("/health/live")
    assert root_resp.status_code == 200
    root_payload = root_resp.json()
    assert root_payload["status"] == "ok"
    assert root_payload["mode"] == "mock"
    assert "data_aliases" not in root_payload

    api_resp = client.get("/api/v1/health/live")
    assert api_resp.status_code == 200
    api_payload = api_resp.json()
    assert api_payload["status"] == "ok"
    assert api_payload["mode"] == "mock"
    assert "data_aliases" not in api_payload
    assert api_payload["service"] == "KairyxAI Operator API"
    assert api_payload["time"]


def test_health_reports_local_cache_stats(client):
    health = client.get("/api/v1/health")
    assert health.status_code == 200
    payload = health.json()
    assert payload["mode"] == "mock"
    assert payload["local_cache"]["retention_days"] == 7
    assert payload["local_cache"]["tables"]["events_staging"]["rows"] >= 0
    assert payload["local_cache"]["tables"]["prediction_results"]["rows"] >= 0


def test_prediction_model_runs_reports_untrained_readiness(client):
    runs = client.get("/api/v1/predictions/models/runs", headers={"x-actor-role": "analyst"})
    assert runs.status_code == 200
    payload = runs.json()
    readiness = payload["readiness"]

    assert payload["items"] == []
    assert payload["training_status"] == {}
    assert readiness["state"] == "untrained"
    assert readiness["using_model_version"] == "heuristic_v1"
    assert readiness["baseline_rows"] == 0
    assert readiness["min_rows_required"] == 12


def test_health_live_bypasses_api_key_guard(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("API_ACCESS_KEY", "top-secret")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as test_client:
        live_resp = test_client.get("/api/v1/health/live")
        assert live_resp.status_code == 200
        assert live_resp.json()["status"] == "ok"

        protected_resp = test_client.get("/api/v1/connectors")
        assert protected_resp.status_code == 401


def test_sqlite_control_plane_uses_wal_and_busy_timeout(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setenv("SQLITE_BUSY_TIMEOUT_SECONDS", "12")
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app):
        engine = db_module.get_engine()
        connection = engine.raw_connection()
        try:
            cursor = connection.cursor()
            cursor.execute("PRAGMA journal_mode;")
            assert str(cursor.fetchone()[0]).lower() == "wal"
            cursor.execute("PRAGMA busy_timeout;")
            assert int(cursor.fetchone()[0]) >= 12000
        finally:
            cursor.close()
            connection.close()


def test_startup_upgrades_legacy_sqlite_control_plane_without_alembic_version(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))

    command.upgrade(_alembic_config(tmp_path), "20260310_0002")
    legacy_connection = sqlite3.connect(tmp_path / "control_plane.db")
    try:
        legacy_connection.execute("DROP TABLE alembic_version")
        legacy_connection.commit()
    finally:
        legacy_connection.close()

    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as test_client:
        listed = test_client.get("/api/v1/connectors")
        assert listed.status_code == 200
        assert listed.json() == []

    upgraded_connection = sqlite3.connect(tmp_path / "control_plane.db")
    try:
        connector_columns = {
            row[1]
            for row in upgraded_connection.execute("PRAGMA table_info('connector_configs')")
        }
        assert "tenant_id" in connector_columns

        version_row = upgraded_connection.execute("SELECT version_num FROM alembic_version").fetchone()
        assert version_row is not None
        assert version_row[0] == "20260322_0003"
    finally:
        upgraded_connection.close()


def test_create_import_returns_423_when_control_plane_database_is_locked(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setenv("SQLITE_BUSY_TIMEOUT_SECONDS", "0.1")
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as test_client:
        connector_resp = test_client.post(
            "/api/v1/connectors",
            json={
                "name": "Adjust Source",
                "type": "adjust",
                "config": {"api_token": "adjust-token"},
            },
        )
        assert connector_resp.status_code == 201

        lock_connection = sqlite3.connect(tmp_path / "control_plane.db", timeout=0.01, isolation_level=None)
        try:
            lock_connection.execute("PRAGMA busy_timeout=10;")
            lock_connection.execute("BEGIN EXCLUSIVE;")
            response = test_client.post(
                "/api/v1/imports",
                json={
                    "source_name": "Adjust Source",
                    "start_date": "20260301",
                    "end_date": "20260302",
                },
            )
        finally:
            lock_connection.rollback()
            lock_connection.close()

    assert response.status_code == 423
    assert response.json()["detail"] == "Control plane database is busy; retry shortly."
    assert response.headers["Retry-After"] == "1"


def test_v1_import_prediction_and_export_flow(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()
    assert import_job["spec"]["display_name"].startswith("Adjust Source-")

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200
    assert run_import.json()["status"] == "completed"

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "completed"
    assert run_prediction.json()["progress"]["details"]["execution_label"] == "Local Model"
    assert run_prediction.json()["progress"]["details"]["prediction_mode"] == "local"
    assert run_prediction.json()["progress"]["details"]["effective_local_model_version"] == "heuristic_v1"
    assert run_prediction.json()["progress"]["details"]["effective_local_model_state"] == "untrained"

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    payload = results.json()
    assert payload["total"] >= 1
    assert payload["items"][0]["user_id"] == "adjust_user_1001"
    assert payload["items"][0]["effective_local_model_version"] == "heuristic_v1"
    assert payload["items"][0]["effective_local_model_state"] == "untrained"

    captured = {}

    class DummyResponse:
        status_code = 202

        def raise_for_status(self):
            return None

    def fake_post(url, json=None, headers=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        return DummyResponse()

    monkeypatch.setattr("app.application.exports.requests.post", fake_post)

    create_export = client.post(
        "/api/v1/exports",
        json={
            "prediction_job_id": prediction_job["id"],
            "provider": "webhook",
            "channel": "push_notification",
            "include_churned": True,
            "include_risks": ["high", "medium", "low", "already_churned"],
            "webhook_url": "https://example.com/hook",
        },
    )
    assert create_export.status_code == 201
    export_job = create_export.json()

    run_export = client.post(export_job["links"]["self"] + "/run")
    assert run_export.status_code == 200
    assert run_export.json()["status"] == "completed"
    assert captured["url"] == "https://example.com/hook"
    assert captured["json"]["count"] >= 1


def test_prediction_local_mode_ignores_saved_google_connector(client, monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_GEMINI_MODEL", raising=False)

    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    gemini_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Google Gemini 1",
            "type": "google",
            "config": {
                "api_key": "google-api-key-from-connector",
                "model_name": "gemini-2.5-flash",
            },
        },
    )
    assert gemini_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()
    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200

    captured = {}

    class UnexpectedGeminiClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("local prediction mode should not construct GeminiClient")

    class FakePlayerModelingEngine:
        def __init__(self, gemini_client, bigquery_service, churn_inactive_days=14, job_id=None):
            captured["modeling_gemini_client_present"] = gemini_client is not None
            captured["job_id"] = job_id

        def get_all_player_ids(self):
            return ["player-1"]

        def build_player_profile(self, player_id):
            return {
                "player_id": player_id,
                "email": "player-1@example.com",
                "first_seen_date": "2026-03-01T00:00:00",
                "last_seen_date": "2026-03-06T00:00:00",
                "total_sessions": 4,
                "total_events": 12,
                "total_revenue": 9.99,
                "days_since_last_seen": 1,
                "churn_state": "active",
                "churn_inactive_days": 14,
            }

        async def estimate_churn_risk(self, player_id, player_profile=None):
            captured["estimate_called"] = True
            return {
                "player_id": player_id,
                "churn_state": "active",
                "churn_risk": "high",
                "reason": "Local model estimate",
                "top_signals": [{"signal": "recent_drop", "value": 3}],
            }

    class FakeDecisionEngine:
        def __init__(self, gemini_client):
            captured["decision_gemini_client_present"] = gemini_client is not None

        def decide_next_action(self, player_profile, churn_estimate, objective):
            captured["decision_called"] = True
            return {"content": "Local retention action"}

    monkeypatch.setattr("app.application.predictions.GeminiClient", UnexpectedGeminiClient)
    monkeypatch.setattr("app.application.predictions.PlayerModelingEngine", FakePlayerModelingEngine)
    monkeypatch.setattr("app.application.predictions.GrowthDecisionEngine", FakeDecisionEngine)

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "completed"
    assert run_prediction.json()["progress"]["details"]["execution_label"] == "Local Model"
    assert run_prediction.json()["progress"]["details"]["prediction_mode"] == "local"

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    payload = results.json()
    assert payload["total"] == 1
    assert payload["items"][0]["churn_reason"] == "Local model estimate"
    assert payload["items"][0]["suggested_action"] == "Local retention action"
    assert captured["modeling_gemini_client_present"] is False
    assert captured["decision_gemini_client_present"] is False
    assert captured["estimate_called"] is True
    assert captured["decision_called"] is True
    assert captured["job_id"] == import_job["id"]


def test_prediction_ai_mode_uses_saved_google_connector(client, monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_GEMINI_MODEL", raising=False)

    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    gemini_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Google Gemini 1",
            "type": "google",
            "config": {
                "api_key": "google-api-key-from-connector",
                "model_name": "gemini-2.5-flash",
            },
        },
    )
    assert gemini_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()
    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200

    captured = {}

    class FakeGeminiClient:
        def __init__(self, api_key=None, model_name=None, stop_checker=None, circuit_namespace=None):
            captured["api_key"] = api_key
            captured["model_name"] = model_name
            captured["stop_checker_present"] = stop_checker is not None
            captured["circuit_namespace"] = circuit_namespace
            captured["reset_called"] = False

        def reset_circuit_breaker(self):
            captured["reset_called"] = True

        def get_ai_response(self, prompt: str):
            if "Provide JSON with keys" in prompt:
                return '{"churn_risk":"high","reason":"Gemini connector used","top_signals":[{"signal":"recent_drop","value":3}]}'
            return '{"decision":"ACT","channel":"push_notification","content":"Gemini save offer"}'

    class FakePlayerModelingEngine:
        def __init__(self, gemini_client, bigquery_service, churn_inactive_days=14, job_id=None):
            captured["gemini_client_present"] = gemini_client is not None
            captured["job_id"] = job_id

        def get_all_player_ids(self):
            return ["player-1"]

        def build_player_profile(self, player_id):
            return {
                "player_id": player_id,
                "email": "player-1@example.com",
                "first_seen_date": "2026-03-01T00:00:00",
                "last_seen_date": "2026-03-06T00:00:00",
                "total_sessions": 4,
                "total_events": 12,
                "total_revenue": 9.99,
                "days_since_last_seen": 1,
                "churn_state": "active",
                "churn_inactive_days": 14,
            }

        async def estimate_churn_risk(self, player_id, player_profile=None):
            captured["estimate_called"] = True
            return {
                "player_id": player_id,
                "churn_state": "active",
                "churn_risk": "high",
                "reason": "Gemini connector used",
                "top_signals": [{"signal": "recent_drop", "value": 3}],
            }

    monkeypatch.setattr("app.application.predictions.GeminiClient", FakeGeminiClient)
    monkeypatch.setattr("app.application.predictions.PlayerModelingEngine", FakePlayerModelingEngine)

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "ai",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "completed"
    assert run_prediction.json()["progress"]["details"]["execution_label"] == "AI"
    assert run_prediction.json()["progress"]["details"]["prediction_mode"] == "ai"

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    payload = results.json()
    assert payload["total"] == 1
    assert payload["items"][0]["churn_reason"] == "Gemini connector used"
    assert payload["items"][0]["suggested_action"] == "Gemini save offer"
    assert captured["api_key"] == "google-api-key-from-connector"
    assert captured["model_name"] == "gemini-2.5-flash"
    assert captured["stop_checker_present"] is True
    assert captured["circuit_namespace"] == "predictions"
    assert captured["reset_called"] is True
    assert captured["gemini_client_present"] is True
    assert captured["estimate_called"] is True
    assert captured["job_id"] == import_job["id"]


def test_online_prediction_times_out_and_marks_job_failed(client, monkeypatch):
    settings = replace(
        get_settings(),
        prediction_network_timeout_seconds=0.2,
        prediction_stop_poll_interval_seconds=0.05,
    )
    client.app.dependency_overrides[get_settings_dependency] = lambda: settings

    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Google AI",
            "type": "google",
            "config": {"api_key": "google-api-key-from-connector", "model_name": "gemini-2.5-flash"},
        },
    )
    assert connector_resp.status_code == 201

    source_connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert source_connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()
    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200

    class SlowGeminiClient:
        def __init__(self, api_key=None, model_name=None, stop_checker=None, circuit_namespace=None):
            self.stop_checker = stop_checker

        def reset_circuit_breaker(self):
            return None

        def get_ai_response(self, prompt):
            time.sleep(2.0)
            return '{"churn_risk":"high","reason":"late","top_signals":[]}'

    class FakePlayerModelingEngine:
        def __init__(self, gemini_client, bigquery_service, churn_inactive_days=14, job_id=None):
            self.gemini_client = gemini_client

        def get_all_player_ids(self):
            return ["player-1"]

        def build_player_profile(self, player_id):
            return {
                "player_id": player_id,
                "email": f"{player_id}@example.com",
                "first_seen_date": "2026-03-01T00:00:00",
                "last_seen_date": "2026-03-06T00:00:00",
                "total_sessions": 4,
                "total_events": 12,
                "total_revenue": 9.99,
                "days_since_last_seen": 1,
                "churn_state": "active",
                "churn_inactive_days": 14,
            }

        async def estimate_churn_risk(self, player_id, player_profile=None):
            self.gemini_client.get_ai_response("slow")
            return {
                "player_id": player_id,
                "churn_state": "active",
                "churn_risk": "high",
                "reason": "slow network response",
                "top_signals": [{"signal": "sessions", "value": 4}],
            }

    class FakeDecisionEngine:
        def __init__(self, gemini_client):
            self.gemini_client = gemini_client

        def decide_next_action(self, player_profile, churn_estimate, objective):
            return {"content": f"message for {player_profile['player_id']}"}

    monkeypatch.setattr("app.application.predictions.GeminiClient", SlowGeminiClient)
    monkeypatch.setattr("app.application.predictions.PlayerModelingEngine", FakePlayerModelingEngine)
    monkeypatch.setattr("app.application.predictions.GrowthDecisionEngine", FakeDecisionEngine)

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "ai",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    started_at = time.monotonic()
    run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    elapsed = time.monotonic() - started_at

    assert elapsed < 1.5
    assert run_prediction.status_code == 500
    assert "timed out" in run_prediction.json()["detail"]
    assert run_prediction.json()["job"]["status"] == "failed"

    failed_job = client.get(prediction_job["links"]["self"])
    assert failed_job.status_code == 200
    assert failed_job.json()["status"] == "failed"
    assert "timed out" in failed_job.json()["error"]


def test_prediction_streams_partial_rows_and_can_be_stopped(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()
    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200

    first_row_written = threading.Event()
    release_remaining_players = threading.Event()
    call_count = {"value": 0}

    class FakePlayerModelingEngine:
        def __init__(self, gemini_client, bigquery_service, churn_inactive_days=14, job_id=None):
            self.job_id = job_id

        def get_all_player_ids(self):
            return ["player-1", "player-2", "player-3"]

        def build_player_profile(self, player_id):
            return {
                "player_id": player_id,
                "email": f"{player_id}@example.com",
                "first_seen_date": "2026-03-01T00:00:00",
                "last_seen_date": "2026-03-06T00:00:00",
                "total_sessions": 4,
                "total_events": 12,
                "total_revenue": 9.99,
                "days_since_last_seen": 1,
                "churn_state": "active",
                "churn_inactive_days": 14,
            }

        async def estimate_churn_risk(self, player_id, player_profile=None):
            call_count["value"] += 1
            if call_count["value"] >= 2:
                release_remaining_players.wait(timeout=5)
            return {
                "player_id": player_id,
                "churn_state": "active",
                "churn_risk": "medium",
                "reason": f"scored {player_id}",
                "top_signals": [{"signal": "sessions", "value": 4}],
            }

    class FakeDecisionEngine:
        def __init__(self, gemini_client):
            self.gemini_client = gemini_client

        def decide_next_action(self, player_profile, churn_estimate, objective):
            return {"content": f"message for {player_profile['player_id']}"}

    original_append = BigQueryService.append_prediction_results

    def tracking_append(self, job_id, rows):
        result = original_append(self, job_id, rows)
        if rows:
            first_row_written.set()
        return result

    monkeypatch.setattr("app.application.predictions.PlayerModelingEngine", FakePlayerModelingEngine)
    monkeypatch.setattr("app.application.predictions.GrowthDecisionEngine", FakeDecisionEngine)
    monkeypatch.setattr("bigquery_service.BigQueryService.append_prediction_results", tracking_append)

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    run_result = {}

    def run_prediction_request():
        with TestClient(client.app) as runner_client:
            run_result["response"] = runner_client.post(prediction_job["links"]["self"] + "/run")

    thread = threading.Thread(target=run_prediction_request)
    thread.start()
    assert first_row_written.wait(timeout=5)

    partial_results = client.get(prediction_job["links"]["results"])
    assert partial_results.status_code == 200
    partial_payload = partial_results.json()
    assert partial_payload["total"] == 1
    assert partial_payload["items"][0]["user_id"] == "player-1"

    running_state = client.get(prediction_job["links"]["self"])
    assert running_state.status_code == 200
    assert running_state.json()["status"] in {"running", "stopping"}
    assert running_state.json()["progress"]["current"] >= 1

    stop_prediction = client.post(prediction_job["links"]["self"] + "/stop")
    assert stop_prediction.status_code == 200
    assert stop_prediction.json()["status"] in {"stopping", "stopped"}

    thread.join(timeout=1.5)
    assert not thread.is_alive()
    assert run_result["response"].status_code == 200
    assert run_result["response"].json()["status"] == "stopped"

    release_remaining_players.set()

    stopped_state = client.get(prediction_job["links"]["self"])
    assert stopped_state.status_code == 200
    assert stopped_state.json()["status"] == "stopped"

    final_results = client.get(prediction_job["links"]["results"])
    assert final_results.status_code == 200
    final_payload = final_results.json()
    assert final_payload["total"] == 1
    assert final_payload["items"][0]["suggested_action"] == "message for player-1"


def test_prediction_results_are_returned_newest_first(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()
    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200

    class FakePlayerModelingEngine:
        def __init__(self, gemini_client, bigquery_service, churn_inactive_days=14, job_id=None):
            self.job_id = job_id

        def get_all_player_ids(self):
            return ["player-1", "player-2", "player-3"]

        def build_player_profile(self, player_id):
            return {
                "player_id": player_id,
                "email": f"{player_id}@example.com",
                "first_seen_date": "2026-03-01T00:00:00",
                "last_seen_date": "2026-03-06T00:00:00",
                "total_sessions": 4,
                "total_events": 12,
                "total_revenue": 9.99,
                "days_since_last_seen": 1,
                "churn_state": "active",
                "churn_inactive_days": 14,
            }

        async def estimate_churn_risk(self, player_id, player_profile=None):
            return {
                "player_id": player_id,
                "churn_state": "active",
                "churn_risk": "medium",
                "reason": f"scored {player_id}",
                "top_signals": [{"signal": "sessions", "value": 4}],
            }

    class FakeDecisionEngine:
        def __init__(self, gemini_client):
            self.gemini_client = gemini_client

        def decide_next_action(self, player_profile, churn_estimate, objective):
            return {"content": f"message for {player_profile['player_id']}"}

    monkeypatch.setattr("app.application.predictions.PlayerModelingEngine", FakePlayerModelingEngine)
    monkeypatch.setattr("app.application.predictions.GrowthDecisionEngine", FakeDecisionEngine)

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "completed"

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    payload = results.json()
    assert [item["user_id"] for item in payload["items"]] == ["player-3", "player-2", "player-1"]


def test_prediction_stops_when_shutdown_requested(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()
    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200

    class FakePlayerModelingEngine:
        def __init__(self, gemini_client, bigquery_service, churn_inactive_days=14, job_id=None):
            self.job_id = job_id

        def get_all_player_ids(self):
            return ["player-1", "player-2", "player-3"]

        def build_player_profile(self, player_id):
            return {
                "player_id": player_id,
                "email": f"{player_id}@example.com",
                "first_seen_date": "2026-03-01T00:00:00",
                "last_seen_date": "2026-03-06T00:00:00",
                "total_sessions": 4,
                "total_events": 12,
                "total_revenue": 9.99,
                "days_since_last_seen": 1,
                "churn_state": "active",
                "churn_inactive_days": 14,
            }

        async def estimate_churn_risk(self, player_id, player_profile=None):
            return {
                "player_id": player_id,
                "churn_state": "active",
                "churn_risk": "medium",
                "reason": f"scored {player_id}",
                "top_signals": [{"signal": "sessions", "value": 4}],
            }

    class ShutdownAfterFirstDecisionEngine:
        def __init__(self, gemini_client):
            self.gemini_client = gemini_client
            self.calls = 0

        def decide_next_action(self, player_profile, churn_estimate, objective):
            self.calls += 1
            if self.calls == 1:
                mark_shutdown_requested()
            return {"content": f"message for {player_profile['player_id']}"}

    monkeypatch.setattr("app.application.predictions.PlayerModelingEngine", FakePlayerModelingEngine)
    monkeypatch.setattr("app.application.predictions.GrowthDecisionEngine", ShutdownAfterFirstDecisionEngine)

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    try:
        run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    finally:
        clear_shutdown_requested()

    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "stopped"

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    payload = results.json()
    assert payload["total"] == 0


def test_import_failure_marks_job_failed(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    def fail_fetch_and_stage_events(*args, **kwargs):
        raise RuntimeError("Adjust API rate limit exceeded")

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        fail_fetch_and_stage_events,
    )

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 500
    assert run_import.json()["detail"] == "Adjust API rate limit exceeded"

    import_state = client.get(import_job["links"]["self"])
    assert import_state.status_code == 200
    payload = import_state.json()
    assert payload["status"] == "failed"
    assert payload["error"] == "Adjust API rate limit exceeded"
    assert payload["progress"]["details"]["failure_reason"] == "Adjust API rate limit exceeded"


def test_import_processing_progress_reports_event_counts(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Amplitude 1",
            "type": "amplitude",
            "config": {"api_key": "mock-key", "secret_key": "mock-secret"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Amplitude 1",
            "start_date": "20260201",
            "end_date": "20260206",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    first_processing_update = threading.Event()
    release_processing = threading.Event()
    run_result = {}

    def fake_fetch_and_stage_events(self, start_date, end_date, job_id=None, page_size=None, should_stop=None, progress_callback=None):
        if callable(progress_callback):
            progress_callback(1000, 1, {})
            progress_callback(2000, 2, {})
        return {
            "job_id": job_id,
            "source": self.connector_type,
            "shards_created": 2,
            "events_staged": 2000,
            "last_checkpoint": {"gcs_uri": "gs://mock/raw/part-00002.jsonl", "event_count": 1000},
            "shard_manifests": [
                {
                    "job_id": job_id,
                    "source": self.connector_type,
                    "gcs_uri": "gs://mock/raw/part-00001.jsonl",
                    "event_count": 1000,
                    "schema_version": "v1",
                    "shard_index": 1,
                    "source_config_id": "Amplitude 1",
                },
                {
                    "job_id": job_id,
                    "source": self.connector_type,
                    "gcs_uri": "gs://mock/raw/part-00002.jsonl",
                    "event_count": 1000,
                    "schema_version": "v1",
                    "shard_index": 2,
                    "source_config_id": "Amplitude 1",
                },
            ],
            "stopped": False,
        }

    def fake_process_notifications(self, notifications, progress_callback=None):
        if callable(progress_callback):
            progress_callback(
                1,
                2,
                {
                    "manifests_processed": 1,
                    "raw_normalized_events": 1000,
                    "events_staging_written": 750,
                    "pipeline_dead_letters_written": 250,
                    "flag_counts": {},
                    "warehouse_stats": {},
                },
            )
            first_processing_update.set()
            release_processing.wait(timeout=5)
            progress_callback(
                2,
                2,
                {
                    "manifests_processed": 2,
                    "raw_normalized_events": 2000,
                    "events_staging_written": 1500,
                    "pipeline_dead_letters_written": 500,
                    "flag_counts": {},
                    "warehouse_stats": {},
                },
            )
        return {
            "manifests_processed": 2,
            "raw_normalized_events": 2000,
            "events_staging_written": 1500,
            "pipeline_dead_letters_written": 500,
            "flag_counts": {},
            "warehouse_stats": {},
        }

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        fake_fetch_and_stage_events,
    )
    monkeypatch.setattr(
        "app.application.imports.DataflowNormalizationRunner.process_notifications",
        fake_process_notifications,
    )

    def run_import_request():
        with TestClient(client.app) as runner_client:
            run_result["response"] = runner_client.post(import_job["links"]["self"] + "/run")

    thread = threading.Thread(target=run_import_request)
    thread.start()
    assert first_processing_update.wait(timeout=5)

    import_state = client.get(import_job["links"]["self"])
    assert import_state.status_code == 200
    payload = import_state.json()
    assert payload["status"] == "running"
    assert payload["progress"]["current"] == 1000
    assert payload["progress"]["total"] == 2000
    assert payload["progress"]["details"]["phase"] == "processing"
    assert payload["progress"]["details"]["processed_manifests"] == 1
    assert payload["progress"]["details"]["total_manifests"] == 2

    release_processing.set()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert run_result["response"].status_code == 200
    assert run_result["response"].json()["status"] == "completed"


def test_import_processing_failure_marks_failed_checkpoints(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Amplitude 1",
            "type": "amplitude",
            "config": {"api_key": "mock-key", "secret_key": "mock-secret"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Amplitude 1",
            "start_date": "20260201",
            "end_date": "20260206",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    def fake_fetch_and_stage_events(self, start_date, end_date, job_id=None, page_size=None, should_stop=None, progress_callback=None):
        if callable(progress_callback):
            progress_callback(1000, 1, {})
            progress_callback(2000, 2, {})
        return {
            "job_id": job_id,
            "source": self.connector_type,
            "shards_created": 2,
            "events_staged": 2000,
            "last_checkpoint": {"gcs_uri": "gs://mock/raw/part-00002.jsonl", "event_count": 1000},
            "shard_manifests": [
                {
                    "job_id": job_id,
                    "source": self.connector_type,
                    "gcs_uri": "gs://mock/raw/part-00001.jsonl",
                    "event_count": 1000,
                    "schema_version": "v1",
                    "shard_index": 1,
                    "source_config_id": "Amplitude 1",
                },
                {
                    "job_id": job_id,
                    "source": self.connector_type,
                    "gcs_uri": "gs://mock/raw/part-00002.jsonl",
                    "event_count": 1000,
                    "schema_version": "v1",
                    "shard_index": 2,
                    "source_config_id": "Amplitude 1",
                },
            ],
            "stopped": False,
        }

    def fake_process_notifications(self, notifications, progress_callback=None):
        if callable(progress_callback):
            progress_callback(
                1,
                2,
                {
                    "manifests_processed": 1,
                    "raw_normalized_events": 1000,
                    "events_staging_written": 750,
                    "pipeline_dead_letters_written": 250,
                    "flag_counts": {},
                    "warehouse_stats": {},
                },
            )
        raise RuntimeError("Normalization failed after staging manifests")

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        fake_fetch_and_stage_events,
    )
    monkeypatch.setattr(
        "app.application.imports.DataflowNormalizationRunner.process_notifications",
        fake_process_notifications,
    )

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 500
    assert run_import.json()["detail"] == "Normalization failed after staging manifests"

    import_state = client.get(import_job["links"]["self"])
    assert import_state.status_code == 200
    payload = import_state.json()
    assert payload["status"] == "failed"
    assert payload["error"] == "Normalization failed after staging manifests"
    assert payload["progress"]["details"]["failure_reason"] == "Normalization failed after staging manifests"
    assert payload["progress"]["details"]["failure_stage"] == "processing"
    assert payload["progress"]["details"]["checkpoint_state"]["failed"] == 2

    checkpoints = client.get(import_job["links"]["checkpoints"])
    assert checkpoints.status_code == 200
    assert [item["status"] for item in checkpoints.json()["items"]] == ["failed", "failed"]


def test_run_import_returns_original_error_after_session_flush_failure(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    def poison_session_and_fail(self, job_id: str):
        self.repository.session.add(
            ImportJobModel(
                id=job_id,
                source_name="Adjust Source",
                status="queued",
                spec_json="{}",
                progress_json="{}",
            )
        )
        with pytest.raises(Exception):
            self.repository.session.flush()
        raise RuntimeError("unable to open database file")

    monkeypatch.setattr(ImportService, "run_job", poison_session_and_fail)

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 500
    payload = run_import.json()
    assert payload["detail"] == "unable to open database file"
    assert payload["job"]["id"] == import_job["id"]
    assert payload["job"]["status"] == "queued"


def test_stop_and_delete_queued_import_job(client):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    stop_import = client.post(import_job["links"]["self"] + "/stop")
    assert stop_import.status_code == 200
    assert stop_import.json()["status"] == "stopped"
    assert stop_import.json()["progress"]["details"]["stop_reason"] == "Stopped by user."

    delete_import = client.delete(import_job["links"]["self"])
    assert delete_import.status_code == 204

    get_deleted = client.get(import_job["links"]["self"])
    assert get_deleted.status_code == 404


def test_delete_connector_is_locked_by_active_import(client):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201

    delete_connector = client.delete("/api/v1/connectors/Adjust%20Source")
    assert delete_connector.status_code == 423
    assert "locked by import jobs" in delete_connector.json()["detail"]


def test_prediction_requires_completed_import_and_locks_import_deletion(client):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    locked_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "local",
        },
    )
    assert locked_prediction.status_code == 423
    assert "cannot be used for prediction until completed" in locked_prediction.json()["detail"]

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200
    assert run_import.json()["status"] == "completed"

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201

    delete_import = client.delete(import_job["links"]["self"])
    assert delete_import.status_code == 423
    assert "locked by prediction jobs" in delete_import.json()["detail"]


def test_export_requires_completed_prediction(client):
    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.create_prediction_job(
            {
                "id": "pred_pending",
                "import_job_id": "imp_completed",
                "status": "queued",
                "spec": {"import_job_id": "imp_completed", "prediction_mode": "local"},
                "progress": {"current": 0, "total": 0, "pct": 0.0, "details": {}},
            }
        )
        session.commit()

    create_export = client.post(
        "/api/v1/exports",
        json={
            "prediction_job_id": "pred_pending",
            "provider": "webhook",
            "channel": "email",
            "audience_name": "pending_prediction",
            "webhook_url": "https://example.com/export",
        },
    )
    assert create_export.status_code == 423
    assert "cannot be used for export until completed" in create_export.json()["detail"]


def test_stop_running_import_job_transitions_to_stopped(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    started = threading.Event()
    run_result = {}

    def slow_fetch_and_stage_events(self, start_date, end_date, job_id=None, page_size=None, should_stop=None, progress_callback=None):
        started.set()
        while True:
            if callable(should_stop) and should_stop():
                return {
                    "job_id": job_id,
                    "source": self.connector_type,
                    "shards_created": 0,
                    "events_staged": 0,
                    "last_checkpoint": None,
                    "shard_manifests": [],
                    "stopped": True,
                    "stop_reason": "Stopped by user.",
                }
            time.sleep(0.01)

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        slow_fetch_and_stage_events,
    )

    def run_import_request():
        with TestClient(client.app) as runner_client:
            run_result["response"] = runner_client.post(import_job["links"]["self"] + "/run")

    thread = threading.Thread(target=run_import_request)
    thread.start()
    assert started.wait(timeout=2)

    with TestClient(client.app) as control_client:
        stop_import = control_client.post(import_job["links"]["self"] + "/stop")
    assert stop_import.status_code == 200
    assert stop_import.json()["status"] == "stopped"

    thread.join(timeout=5)
    assert not thread.is_alive()
    assert run_result["response"].status_code == 200
    assert run_result["response"].json()["status"] == "stopped"

    import_state = client.get(import_job["links"]["self"])
    assert import_state.status_code == 200
    payload = import_state.json()
    assert payload["status"] == "stopped"
    assert payload["progress"]["details"]["stop_reason"] == "Stopped by user."


def test_stop_running_import_returns_immediately_even_if_staging_call_is_stuck(client, monkeypatch):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    started = threading.Event()
    release_worker = threading.Event()
    run_result = {}

    def stuck_fetch_and_stage_events(self, start_date, end_date, job_id=None, page_size=None, should_stop=None, progress_callback=None):
        started.set()
        release_worker.wait(timeout=5)
        return {
            "job_id": job_id,
            "source": self.connector_type,
            "shards_created": 0,
            "events_staged": 0,
            "last_checkpoint": None,
            "shard_manifests": [],
            "stopped": False,
        }

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        stuck_fetch_and_stage_events,
    )

    def run_import_request():
        with TestClient(client.app) as runner_client:
            run_result["response"] = runner_client.post(import_job["links"]["self"] + "/run")

    thread = threading.Thread(target=run_import_request)
    thread.start()
    assert started.wait(timeout=2)

    with TestClient(client.app) as control_client:
        stop_import = control_client.post(import_job["links"]["self"] + "/stop")
    assert stop_import.status_code == 200
    assert stop_import.json()["status"] == "stopped"

    thread.join(timeout=2)
    assert not thread.is_alive()
    assert run_result["response"].status_code == 200
    assert run_result["response"].json()["status"] == "stopped"

    release_worker.set()


def test_restart_discards_stopping_import_job(client):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()

    session = db_module.get_session_factory()()
    try:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.update_import_job(
            import_job["id"],
            {
                "status": "stopping",
                "progress": {
                    "current": 125,
                    "total": 200,
                    "pct": 62.5,
                    "details": {
                        "source": "Adjust Source",
                        "stop_requested": True,
                    },
                },
            },
        )
        session.commit()
    finally:
        session.close()

    restarted_app = create_app()
    with TestClient(restarted_app) as restarted_client:
        import_state = restarted_client.get(import_job["links"]["self"])
        assert import_state.status_code == 404


def test_restart_discards_running_import_and_keeps_completed_import(client):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Amplitude 1",
            "type": "amplitude",
            "config": {"api_key": "mock-key", "secret_key": "mock-secret"},
        },
    )
    assert connector_resp.status_code == 201

    create_running_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Amplitude 1",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_running_import.status_code == 201
    running_job = create_running_import.json()

    create_completed_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Amplitude 1",
            "start_date": "20260303",
            "end_date": "20260304",
        },
    )
    assert create_completed_import.status_code == 201
    completed_job = create_completed_import.json()

    gcs_service = GcsService()
    shard_payloads = [
        [
            {
                "source": "amplitude",
                "player_id": "player-1",
                "event_name": "session_start",
                "timestamp": "2026-03-05T00:00:00",
                "source_event_id": "evt-1",
            },
            {
                "source": "amplitude",
                "player_id": "player-2",
                "event_name": "session_start",
                "timestamp": "2026-03-05T00:05:00",
                "source_event_id": "evt-2",
            },
        ],
        [
            {
                "source": "amplitude",
                "player_id": "player-1",
                "event_name": "purchase",
                "timestamp": "2026-03-05T00:10:00",
                "source_event_id": "evt-3",
                "event_properties": {"revenue": "4.99"},
            },
            {
                "source": "amplitude",
                "player_id": "player-3",
                "event_name": "session_start",
                "timestamp": "2026-03-05T00:15:00",
                "source_event_id": "evt-4",
            },
        ],
    ]

    session = db_module.get_session_factory()()
    try:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.update_import_job(
            running_job["id"],
            {
                "status": "running",
                "progress": {
                    "current": 4,
                    "total": 4,
                    "pct": 100.0,
                    "details": {
                        "source": "Amplitude 1",
                        "connector_type": "amplitude",
                        "events_staged": 4,
                        "shards_created": 2,
                    },
                },
            },
        )
        repository.update_import_job(
            completed_job["id"],
            {
                "status": "completed",
                "progress": {
                    "current": 2,
                    "total": 2,
                    "pct": 100.0,
                    "details": {
                        "source": "Amplitude 1",
                        "connector_type": "amplitude",
                        "events_staged": 2,
                        "shards_created": 1,
                    },
                },
            },
        )

        for index, shard_events in enumerate(shard_payloads, start=1):
            blob_name = f"raw/source=amplitude/job={running_job['id']}/part-{index:05d}.jsonl"
            gcs_uri = gcs_service.upload_raw_events(shard_events, blob_name)
            manifest = {
                "job_id": running_job["id"],
                "source": "amplitude",
                "gcs_uri": gcs_uri,
                "event_count": len(shard_events),
                "start_date": "20260301",
                "end_date": "20260302",
                "shard_index": index,
                "source_config_id": "Amplitude 1",
                "schema_version": "v1",
            }
            repository.upsert_checkpoint(
                {
                    "job_id": running_job["id"],
                    "shard_index": index,
                    "source_name": "Amplitude 1",
                    "status": "published",
                    "cursor": str(index),
                    "gcs_uri": gcs_uri,
                    "message_id": f"mock-{index}",
                    "manifest": manifest,
                }
            )
        session.commit()
    finally:
        session.close()

    restarted_app = create_app()
    with TestClient(restarted_app) as restarted_client:
        import_state = restarted_client.get(running_job["links"]["self"])
        assert import_state.status_code == 404

        completed_state = restarted_client.get(completed_job["links"]["self"])
        assert completed_state.status_code == 200
        payload = completed_state.json()
        assert payload["status"] == "completed"
        assert payload["progress"]["current"] == 2


def test_startup_retention_cleanup_removes_expired_import_and_prediction_cache(client):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    create_import = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Adjust Source",
            "start_date": "20260301",
            "end_date": "20260302",
        },
    )
    assert create_import.status_code == 201
    import_job = create_import.json()
    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": import_job["id"],
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    bigquery_service = BigQueryService()
    bigquery_service.write_events_staging(
        [
            {
                "job_id": import_job["id"],
                "job_identifier": import_job["id"],
                "source": "adjust",
                "player_id": "player-1",
                "canonical_user_id": "uid:player-1",
                "event_type": "session_start",
                "event_time": "2026-03-01T00:00:00",
                "event_date": "2026-03-01",
                "source_config_id": "Adjust Source",
                "raw_gcs_uri": "gs://mock/raw/part-00001.jsonl",
                "shard_index": 1,
                "schema_version": "v1",
                "event_fingerprint": "fp-1",
                "data_quality_flags": [],
            }
        ],
        job_id=import_job["id"],
    )
    bigquery_service.append_prediction_results(
        job_id=prediction_job["id"],
        rows=[
            {
                "prediction_job_id": prediction_job["id"],
                "import_job_id": import_job["id"],
                "completed_at": "2026-02-01T00:00:00",
                "user_id": "player-1",
                "predicted_churn_risk": "high",
                "churn_reason": "expired",
                "prediction_source": "local",
                "suggested_action": "cleanup me",
            }
        ],
    )

    expired_timestamp = datetime.utcnow() - timedelta(days=8)
    session = db_module.get_session_factory()()
    try:
        import_row = session.get(ImportJobModel, import_job["id"])
        prediction_row = session.get(PredictionJobModel, prediction_job["id"])
        assert import_row is not None
        assert prediction_row is not None
        import_row.status = "completed"
        import_row.updated_at = expired_timestamp
        prediction_row.status = "completed"
        prediction_row.updated_at = expired_timestamp
        session.commit()
    finally:
        session.close()

    restarted_app = create_app()
    with TestClient(restarted_app) as restarted_client:
        expired_import = restarted_client.get(import_job["links"]["self"])
        assert expired_import.status_code == 404

        expired_prediction = restarted_client.get(prediction_job["links"]["self"])
        assert expired_prediction.status_code == 404

    cleaned_service = BigQueryService()
    prediction_results = cleaned_service.list_prediction_results(prediction_job["id"])
    assert prediction_results["total"] == 0
    if not cleaned_service._table.empty:
        assert import_job["id"] not in set(cleaned_service._table.get("job_id", []))
