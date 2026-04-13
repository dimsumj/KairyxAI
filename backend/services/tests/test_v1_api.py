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

from app.application.churn_models import LocalChurnModelService
from app.application.imports import ImportService
from app.core import db as db_module
from app.core.deps import get_settings_dependency
from app.core.runtime import clear_shutdown_requested, mark_shutdown_requested
from app.core.settings import get_settings
from app.infrastructure.db_models import ControlPlaneResourceModel, ImportJobModel, PredictionJobModel
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from app.main import create_app
from bigquery_service import BigQueryService, get_shared_bigquery_service
from connectors.adjust_connector import AdjustConnector
from connectors.appsflyer_connector import AppsFlyerConnector
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
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    db_module.clear_runtime_database_fallback()


def _create_completed_import_job(
    job_id: str,
    *,
    source_name: str = "Manual Source",
    start_date: str = "20260301",
    end_date: str = "20260301",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> dict:
    with db_module.session_scope() as session:
        repo = SqlAlchemyControlPlaneRepository(session)
        job = repo.create_import_job(
            {
                "id": job_id,
                "source_name": source_name,
                "status": "completed",
                "spec": {
                    "source_name": source_name,
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "progress": {},
            }
        )
        row = session.get(ImportJobModel, job_id)
        if row is None:
            raise AssertionError(f"import job {job_id} was not created")
        if created_at is not None:
            row.created_at = created_at
        if updated_at is not None:
            row.updated_at = updated_at
        session.flush()
        return repo.get_import_job(job_id)


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
    assert "Player Engagement Platform" in resp.text
    assert 'id="sidebar-nav"' in resp.text
    assert '<main class="content">' in resp.text


def test_org_root_serves_frontend_shell(client):
    resp = client.get("/northstar")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "/static/operator-console.css" in resp.text
    assert "/static/operator-console.js" in resp.text
    assert "Player Engagement Platform" in resp.text
    assert 'id="sidebar-nav"' in resp.text
    assert '<main class="content">' in resp.text
    assert "Browse Tables" in resp.text
    assert "Import Type" in resp.text
    assert "WHERE Filter (optional)" in resp.text


def test_root_serves_frontend_static_assets(client):
    css_resp = client.get("/static/operator-console.css")
    assert css_resp.status_code == 200
    assert "text/css" in css_resp.headers["content-type"]
    assert "--bg-color" in css_resp.text

    js_resp = client.get("/static/operator-console.js")
    assert js_resp.status_code == 200
    assert "javascript" in js_resp.headers["content-type"]
    assert "initializeOperatorConsole" in js_resp.text
    assert "workspace-org-url-input" in js_resp.text
    assert "syncBrowserOrganizationPath" in js_resp.text
    assert "/api/v1" in js_resp.text
    assert "Connect Data Source" in js_resp.text
    assert "bigquery_credentials_entry_mode" in js_resp.text
    assert "Service Account JSON File" in js_resp.text
    assert "Import BigQuery Table" in js_resp.text
    assert "BigQuery imports read one table at a time." in js_resp.text
    assert "Column mappings must use simple BigQuery identifiers." in js_resp.text
    assert "Connected to source" in js_resp.text
    assert "Show import status details" in js_resp.text
    assert "<code>*_ref</code> values through the API." in js_resp.text
    assert "`*_ref` values through the API." not in js_resp.text

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

    org_resp = client.get("/default/v1/health/live")
    assert org_resp.status_code == 200
    org_payload = org_resp.json()
    assert org_payload["status"] == "ok"
    assert org_payload["mode"] == "mock"


def test_org_scoped_v1_import_links_use_org_prefix(client):
    connector_resp = client.post(
        "/studio-a/v1/connectors",
        headers={"x-actor-role": "admin", "x-project-id": "default"},
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    import_resp = client.post(
        "/studio-a/v1/imports",
        headers={"x-actor-role": "admin", "x-project-id": "default"},
        json={
            "source_name": "Adjust Source",
            "start_date": "2026-01-01",
            "end_date": "2026-01-02",
        },
    )
    assert import_resp.status_code == 201
    payload = import_resp.json()
    assert payload["tenant_id"] == "studio-a"
    assert payload["links"]["self"].startswith("/studio-a/v1/imports/")
    assert payload["links"]["checkpoints"].startswith("/studio-a/v1/imports/")


def test_health_reports_local_cache_stats(client):
    health = client.get("/api/v1/health")
    assert health.status_code == 200
    payload = health.json()
    assert payload["mode"] == "mock"
    assert payload["mock_state_backend"] == "local_files"
    assert payload["mock_state_persistent"] is False
    assert payload["local_cache"]["retention_days"] == 7
    assert payload["local_cache"]["tables"]["events_staging"]["rows"] >= 0
    assert payload["local_cache"]["tables"]["prediction_results"]["rows"] >= 0


def test_health_reports_database_mock_storage(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_MOCK_STORAGE_BACKEND", "database")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as test_client:
        health = test_client.get("/api/v1/health")

    assert health.status_code == 200
    payload = health.json()
    assert payload["mode"] == "mock"
    assert payload["mock_state_backend"] == "database"
    assert payload["mock_state_persistent"] is True
    assert payload["control_plane_database_backend"] == "sqlite"
    assert payload["control_plane_database_persistent"] is True
    assert payload["control_plane_database_fallback_active"] is False
    assert payload["local_cache"]["storage_backend"] == "database"
    assert payload["local_cache"]["persistent"] is True


def test_upsert_resource_recovers_from_concurrent_insert(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    db_module.init_db()

    session = db_module.get_session_factory()()
    repository = SqlAlchemyControlPlaneRepository(session)
    original_flush = session.flush
    resource_id = "data_core:canonical_coverage_low"
    injected_race = False

    def flush_with_race(*args, **kwargs):
        nonlocal injected_race
        if not injected_race:
            injected_race = True
            with db_module.session_scope() as competing_session:
                competing_repository = SqlAlchemyControlPlaneRepository(competing_session)
                competing_repository.upsert_resource(
                    "health_alert",
                    resource_id,
                    status="open",
                    name="canonical_coverage_low",
                    payload={"alert_id": resource_id, "code": "canonical_coverage_low", "status": "open"},
                )
        return original_flush(*args, **kwargs)

    monkeypatch.setattr(session, "flush", flush_with_race)

    try:
        saved = repository.upsert_resource(
            "health_alert",
            resource_id,
            status="open",
            name="canonical_coverage_low",
            payload={"alert_id": resource_id, "code": "canonical_coverage_low", "status": "open", "message": "latest"},
        )
        session.commit()
    finally:
        session.close()

    assert saved["resource_id"] == resource_id
    with db_module.session_scope() as verification_session:
        rows = verification_session.query(ControlPlaneResourceModel).filter_by(resource_type="health_alert", resource_id=resource_id).all()
    assert len(rows) == 1


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


def test_prediction_model_training_can_start_stop_and_report_progress(client, monkeypatch):
    def fake_train_model(self, *, reference_time=None, min_rows=12, should_stop=None, persist_initial_status=True):
        for index in range(1, 6):
            training_status = self.get_training_status() or {}
            training_status.update(
                {
                    "status": "running",
                    "stage": "building_dataset",
                    "reference_time": reference_time,
                    "started_at": reference_time,
                    "min_rows_required": min_rows,
                    "row_count": index * 3,
                    "users_processed": index,
                    "users_total": 5,
                    "exposures_processed": 0,
                    "exposures_total": 0,
                }
            )
            self._persist_training_status(training_status)
            self._commit_session()
            time.sleep(0.03)
            if should_stop and should_stop():
                self.mark_training_stopped(reason="Stopped by user.")
                return {"model_version": "heuristic_v1", "status": "stopped"}
        return {"model_version": "heuristic_v1", "status": "fallback"}

    monkeypatch.setattr(LocalChurnModelService, "train_model", fake_train_model)

    start = client.post(
        "/api/v1/predictions/models/train/start",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-24T09:00:00", "min_rows": 12},
    )
    assert start.status_code == 200
    assert start.json()["started"] is True
    assert start.json()["training_status"]["status"] == "running"

    progress_payload = None
    deadline = time.time() + 2.0
    while time.time() < deadline:
        runs = client.get("/api/v1/predictions/models/runs", headers={"x-actor-role": "analyst"})
        assert runs.status_code == 200
        progress_payload = runs.json()
        if int(progress_payload["training_status"].get("row_count") or 0) > 0:
            break
        time.sleep(0.03)

    assert progress_payload is not None
    assert int(progress_payload["training_status"].get("row_count") or 0) > 0

    stop = client.post("/api/v1/predictions/models/train/stop", headers={"x-actor-role": "operator"})
    assert stop.status_code == 200
    assert stop.json()["training_status"]["status"] in {"stopping", "stopped"}

    stopped_payload = None
    deadline = time.time() + 2.0
    while time.time() < deadline:
        runs = client.get("/api/v1/predictions/models/runs", headers={"x-actor-role": "analyst"})
        assert runs.status_code == 200
        stopped_payload = runs.json()
        if str(stopped_payload["training_status"].get("status") or "").lower() == "stopped":
            break
        time.sleep(0.03)

    assert stopped_payload is not None
    assert stopped_payload["training_status"]["status"] == "stopped"
    assert stopped_payload["training_status"]["stop_reason"] == "Stopped by user."


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


def test_mock_database_storage_persists_between_service_instances(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_MOCK_STORAGE_BACKEND", "database")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    first_service = BigQueryService()
    first_service.write_events_staging(
        [
            {
                "job_id": "import_1",
                "job_identifier": "import_1",
                "source": "adjust",
                "player_id": "player-1",
                "canonical_user_id": "uid:player-1",
                "event_type": "session_start",
                "event_time": "2026-03-12T12:00:00",
                "event_properties": {"source_user_id": "player-1"},
                "user_properties": {"email": "player-1@example.com"},
                "data_quality_flags": [],
            }
        ],
        job_id="import_1",
    )
    first_service.run_events_curation(job_id="import_1")
    first_service.refresh_player_latest_state(job_id="import_1")
    first_service.append_prediction_results(
        "prediction_1",
        [
            {
                "user_id": "player-1",
                "canonical_user_id": "uid:player-1",
                "email": "player-1@example.com",
                "predicted_churn_risk": "medium",
                "completed_at": "2026-03-12T12:05:00",
            }
        ],
    )

    second_service = BigQueryService()
    staged_rows = second_service.get_rows_for_alias("standardized")
    curated_rows = second_service.get_rows_for_alias("fact_events_unified")
    latest_state = second_service.get_player_latest_state("player-1", job_id="import_1")
    prediction_results = second_service.list_prediction_results("prediction_1")

    assert len(staged_rows) == 1
    assert len(curated_rows) == 1
    assert latest_state is not None
    assert latest_state["canonical_user_id"] == "uid:player-1"
    assert prediction_results["total"] == 1
    assert prediction_results["items"][0]["user_id"] == "player-1"


def test_mock_connectors_accept_escaped_newline_backend_mode(monkeypatch):
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock\\n")

    adjust_rows = AdjustConnector({"api_token": "adjust-token"}).fetch_events("20260301", "20260302")
    appsflyer_rows = AppsFlyerConnector({"api_token": "af-token", "app_id": "demo-app"}).fetch_events("20260301", "20260302")

    assert adjust_rows[0]["player_id"] == "adjust_user_1001"
    assert appsflyer_rows[0]["player_id"] == "af_user_2001"


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
        assert "project_id" in connector_columns

        version_row = upgraded_connection.execute("SELECT version_num FROM alembic_version").fetchone()
        assert version_row is not None
        assert version_row[0] == "20260402_0005"
    finally:
        upgraded_connection.close()


def test_startup_resumes_partial_multitenant_sqlite_upgrade(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))

    command.upgrade(_alembic_config(tmp_path), "20260310_0002")
    partial_connection = sqlite3.connect(tmp_path / "control_plane.db")
    try:
        partial_connection.executescript(
            """
            CREATE TABLE tenants_v1 (
                tenant_id VARCHAR(64) NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                status VARCHAR(64) NOT NULL DEFAULT 'active',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX ix_tenants_v1_name ON tenants_v1 (name);
            CREATE INDEX ix_tenants_v1_status ON tenants_v1 (status);
            CREATE TABLE platform_users_v1 (
                user_id VARCHAR(128) NOT NULL PRIMARY KEY,
                email VARCHAR(255),
                display_name VARCHAR(255),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX ix_platform_users_v1_email ON platform_users_v1 (email);
            CREATE TABLE tenant_memberships_v1 (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                tenant_id VARCHAR(64) NOT NULL,
                user_id VARCHAR(128) NOT NULL,
                role VARCHAR(32) NOT NULL,
                status VARCHAR(64) NOT NULL DEFAULT 'active',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_tenant_membership_tenant_user UNIQUE (tenant_id, user_id)
            );
            CREATE INDEX ix_tenant_memberships_v1_tenant_id ON tenant_memberships_v1 (tenant_id);
            CREATE INDEX ix_tenant_memberships_v1_user_id ON tenant_memberships_v1 (user_id);
            CREATE INDEX ix_tenant_memberships_v1_role ON tenant_memberships_v1 (role);
            CREATE INDEX ix_tenant_memberships_v1_status ON tenant_memberships_v1 (status);
            CREATE INDEX ix_field_mappings_v2_connector_name ON field_mappings_v2 (connector_name);
            CREATE INDEX ix_experiment_configs_config_key ON experiment_configs (config_key);
            CREATE INDEX ix_action_history_v2_action_type ON action_history_v2 (action_type);
            CREATE INDEX ix_action_history_v2_resource_type ON action_history_v2 (resource_type);
            CREATE INDEX ix_action_history_v2_resource_id ON action_history_v2 (resource_id);
            INSERT INTO tenants_v1 (tenant_id, name, status, created_at, updated_at)
            VALUES ('default', 'Default Tenant', 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
            """
        )
        partial_connection.commit()
    finally:
        partial_connection.close()

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
        assert {"tenant_id", "connector_id", "created_by", "updated_by", "correlation_id"} <= connector_columns

        version_row = upgraded_connection.execute("SELECT version_num FROM alembic_version").fetchone()
        assert version_row is not None
        assert version_row[0] == "20260402_0005"

        bootstrap_tenants = upgraded_connection.execute(
            "SELECT COUNT(*) FROM tenants_v1 WHERE tenant_id = 'default'"
        ).fetchone()
        assert bootstrap_tenants is not None
        assert bootstrap_tenants[0] == 1
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
    monkeypatch.setattr(BigQueryService, "get_import_roster_player_ids", lambda self, job_id: ["player-1"])

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
    assert captured["job_id"] is None


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
    monkeypatch.setattr(BigQueryService, "get_import_roster_player_ids", lambda self, job_id: ["player-1"])

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
    assert captured["job_id"] is None


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
    monkeypatch.setattr(BigQueryService, "get_import_roster_player_ids", lambda self, job_id: ["player-1"])

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
    monkeypatch.setattr(BigQueryService, "get_import_roster_player_ids", lambda self, job_id: ["player-1", "player-2", "player-3"])

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
    monkeypatch.setattr(BigQueryService, "get_import_roster_player_ids", lambda self, job_id: ["player-1", "player-2", "player-3"])

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
    monkeypatch.setattr(BigQueryService, "get_import_roster_player_ids", lambda self, job_id: ["player-1", "player-2", "player-3"])

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


def test_prediction_uses_merged_history_for_selected_import_roster(client):
    now = datetime.utcnow().replace(microsecond=0)
    old_updated_at = now - timedelta(days=40)
    new_updated_at = now - timedelta(days=1)
    old_import = _create_completed_import_job(
        "imp_old_history",
        source_name="Manual Old Import",
        start_date=(now - timedelta(days=60)).strftime("%Y%m%d"),
        end_date=(now - timedelta(days=30)).strftime("%Y%m%d"),
        created_at=old_updated_at,
        updated_at=old_updated_at,
    )
    _create_completed_import_job(
        "imp_new_history",
        source_name="Manual New Import",
        start_date=(now - timedelta(days=3)).strftime("%Y%m%d"),
        end_date=(now - timedelta(days=1)).strftime("%Y%m%d"),
        created_at=new_updated_at,
        updated_at=new_updated_at,
    )

    service = get_shared_bigquery_service()
    service.write_events_staging(
        [
            {
                "job_id": old_import["id"],
                "job_identifier": old_import["id"],
                "source": "manual",
                "player_id": "player-1",
                "canonical_user_id": "canon-1",
                "source_event_id": "old-evt-1",
                "event_fingerprint": "old-fp-1",
                "event_type": "session_start",
                "event_time": (now - timedelta(days=42)).isoformat(),
                "event_properties": {},
                "user_properties": {"email": "player-1@example.com"},
            },
            {
                "job_id": old_import["id"],
                "job_identifier": old_import["id"],
                "source": "manual",
                "player_id": "player-2",
                "canonical_user_id": "canon-2",
                "source_event_id": "old-evt-2",
                "event_fingerprint": "old-fp-2",
                "event_type": "session_start",
                "event_time": (now - timedelta(days=41)).isoformat(),
                "event_properties": {},
                "user_properties": {"email": "player-2@example.com"},
            },
            {
                "job_id": "imp_new_history",
                "job_identifier": "imp_new_history",
                "source": "manual",
                "player_id": "player-1",
                "canonical_user_id": "canon-1",
                "source_event_id": "new-evt-1",
                "event_fingerprint": "new-fp-1",
                "event_type": "session_start",
                "event_time": (now - timedelta(days=1)).isoformat(),
                "event_properties": {"campaign": "revive"},
                "user_properties": {"email": "player-1@example.com"},
            },
        ]
    )
    service.run_events_curation()
    service.refresh_player_latest_state()

    assert service.get_import_roster_player_ids(old_import["id"]) == ["player-1", "player-2"]

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "import_job_id": old_import["id"],
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "completed"
    assert run_prediction.json()["progress"]["details"]["history_scope"] == "tenant_merged"
    assert run_prediction.json()["progress"]["details"]["history_snapshot_at"]
    assert run_prediction.json()["progress"]["details"]["stale"] is False

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    items = {item["user_id"]: item for item in results.json()["items"]}
    assert set(items) == {"player-1", "player-2"}
    assert items["player-1"]["churn_state"] == "active"
    assert int(items["player-1"]["days_since_last_seen"]) < 14
    assert items["player-2"]["churn_state"] == "churned"
    assert int(items["player-2"]["days_since_last_seen"]) >= 14


def test_completed_prediction_job_is_marked_stale_after_later_import(client):
    import_job = _create_completed_import_job(
        "imp_stale_anchor",
        source_name="Manual Anchor Import",
        start_date="20260301",
        end_date="20260301",
    )

    service = get_shared_bigquery_service()
    service.write_events_staging(
        [
            {
                "job_id": import_job["id"],
                "job_identifier": import_job["id"],
                "source": "manual",
                "player_id": "player-stale",
                "canonical_user_id": "canon-stale",
                "source_event_id": "anchor-evt-1",
                "event_fingerprint": "anchor-fp-1",
                "event_type": "session_start",
                "event_time": "2026-03-10T00:00:00",
                "event_properties": {},
                "user_properties": {"email": "stale@example.com"},
            }
        ]
    )
    service.run_events_curation()
    service.refresh_player_latest_state()

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
    snapshot_at = run_prediction.json()["progress"]["details"]["history_snapshot_at"]
    assert snapshot_at

    later_import = _create_completed_import_job(
        "imp_stale_newer",
        source_name="Manual Later Import",
        start_date="20260324",
        end_date="20260324",
    )
    service.write_events_staging(
        [
            {
                "job_id": later_import["id"],
                "job_identifier": later_import["id"],
                "source": "manual",
                "player_id": "player-fresh",
                "canonical_user_id": "canon-fresh",
                "source_event_id": "later-evt-1",
                "event_fingerprint": "later-fp-1",
                "event_type": "session_start",
                "event_time": "2026-03-24T00:00:00",
                "event_properties": {},
                "user_properties": {},
            }
        ]
    )
    service.run_events_curation()
    service.refresh_player_latest_state()

    stale_job = client.get(prediction_job["links"]["self"])
    assert stale_job.status_code == 200
    assert stale_job.json()["progress"]["details"]["history_scope"] == "tenant_merged"
    assert stale_job.json()["progress"]["details"]["history_snapshot_at"] == snapshot_at
    assert stale_job.json()["progress"]["details"]["stale"] is True
    assert later_import["id"] in stale_job.json()["progress"]["details"]["stale_reason"]

    listed_jobs = client.get("/api/v1/predictions")
    assert listed_jobs.status_code == 200
    listed_item = next(item for item in listed_jobs.json()["items"] if item["id"] == prediction_job["id"])
    assert listed_item["progress"]["details"]["stale"] is True


def test_prediction_source_mode_resolves_latest_import_on_run(client):
    source_name = "Amplitude 1"
    current_import_time = datetime(2026, 3, 20, 12, 0, 0)
    latest_import_time = datetime(2026, 3, 24, 12, 0, 0)
    current_import = _create_completed_import_job(
        "imp_source_current",
        source_name=source_name,
        start_date="20260320",
        end_date="20260320",
        created_at=current_import_time,
        updated_at=current_import_time,
    )

    service = get_shared_bigquery_service()
    service.write_events_staging(
        [
            {
                "job_id": current_import["id"],
                "job_identifier": current_import["id"],
                "source": "manual",
                "player_id": "player-current",
                "canonical_user_id": "canon-current",
                "source_event_id": "current-evt-1",
                "event_fingerprint": "current-fp-1",
                "event_type": "session_start",
                "event_time": "2026-03-20T00:00:00",
                "event_properties": {},
                "user_properties": {"email": "current@example.com"},
            }
        ]
    )
    service.run_events_curation()
    service.refresh_player_latest_state()

    create_prediction = client.post(
        "/api/v1/predictions",
        json={
            "source_name": source_name,
            "audience_scope": "source",
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()
    assert prediction_job["spec"]["audience_scope"] == "source"
    assert prediction_job["spec"]["source_name"] == source_name
    assert prediction_job["spec"]["import_job_id"] == current_import["id"]

    latest_import = _create_completed_import_job(
        "imp_source_latest",
        source_name=source_name,
        start_date="20260324",
        end_date="20260324",
        created_at=latest_import_time,
        updated_at=latest_import_time,
    )
    service.write_events_staging(
        [
            {
                "job_id": latest_import["id"],
                "job_identifier": latest_import["id"],
                "source": "manual",
                "player_id": "player-latest",
                "canonical_user_id": "canon-latest",
                "source_event_id": "latest-evt-1",
                "event_fingerprint": "latest-fp-1",
                "event_type": "session_start",
                "event_time": "2026-03-24T00:00:00",
                "event_properties": {},
                "user_properties": {"email": "latest@example.com"},
            }
        ]
    )
    service.run_events_curation()
    service.refresh_player_latest_state()

    run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "completed"
    assert run_prediction.json()["spec"]["import_job_id"] == latest_import["id"]
    assert run_prediction.json()["progress"]["details"]["audience_scope"] == "source"
    assert run_prediction.json()["progress"]["details"]["source_name"] == source_name
    assert run_prediction.json()["progress"]["details"]["import_job_id"] == latest_import["id"]

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    payload = results.json()
    assert payload["total"] == 1
    assert payload["items"][0]["user_id"] == "player-latest"


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

    def fake_fetch_and_stage_events(
        self,
        start_date,
        end_date,
        job_id=None,
        page_size=None,
        should_stop=None,
        progress_callback=None,
        page_fetch_wrapper=None,
    ):
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


def test_import_staging_progress_resets_timeout_budget(client, monkeypatch):
    settings = replace(
        get_settings(),
        import_network_timeout_seconds=0.2,
        import_stop_poll_interval_seconds=0.05,
    )
    client.app.dependency_overrides[get_settings_dependency] = lambda: settings

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

    def slow_but_progressing_fetch(
        self,
        start_date,
        end_date,
        job_id=None,
        page_size=None,
        should_stop=None,
        progress_callback=None,
        page_fetch_wrapper=None,
    ):
        for index in range(3):
            time.sleep(0.12)
            if callable(progress_callback):
                progress_callback((index + 1) * 1000, index + 1, {})
        return {
            "job_id": job_id,
            "source": self.connector_type,
            "shards_created": 0,
            "events_staged": 3000,
            "last_checkpoint": None,
            "shard_manifests": [],
            "stopped": False,
        }

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        slow_but_progressing_fetch,
    )

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200
    assert run_import.json()["status"] == "completed"
    assert run_import.json()["progress"]["current"] == 3000
    assert run_import.json()["progress"]["details"]["events_staged"] == 3000


def test_import_staging_startup_grace_allows_first_progress_heartbeat(client, monkeypatch):
    settings = replace(
        get_settings(),
        import_network_timeout_seconds=0.2,
        import_stop_poll_interval_seconds=0.05,
    )
    client.app.dependency_overrides[get_settings_dependency] = lambda: settings

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

    def delayed_first_progress_fetch(
        self,
        start_date,
        end_date,
        job_id=None,
        page_size=None,
        should_stop=None,
        progress_callback=None,
        page_fetch_wrapper=None,
    ):
        time.sleep(0.22)
        if callable(progress_callback):
            progress_callback(1000, 1, {})
        time.sleep(0.02)
        return {
            "job_id": job_id,
            "source": self.connector_type,
            "shards_created": 0,
            "events_staged": 1000,
            "last_checkpoint": None,
            "shard_manifests": [],
            "stopped": False,
        }

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        delayed_first_progress_fetch,
    )

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200
    assert run_import.json()["status"] == "completed"
    assert run_import.json()["progress"]["current"] == 1000
    assert run_import.json()["progress"]["details"]["events_staged"] == 1000


def test_import_staging_upload_delay_does_not_consume_network_timeout_budget(client, monkeypatch):
    settings = replace(
        get_settings(),
        data_backend_mode="gcp",
        import_network_timeout_seconds=0.2,
        import_stop_poll_interval_seconds=0.05,
    )
    client.app.dependency_overrides[get_settings_dependency] = lambda: settings

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

    def single_page(self, start_date, end_date, page_size=None):
        yield [
            {
                "player_id": "u_1",
                "event_name": "session_start",
                "timestamp": "2026-02-01T00:00:00",
            }
        ]

    original_upload = GcsService.upload_raw_events

    def slow_upload(self, events, destination_blob_name):
        time.sleep(0.3)
        return original_upload(self, events, destination_blob_name)

    monkeypatch.setattr("ingestion_service.IngestionService._iter_event_pages", single_page)
    monkeypatch.setattr("gcs_service.GcsService.upload_raw_events", slow_upload)

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 200
    assert run_import.json()["status"] == "completed"
    assert run_import.json()["progress"]["current"] == 1
    assert run_import.json()["progress"]["details"]["events_staged"] == 1


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

    def fake_fetch_and_stage_events(
        self,
        start_date,
        end_date,
        job_id=None,
        page_size=None,
        should_stop=None,
        progress_callback=None,
        page_fetch_wrapper=None,
    ):
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


def test_delete_failed_import_skips_warehouse_cleanup_without_processed_rows(client, monkeypatch):
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

    def fake_fetch_and_stage_events(
        self,
        start_date,
        end_date,
        job_id=None,
        page_size=None,
        should_stop=None,
        progress_callback=None,
        page_fetch_wrapper=None,
    ):
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

    def fail_before_processing_writes(self, notifications, progress_callback=None):
        raise RuntimeError("Normalization failed before warehouse writes")

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        fake_fetch_and_stage_events,
    )
    monkeypatch.setattr(
        "app.application.imports.DataflowNormalizationRunner.process_notifications",
        fail_before_processing_writes,
    )

    run_import = client.post(import_job["links"]["self"] + "/run")
    assert run_import.status_code == 500
    assert run_import.json()["detail"] == "Normalization failed before warehouse writes"

    checkpoints = client.get(import_job["links"]["checkpoints"])
    assert checkpoints.status_code == 200
    assert [item["status"] for item in checkpoints.json()["items"]] == ["failed", "failed"]

    def fail_if_called(self, job_identifier):
        raise AssertionError("Warehouse cleanup should be skipped for failed imports without processed rows.")

    monkeypatch.setattr(BigQueryService, "delete_data_for_job", fail_if_called)

    delete_import = client.delete(import_job["links"]["self"])
    assert delete_import.status_code == 204

    get_deleted = client.get(import_job["links"]["self"])
    assert get_deleted.status_code == 404


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


def test_run_import_background_returns_accepted_without_waiting_for_completion(client, monkeypatch):
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
    release = threading.Event()

    original_run_job = ImportService.run_job

    def fake_run_job(self, job_id):
        started.set()
        release.wait(timeout=5)
        return original_run_job(self, job_id)

    monkeypatch.setattr("app.application.imports.ImportService.run_job", fake_run_job)

    run_import = client.post(import_job["links"]["self"] + "/run?background=true")
    assert run_import.status_code == 202
    payload = run_import.json()
    assert payload["accepted"] is True
    assert payload["background"] is True
    assert payload["id"] == import_job["id"]
    assert started.wait(timeout=2)

    release.set()

    completed = None
    for _ in range(50):
        completed = client.get(import_job["links"]["self"])
        assert completed.status_code == 200
        if completed.json()["status"] == "completed":
            break
        time.sleep(0.05)
    assert completed is not None
    assert completed.json()["status"] == "completed"


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

    def slow_fetch_and_stage_events(
        self,
        start_date,
        end_date,
        job_id=None,
        page_size=None,
        should_stop=None,
        progress_callback=None,
        page_fetch_wrapper=None,
    ):
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

    def stuck_fetch_and_stage_events(
        self,
        start_date,
        end_date,
        job_id=None,
        page_size=None,
        should_stop=None,
        progress_callback=None,
        page_fetch_wrapper=None,
    ):
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


def test_delete_stopped_import_cleans_raw_and_warehouse_state(client, monkeypatch):
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
    job_id = import_job["id"]

    gcs_service = GcsService()
    bigquery_service = get_shared_bigquery_service()
    processing_started = threading.Event()
    seeded = {"done": False, "gcs_uri": ""}

    def fake_fetch_and_stage_events(
        self,
        start_date,
        end_date,
        job_id=None,
        page_size=None,
        should_stop=None,
        progress_callback=None,
        page_fetch_wrapper=None,
    ):
        gcs_uri = gcs_service.upload_raw_events(
            [
                {
                    "player_id": "delete-me",
                    "event_name": "session_start",
                    "timestamp": "2026-03-01T08:00:00",
                }
            ],
            f"raw/source={self.connector_type}/job={job_id}/part-00001.jsonl",
        )
        seeded["gcs_uri"] = gcs_uri
        return {
            "job_id": job_id,
            "source": self.connector_type,
            "shards_created": 1,
            "events_staged": 1,
            "last_checkpoint": {"gcs_uri": gcs_uri, "event_count": 1},
            "shard_manifests": [
                {
                    "job_id": job_id,
                    "source": self.connector_type,
                    "gcs_uri": gcs_uri,
                    "event_count": 1,
                    "schema_version": "v1",
                    "shard_index": 1,
                    "source_config_id": "Adjust Source",
                }
            ],
            "stopped": False,
        }

    def fake_process_notifications(self, notifications, progress_callback=None):
        if not seeded["done"]:
            bigquery_service.write_events_staging(
                [
                    {
                        "job_id": job_id,
                        "source": "adjust",
                        "player_id": "delete-me",
                        "canonical_user_id": "delete-me",
                        "event_type": "session_start",
                        "event_time": "2026-03-01T08:00:00",
                        "event_properties": {"platform": "ios"},
                        "user_properties": {"email": "delete-me@example.com"},
                    }
                ],
                job_id=job_id,
            )
            bigquery_service.run_events_curation(job_id=job_id)
            bigquery_service.refresh_player_latest_state(job_id=job_id)
            with db_module.get_session_factory()() as session:
                repository = SqlAlchemyControlPlaneRepository(session)
                repository.upsert_resource(
                    "identity_summary",
                    job_id,
                    status="ready",
                    name=job_id,
                    payload={"job_id": job_id, "canonical_user_id_coverage": 100.0},
                )
                session.commit()
            seeded["done"] = True
        processing_started.set()
        while True:
            if callable(progress_callback):
                progress_callback(
                    1,
                    1,
                    {
                        "manifests_processed": 1,
                        "raw_normalized_events": 1,
                        "events_staging_written": 1,
                        "pipeline_dead_letters_written": 0,
                        "flag_counts": {},
                        "warehouse_stats": {},
                    },
                )
            time.sleep(0.02)

    monkeypatch.setattr(
        "app.application.imports.IngestionService.fetch_and_stage_events",
        fake_fetch_and_stage_events,
    )
    monkeypatch.setattr(
        "app.application.imports.DataflowNormalizationRunner.process_notifications",
        fake_process_notifications,
    )

    run_import = client.post(import_job["links"]["self"] + "/run?background=true")
    assert run_import.status_code == 202
    assert processing_started.wait(timeout=2)

    stop_import = client.post(import_job["links"]["self"] + "/stop")
    assert stop_import.status_code == 200
    assert stop_import.json()["status"] == "stopped"

    delete_import = client.delete(import_job["links"]["self"])
    assert delete_import.status_code == 204

    get_deleted = client.get(import_job["links"]["self"])
    assert get_deleted.status_code == 404

    with pytest.raises(FileNotFoundError):
        gcs_service.download_raw_events(seeded["gcs_uri"])

    standardized_rows = [
        row
        for row in bigquery_service.get_rows_for_alias("standardized")
        if str(row.get("job_id") or row.get("job_identifier") or "") == job_id
    ]
    curated_rows = [
        row
        for row in bigquery_service.get_rows_for_alias("fact_events_unified")
        if str(row.get("job_id") or row.get("job_identifier") or "") == job_id
    ]
    latest_rows = [
        row
        for row in bigquery_service.get_rows_for_alias("mart_user_daily")
        if str(row.get("last_job_id") or row.get("job_id") or "") == job_id
        or str(row.get("player_id") or row.get("canonical_user_id") or "") == "delete-me"
    ]
    assert standardized_rows == []
    assert curated_rows == []
    assert latest_rows == []

    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        assert repository.get_resource("identity_summary", job_id) is None
        manifest_resources = [
            item
            for item in repository.list_resources("import_manifest")
            if str(item.get("name") or "") == job_id
            or str((item.get("payload") or {}).get("job_id") or "") == job_id
        ]
        assert manifest_resources == []


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
