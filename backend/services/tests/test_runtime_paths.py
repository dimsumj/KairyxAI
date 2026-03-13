from __future__ import annotations

import logging
import sqlite3

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from app.core import db as db_module
from app.core.settings import get_settings
from app.core.logging import PredictionPollingAccessFilter
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service
import local_job_store
from local_job_store import list_identity_links, resolve_or_create_canonical_user_id
import runtime_paths


@pytest.fixture(autouse=True)
def _reset_runtime_database_state():
    db_module.clear_runtime_database_fallback()
    clear_shared_bigquery_service_cache()
    yield
    clear_shared_bigquery_service_cache()
    db_module.clear_runtime_database_fallback()


def test_local_job_store_accepts_sqlite_url_override(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "state" / "local_jobs.db"
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", f"sqlite:///{target}")

    canonical = resolve_or_create_canonical_user_id("amplitude", "user-123")

    assert canonical == "uid:user-123"
    assert target.exists()
    assert list_identity_links(limit=1)[0]["canonical_user_id"] == "uid:user-123"


def test_app_startup_creates_sqlite_parent_dirs(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "state" / "control_plane.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{target}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert target.exists()


def test_app_startup_continues_when_restart_reconciliation_fails(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "state" / "control_plane.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{target}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setattr(
        "app.main.ImportService.reconcile_jobs_after_restart",
        lambda self: (_ for _ in ()).throw(RuntimeError("unable to open database file")),
    )
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/api/v1/health")

    assert response.status_code == 200


def test_database_url_normalization_uses_psycopg_driver():
    assert db_module.normalize_database_url("postgres://user:pass@example.com:5432/demo") == (
        "postgresql+psycopg://user:pass@example.com:5432/demo"
    )
    assert db_module.normalize_database_url("postgresql://user:pass@example.com:5432/demo") == (
        "postgresql+psycopg://user:pass@example.com:5432/demo"
    )
    assert db_module.normalize_database_url("postgresql+psycopg://user:pass@example.com:5432/demo") == (
        "postgresql+psycopg://user:pass@example.com:5432/demo"
    )


def test_local_job_store_closes_sqlite_connections(tmp_path, monkeypatch):
    target = tmp_path / "tracked" / "local_jobs.db"
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(target))

    real_connect = sqlite3.connect
    open_count = 0
    close_count = 0

    class TrackingConnection:
        def __init__(self, conn):
            self._conn = conn

        def close(self):
            nonlocal close_count
            close_count += 1
            return self._conn.close()

        def __getattr__(self, name):
            return getattr(self._conn, name)

    def tracking_connect(*args, **kwargs):
        nonlocal open_count
        open_count += 1
        return TrackingConnection(real_connect(*args, **kwargs))

    monkeypatch.setattr(local_job_store.sqlite3, "connect", tracking_connect)

    resolve_or_create_canonical_user_id("amplitude", "user-1")
    resolve_or_create_canonical_user_id("amplitude", "user-2")
    list_identity_links(limit=10)

    assert open_count > 0
    assert close_count == open_count


def test_shared_bigquery_service_reuses_instance_per_runtime_context(tmp_path, monkeypatch):
    clear_shared_bigquery_service_cache()

    workspace_a = tmp_path / "workspace-a"
    workspace_b = tmp_path / "workspace-b"
    workspace_a.mkdir()
    workspace_b.mkdir()

    monkeypatch.chdir(workspace_a)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    service_a1 = get_shared_bigquery_service()
    service_a2 = get_shared_bigquery_service()

    monkeypatch.chdir(workspace_b)
    service_b = get_shared_bigquery_service()

    assert service_a1 is service_a2
    assert service_a1 is not service_b


def test_vercel_runtime_defaults_use_tmp_storage(tmp_path, monkeypatch):
    monkeypatch.delenv("KAIRYX_RUNTIME_DIR", raising=False)
    monkeypatch.delenv("SCHEDULER_ENABLED", raising=False)
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setattr(runtime_paths.tempfile, "gettempdir", lambda: str(tmp_path))

    control_plane_url = runtime_paths.default_control_plane_database_url()
    local_job_db = runtime_paths.default_local_job_store_path()
    cache_path = runtime_paths.resolve_runtime_file_path(".cache/demo.jsonl", ensure_parent=True)
    settings = get_settings()

    expected_root = tmp_path / "kairyxai-runtime"
    assert control_plane_url == f"sqlite:///{expected_root / '.kairyx_control_plane.db'}"
    assert local_job_db == expected_root / ".kairyx_local.db"
    assert cache_path == expected_root / ".cache" / "demo.jsonl"
    assert settings.scheduler_enabled is False


def test_vercel_remote_control_plane_db_falls_back_to_local_sqlite(tmp_path, monkeypatch):
    runtime_dir = tmp_path / "runtime"
    local_db = tmp_path / "local_jobs.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("VERCEL", "1")
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("KAIRYX_MOCK_STORAGE_BACKEND", "database")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", "postgresql://demo:demo@example.com:5432/kairyx")
    monkeypatch.setenv("KAIRYX_RUNTIME_DIR", str(runtime_dir))
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(local_db))

    create_all_attempts: list[str] = []
    real_create_all = db_module.Base.metadata.create_all

    def flaky_create_all(bind=None, *args, **kwargs):
        create_all_attempts.append(str(bind.url))
        if len(create_all_attempts) == 1:
            raise SQLAlchemyError("quota exceeded")
        return real_create_all(bind=bind, *args, **kwargs)

    monkeypatch.setattr(db_module.Base.metadata, "create_all", flaky_create_all)

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/api/v1/health")

    payload = response.json()
    fallback_db = runtime_dir / ".kairyx_control_plane.db"

    assert response.status_code == 200
    assert create_all_attempts[0].startswith("postgresql+psycopg://")
    assert any(attempt.startswith(f"sqlite:///{fallback_db}") for attempt in create_all_attempts[1:])
    assert payload["mock_state_backend"] == "database"
    assert payload["mock_state_persistent"] is False
    assert payload["control_plane_database_backend"] == "sqlite"
    assert payload["control_plane_database_persistent"] is False
    assert payload["control_plane_database_fallback_active"] is True
    assert payload["local_cache"]["storage_backend"] == "database"
    assert payload["local_cache"]["persistent"] is False
    assert fallback_db.exists()


def test_prediction_polling_access_filter_logs_only_first_request_per_job():
    log_filter = PredictionPollingAccessFilter()

    def build_record(path: str) -> logging.LogRecord:
        return logging.LogRecord(
            name="uvicorn.access",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg='%s - "%s %s HTTP/%s" %s',
            args=("127.0.0.1:12345", "GET", path, "1.1", 200),
            exc_info=None,
        )

    assert log_filter.filter(build_record("/api/v1/predictions/pred_abc123")) is True
    assert log_filter.filter(build_record("/api/v1/predictions/pred_abc123/results?page=1&page_size=500")) is False
    assert log_filter.filter(build_record("/api/v1/predictions/pred_other/results?page=1&page_size=500")) is True
    assert log_filter.filter(build_record("/api/v1/health")) is True
