from __future__ import annotations

import logging
import sqlite3

from fastapi.testclient import TestClient
import pytest
from sqlalchemy.exc import SQLAlchemyError

from app.core import db as db_module
from app.core.errors import is_database_locked_error
from app.core.logging import PredictionPollingAccessFilter
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service
import local_job_store
from local_job_store import list_identity_links, resolve_or_create_canonical_user_id
from runtime_paths import resolve_runtime_file_path


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


def test_init_db_does_not_disable_uvicorn_loggers(tmp_path, monkeypatch):
    target = tmp_path / "nested" / "state" / "control_plane.db"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{target}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    uvicorn_logger = logging.getLogger("uvicorn.error")
    original_disabled = uvicorn_logger.disabled
    uvicorn_logger.disabled = False
    try:
        db_module.init_db()
        assert uvicorn_logger.disabled is False
    finally:
        uvicorn_logger.disabled = original_disabled


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


def test_database_lock_error_detection_handles_exception_groups():
    exc = ExceptionGroup(
        "database lock group",
        [sqlite3.OperationalError("database is locked")],
    )

    assert is_database_locked_error(exc) is True


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


def test_resolve_runtime_file_path_uses_runtime_root_override(tmp_path, monkeypatch):
    runtime_root = tmp_path / "vercel-runtime"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KAIRYX_PLATFORM_SURFACE", "vercel_demo")
    monkeypatch.setenv("KAIRYX_RUNTIME_DIR", str(runtime_root))

    resolved = resolve_runtime_file_path(".cache/demo/state.json", ensure_parent=True)

    assert resolved == (runtime_root / ".cache" / "demo" / "state.json").resolve()
    assert resolved.parent.exists()


def test_init_db_falls_back_to_runtime_sqlite_only_for_vercel_demo(tmp_path, monkeypatch):
    runtime_root = tmp_path / "runtime"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_ENV", "demo")
    monkeypatch.setenv("KAIRYX_PLATFORM_SURFACE", "vercel_demo")
    monkeypatch.setenv("KAIRYX_RUNTIME_DIR", str(runtime_root))
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:1/kairyx")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setenv("SCHEDULER_ENABLED", "false")
    monkeypatch.setenv("CONTROL_PLANE_CONNECT_TIMEOUT_SECONDS", "1")
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    app = create_app()
    with TestClient(app) as client:
        response = client.get("/api/v1/health/live")

    payload = response.json()
    assert response.status_code == 200
    assert payload["control_plane_database_backend"] == "sqlite"
    assert payload["control_plane_database_fallback_active"] is True
    assert payload["control_plane_database_persistent"] is False
    assert (runtime_root / ".kairyx_control_plane.db").exists()

    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()


def test_init_db_does_not_fallback_outside_vercel_demo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()

    def fail_initialize_schema():
        raise SQLAlchemyError("database unavailable")

    monkeypatch.setattr(db_module, "_initialize_schema", fail_initialize_schema)

    try:
        with pytest.raises(SQLAlchemyError):
            db_module.init_db()
        assert db_module.is_runtime_database_fallback_active() is False
    finally:
        db_module.clear_runtime_database_fallback()
        db_module.get_engine.cache_clear()
        db_module.get_session_factory.cache_clear()


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
    assert log_filter.filter(build_record("/northstar/v1/predictions/pred_workspace")) is True
    assert log_filter.filter(build_record("/northstar/v1/predictions/pred_workspace/results?page=1&page_size=500")) is False
    assert log_filter.filter(build_record("/api/v1/health")) is True
