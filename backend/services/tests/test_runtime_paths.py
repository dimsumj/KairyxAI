from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

from app.core import db as db_module
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service
import local_job_store
from local_job_store import list_identity_links, resolve_or_create_canonical_user_id


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
