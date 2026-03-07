from __future__ import annotations

from fastapi.testclient import TestClient

from app.core import db as db_module
from app.main import create_app
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
