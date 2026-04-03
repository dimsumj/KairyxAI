from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.core import db as db_module
from bigquery_service import clear_shared_bigquery_service_cache
from workers import export_worker_app, import_worker_app, prediction_worker_app, scheduler_worker_app


WORKER_APPS = (
    (import_worker_app.app, "import-worker", "/pubsub/push"),
    (prediction_worker_app.app, "prediction-worker", "/pubsub/push"),
    (export_worker_app.app, "export-worker", "/pubsub/push"),
    (scheduler_worker_app.app, "scheduler-worker", "/run"),
)


@pytest.fixture(autouse=True)
def _configure_worker_environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_ENV", "local")
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("WORKER_SHARED_TOKEN", "test-worker-token")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local.db"))
    monkeypatch.setenv("BOOTSTRAP_TENANT_ID", "default")
    monkeypatch.setenv("BOOTSTRAP_PROJECT_ID", "default")
    db_module.clear_runtime_database_fallback()
    clear_shared_bigquery_service_cache()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    yield
    clear_shared_bigquery_service_cache()
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()


@pytest.mark.parametrize(("app", "service_name", "_path"), WORKER_APPS)
def test_worker_health_endpoint_is_public(app, service_name, _path):
    with TestClient(app) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": service_name}


@pytest.mark.parametrize(("app", "_service_name", "path"), WORKER_APPS)
def test_worker_endpoint_rejects_missing_token(app, _service_name, path):
    with TestClient(app) as client:
        response = client.post(path, json={})

    assert response.status_code == 401
    assert response.json()["detail"] == "Worker token is missing or invalid."


@pytest.mark.parametrize(
    ("app", "path"),
    (
        (import_worker_app.app, "/pubsub/push"),
        (prediction_worker_app.app, "/pubsub/push"),
        (export_worker_app.app, "/pubsub/push"),
    ),
)
def test_worker_push_accepts_bearer_token_before_payload_validation(app, path):
    with TestClient(app) as client:
        response = client.post(path, json={}, headers={"Authorization": "Bearer test-worker-token"})

    assert response.status_code == 400
    assert response.json()["detail"] == "Pub/Sub payload is missing job_id."


def test_worker_query_token_takes_precedence_over_authorization_header():
    with TestClient(import_worker_app.app) as client:
        response = client.post(
            "/pubsub/push?token=test-worker-token",
            json={},
            headers={"Authorization": "Bearer wrong-token"},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Pub/Sub payload is missing job_id."


def test_scheduler_run_accepts_query_token(monkeypatch):
    class FakeControlLoopService:
        def __init__(self, repository, settings):
            self.repository = repository
            self.settings = settings

        def tick(self, reference_time=None):
            return {"reference_time": reference_time, "status": "ok"}

    monkeypatch.setattr(scheduler_worker_app, "ControlLoopService", FakeControlLoopService)

    with TestClient(scheduler_worker_app.app) as client:
        response = client.post("/run?token=test-worker-token", json={"reference_time": "2026-04-02T12:00:00Z"})

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "result": {"reference_time": "2026-04-02T12:00:00Z", "status": "ok"},
    }
