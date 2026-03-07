from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.core import db as db_module
from app.main import create_app


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


def test_legacy_frontend_bootstrap_routes(client):
    root = client.get("/")
    assert root.status_code == 200
    assert root.headers["content-type"].startswith("text/html")

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    connectors = client.get("/connectors/list")
    assert connectors.status_code == 200
    assert connectors.json() == {"connectors": []}

    sources = client.get("/list-configured-sources")
    assert sources.status_code == 200
    assert sources.json() == {"sources": []}

    imports = client.get("/list-imports")
    assert imports.status_code == 200
    assert imports.json() == {"imports": []}

    experiment = client.get("/experiments/config")
    assert experiment.status_code == 200
    assert experiment.json()["experiment"]["experiment_id"] == "churn_engagement_v1"

    churn = client.get("/churn/config")
    assert churn.status_code == 200
    assert churn.json()["churn"]["churn_inactive_days"] == 14

    freshness = client.get("/connector-freshness")
    assert freshness.status_code == 200
    assert freshness.json() == {"connectors": {}}

    identity = client.get("/identity-links?limit=10")
    assert identity.status_code == 200
    assert identity.json() == {"identity_links": []}

    rejected = client.get("/cleanup/rejected-events?limit=10")
    assert rejected.status_code == 200
    assert rejected.json() == {"rejected_events": []}

    conflicts = client.get("/cleanup/conflicts?limit=10")
    assert conflicts.status_code == 200
    assert conflicts.json() == {"conflicts": []}

    external = client.get("/churn/external-updates?limit=10")
    assert external.status_code == 200
    assert external.json() == {"updated_at": None, "by_user_id": [], "by_email": []}


def test_legacy_connector_import_prediction_flow(client):
    configure = client.post(
        "/configure-adjust-credentials",
        json={"api_token": "adjust-token", "api_url": "https://example.com/adjust"},
    )
    assert configure.status_code == 200
    assert configure.json()["connector"]["name"] == "Adjust 1"

    sources = client.get("/list-configured-sources")
    assert sources.status_code == 200
    assert sources.json()["sources"][0]["name"] == "Adjust 1"

    import_response = client.post(
        "/ingest-and-process-data",
        json={"start_date": "20260301", "end_date": "20260301", "source": "Adjust 1"},
    )
    assert import_response.status_code == 200
    import_job_name = import_response.json()["job"]["name"]

    imports = client.get("/list-imports")
    assert imports.status_code == 200
    listed_job = imports.json()["imports"][0]
    assert listed_job["name"] == import_job_name
    assert listed_job["status"] == "Ready to Use"

    freshness = client.get("/connector-freshness")
    freshness_payload = freshness.json()["connectors"]["Adjust 1"]
    assert freshness_payload["last_success_at"] is not None
    assert freshness_payload["last_ingested_events"] >= 1

    sync_prediction = client.post(
        "/predict-churn-for-import",
        json={"job_name": import_job_name, "force_recalculate": True, "prediction_mode": "local"},
    )
    assert sync_prediction.status_code == 200
    sync_body = sync_prediction.json()
    assert sync_body["prediction_job_id"].startswith("pred_")
    assert len(sync_body["predictions"]) >= 1

    async_prediction = client.post(
        "/predict-churn-for-import-async",
        json={"job_name": import_job_name, "force_recalculate": True, "prediction_mode": "local"},
    )
    assert async_prediction.status_code == 200
    async_job_id = async_prediction.json()["prediction_job_id"]

    latest_status = None
    for _ in range(20):
        status = client.get(f"/prediction-job/{async_job_id}")
        assert status.status_code == 200
        latest_status = status.json()["prediction_job"]
        if latest_status["status"] == "Ready":
            break
        time.sleep(0.05)

    assert latest_status is not None
    assert latest_status["status"] in {"Queued", "Processing", "Ready"}
