from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.core import db as db_module
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache


SERVICE_ACCOUNT_JSON = """{
  "type": "service_account",
  "project_id": "tenant-warehouse",
  "private_key_id": "key-id-1",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nabc123\\n-----END PRIVATE KEY-----\\n",
  "client_email": "warehouse-reader@tenant-warehouse.iam.gserviceaccount.com",
  "client_id": "1234567890",
  "token_uri": "https://oauth2.googleapis.com/token"
}"""


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    clear_shared_bigquery_service_cache()
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    clear_shared_bigquery_service_cache()
    db_module.clear_runtime_database_fallback()


def test_bigquery_connector_redacts_inline_service_account_json(client: TestClient):
    created = client.post(
        "/api/v1/connectors",
        json={
            "name": "Warehouse Scores",
            "type": "bigquery",
            "config": {
                "project_id": "warehouse-project",
                "dataset_id": "growth_inputs",
                "service_account_json": SERVICE_ACCOUNT_JSON,
            },
        },
    )
    assert created.status_code == 201, created.text
    assert created.json()["config"]["service_account_json"] is None
    assert created.json()["config"]["service_account_json_configured"] is True

    listed = client.get("/api/v1/connectors")
    assert listed.status_code == 200
    assert listed.json()[0]["config"]["service_account_json"] is None
    assert listed.json()[0]["config"]["service_account_json_configured"] is True


def test_bigquery_connector_accepts_service_account_info_object(client: TestClient):
    created = client.post(
        "/api/v1/connectors",
        json={
            "name": "Warehouse Lists",
            "type": "bigquery",
            "config": {
                "project_id": "warehouse-project",
                "dataset_id": "growth_inputs",
                "service_account_info_json": {
                    "type": "service_account",
                    "client_email": "warehouse-reader@tenant-warehouse.iam.gserviceaccount.com",
                    "private_key": "-----BEGIN PRIVATE KEY-----\\nabc123\\n-----END PRIVATE KEY-----\\n",
                    "token_uri": "https://oauth2.googleapis.com/token",
                },
            },
        },
    )
    assert created.status_code == 201, created.text
    assert created.json()["config"]["service_account_info_json"] is None
    assert created.json()["config"]["service_account_info_json_configured"] is True


def test_bigquery_connector_rejects_invalid_inline_service_account_json(client: TestClient):
    created = client.post(
        "/api/v1/connectors",
        json={
            "name": "Warehouse Scores",
            "type": "bigquery",
            "config": {
                "project_id": "warehouse-project",
                "dataset_id": "growth_inputs",
                "service_account_json": "{not valid json}",
            },
        },
    )
    assert created.status_code == 409
    assert created.json()["detail"] == "BigQuery service account JSON must be valid JSON."
