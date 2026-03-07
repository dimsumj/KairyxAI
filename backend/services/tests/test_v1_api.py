from __future__ import annotations

from pathlib import Path
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.application.imports import ImportService
from app.core import db as db_module
from app.infrastructure.db_models import ImportJobModel
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
    assert "window.location.origin" in resp.text
    assert "/api/v1" in resp.text


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

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    payload = results.json()
    assert payload["total"] >= 1
    assert payload["items"][0]["user_id"] == "adjust_user_1001"

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
    assert stop_import.json()["status"] == "stopping"

    thread.join(timeout=5)
    assert not thread.is_alive()
    assert run_result["response"].status_code == 200
    assert run_result["response"].json()["status"] == "stopped"

    import_state = client.get(import_job["links"]["self"])
    assert import_state.status_code == 200
    payload = import_state.json()
    assert payload["status"] == "stopped"
    assert payload["progress"]["details"]["stop_reason"] == "Stopped by user."
