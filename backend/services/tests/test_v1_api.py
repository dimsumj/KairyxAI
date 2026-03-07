from __future__ import annotations

from pathlib import Path
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.application.imports import ImportService
from app.core import db as db_module
from app.infrastructure.db_models import ImportJobModel
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from app.main import create_app
from bigquery_service import BigQueryService
from gcs_service import GcsService


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
    assert run_prediction.json()["progress"]["details"]["execution_label"] == "Local"
    assert run_prediction.json()["progress"]["details"]["prediction_mode"] == "local"

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


def test_prediction_uses_saved_google_connector(client, monkeypatch):
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

    captured = {}

    class FakeGeminiClient:
        def __init__(self, api_key=None, model_name=None):
            captured["api_key"] = api_key
            captured["model_name"] = model_name

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
            "prediction_mode": "local",
        },
    )
    assert create_prediction.status_code == 201
    prediction_job = create_prediction.json()

    run_prediction = client.post(prediction_job["links"]["self"] + "/run")
    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "completed"
    assert run_prediction.json()["progress"]["details"]["execution_label"] == "AI"
    assert run_prediction.json()["progress"]["details"]["prediction_mode"] == "local"

    results = client.get(prediction_job["links"]["results"])
    assert results.status_code == 200
    payload = results.json()
    assert payload["total"] == 1
    assert payload["items"][0]["churn_reason"] == "Gemini connector used"
    assert payload["items"][0]["suggested_action"] == "Gemini save offer"
    assert captured["api_key"] == "google-api-key-from-connector"
    assert captured["model_name"] == "gemini-2.5-flash"
    assert captured["gemini_client_present"] is True
    assert captured["estimate_called"] is True
    assert captured["job_id"] == import_job["id"]


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

    release_remaining_players.set()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert run_result["response"].status_code == 200
    assert run_result["response"].json()["status"] == "stopped"

    stopped_state = client.get(prediction_job["links"]["self"])
    assert stopped_state.status_code == 200
    assert stopped_state.json()["status"] == "stopped"

    final_results = client.get(prediction_job["links"]["results"])
    assert final_results.status_code == 200
    final_payload = final_results.json()
    assert final_payload["total"] == 1
    assert final_payload["items"][0]["suggested_action"] == "message for player-1"


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
