from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from app.core import db as db_module
from app.core.db import session_scope
from app.main import create_app
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / 'local_jobs.db'))
    monkeypatch.setenv("SCHEDULER_ENABLED", "false")
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    clear_shared_bigquery_service_cache()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def _seed_prediction_job(
    job_id: str,
    *,
    source_name: str,
    history_snapshot_at: str,
    prediction_mode: str = "local",
) -> None:
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.create_prediction_job(
            {
                "id": job_id,
                "import_job_id": f"imp_{job_id}",
                "status": "completed",
                "spec": {
                    "import_job_id": f"imp_{job_id}",
                    "audience_scope": "source",
                    "source_name": source_name,
                    "prediction_mode": prediction_mode,
                },
                "progress": {
                    "current": 2,
                    "total": 2,
                    "pct": 100.0,
                    "details": {
                        "source_name": source_name,
                        "audience_scope": "source",
                        "prediction_mode": prediction_mode,
                        "audience_label": source_name,
                        "history_snapshot_at": history_snapshot_at,
                    },
                },
            }
        )
        session.commit()


def _append_prediction_rows(job_id: str, rows: list[dict]) -> None:
    service = get_shared_bigquery_service()
    service.append_prediction_results(job_id, rows)


def _seed_builder_prediction_data() -> None:
    _seed_prediction_job(
        "pred_source_a_old",
        source_name="Amplitude 1",
        history_snapshot_at="2026-04-01T00:00:00+00:00",
    )
    _append_prediction_rows(
        "pred_source_a_old",
        [
            {
                "prediction_job_id": "pred_source_a_old",
                "canonical_user_id": "u_old",
                "user_id": "u_old",
                "email": "old@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "local",
                "suggested_action": "email",
                "days_since_last_seen": 12,
                "session_count": 1,
                "event_count": 4,
                "completed_at": "2026-04-01T00:00:00+00:00",
            }
        ],
    )

    _seed_prediction_job(
        "pred_source_a_new",
        source_name="Amplitude 1",
        history_snapshot_at="2026-04-10T00:00:00+00:00",
    )
    _append_prediction_rows(
        "pred_source_a_new",
        [
            {
                "prediction_job_id": "pred_source_a_new",
                "canonical_user_id": "u_shared",
                "user_id": "u_shared",
                "email": "shared@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "local",
                "suggested_action": "email",
                "days_since_last_seen": 14,
                "session_count": 2,
                "event_count": 5,
                "completed_at": "2026-04-10T00:00:00+00:00",
            },
            {
                "prediction_job_id": "pred_source_a_new",
                "canonical_user_id": "u_a_only",
                "user_id": "u_a_only",
                "email": "aonly@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "local",
                "suggested_action": "push_notification",
                "days_since_last_seen": 21,
                "session_count": 3,
                "event_count": 7,
                "completed_at": "2026-04-10T00:00:00+00:00",
            },
        ],
    )

    _seed_prediction_job(
        "pred_source_b",
        source_name="Adjust Source",
        history_snapshot_at="2026-04-11T00:00:00+00:00",
        prediction_mode="cloud",
    )
    _append_prediction_rows(
        "pred_source_b",
        [
            {
                "prediction_job_id": "pred_source_b",
                "canonical_user_id": "u_shared",
                "user_id": "u_shared",
                "email": "shared@example.com",
                "predicted_churn_risk": "medium",
                "churn_state": "active",
                "prediction_source": "cloud",
                "suggested_action": "email",
                "days_since_last_seen": 9,
                "session_count": 8,
                "event_count": 11,
                "completed_at": "2026-04-11T00:00:00+00:00",
            },
            {
                "prediction_job_id": "pred_source_b",
                "canonical_user_id": "u_b_only",
                "user_id": "u_b_only",
                "email": "bonly@example.com",
                "predicted_churn_risk": "high",
                "churn_state": "active",
                "prediction_source": "cloud",
                "suggested_action": "push_notification",
                "days_since_last_seen": 30,
                "session_count": 1,
                "event_count": 2,
                "completed_at": "2026-04-11T00:00:00+00:00",
            },
        ],
    )


def test_cohort_builder_options_expose_prediction_sources_and_fields(client):
    _seed_builder_prediction_data()

    response = client.get("/api/v1/cohorts/builder/options")

    assert response.status_code == 200
    payload = response.json()
    assert payload["defaults"]["audience_basis"] == "prediction"
    assert payload["defaults"]["prediction_scope"] == "source"
    source_names = {item["source_name"] for item in payload["prediction_sources"]}
    assert source_names == {"Amplitude 1", "Adjust Source"}
    field_names = {item["field"] for item in payload["filter_fields"]}
    assert {"predicted_churn_risk", "days_since_last_seen", "source_name"} <= field_names


def test_cohort_builder_preview_uses_latest_source_job_and_dedupes_users(client):
    _seed_builder_prediction_data()

    response = client.post(
        "/api/v1/cohorts/builder/preview",
        json={
            "name": "winback_priority",
            "audience_basis": "prediction",
            "prediction_scope": "source",
            "source_names": ["Amplitude 1", "Adjust Source"],
            "output_mode": "combined",
            "conditions": [
                {
                    "field": "predicted_churn_risk",
                    "op": "in",
                    "values": ["high", "medium"],
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["member_count"] == 3
    resolved_ids = {item["prediction_job_id"] for item in payload["resolved_predictions"]}
    assert resolved_ids == {"pred_source_a_new", "pred_source_b"}
    preview_ids = {item["canonical_user_id"] for item in payload["preview_members"]}
    assert preview_ids == {"u_shared", "u_a_only", "u_b_only"}
    breakdown = {item["prediction_job_id"]: item["member_count"] for item in payload["source_breakdown"]}
    assert breakdown["pred_source_a_new"] == 2
    assert breakdown["pred_source_b"] == 2


def test_cohort_builder_create_separate_creates_one_draft_per_source(client):
    _seed_builder_prediction_data()

    response = client.post(
        "/api/v1/cohorts/builder/create",
        json={
            "name": "retention_push",
            "audience_basis": "prediction",
            "prediction_scope": "source",
            "source_names": ["Amplitude 1", "Adjust Source"],
            "output_mode": "separate",
            "conditions": [
                {
                    "field": "predicted_churn_risk",
                    "op": "=",
                    "value": "high",
                }
            ],
            "tags": ["guided-builder"],
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["created_count"] == 2
    names = {item["name"] for item in payload["items"]}
    assert "retention_push__amplitude_1" in names
    assert "retention_push__adjust_source" in names
    for item in payload["items"]:
        assert item["status"] == "draft"
        assert item["definition"]["entrypoint"] == "guided_builder"
        assert len(item["definition"]["prediction_job_ids"]) == 1
