from __future__ import annotations

from dataclasses import replace
import time

import pytest
from fastapi.testclient import TestClient

from app.application.imports import ImportService
from app.core import db as db_module
from app.core.deps import get_settings_dependency
from app.core.settings import get_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache


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


def _create_bigquery_connector(client: TestClient, *, name: str, mock_tables: dict) -> None:
    response = client.post(
        "/api/v1/connectors",
        json={
            "name": name,
            "type": "bigquery",
            "config": {
                "project_id": "warehouse-project",
                "dataset_id": "growth_inputs",
                "mock_tables": mock_tables,
            },
        },
    )
    assert response.status_code == 201, response.text


def test_bigquery_connector_health_and_table_listing(client: TestClient):
    _create_bigquery_connector(
        client,
        name="Warehouse Scores",
        mock_tables={
            "prediction_scores": [{"player_id": "u_1"}],
            "churn_view": {"rows": [{"player_id": "u_2"}], "table_type": "view"},
        },
    )

    health = client.get("/api/v1/connectors/Warehouse%20Scores/health")
    assert health.status_code == 200
    assert health.json()["ok"] is True

    tables = client.get("/api/v1/connectors/Warehouse%20Scores/tables")
    assert tables.status_code == 200
    assert tables.json()["type"] == "bigquery"
    assert tables.json()["items"] == [
        {"table_name": "churn_view", "table_type": "view", "row_count": 1},
        {"table_name": "prediction_scores", "table_type": "table", "row_count": 1},
    ]

    count = client.get("/api/v1/connectors/Warehouse%20Scores/tables/prediction_scores/count")
    assert count.status_code == 200
    assert count.json()["table_name"] == "prediction_scores"
    assert count.json()["table_type"] == "table"
    assert count.json()["row_count"] == 1


def test_bigquery_connector_table_listing_returns_controlled_error(client: TestClient, monkeypatch):
    monkeypatch.setenv("DATA_BACKEND_MODE", "gcp")
    _create_bigquery_connector(
        client,
        name="Warehouse Scores",
        mock_tables={},
    )

    class BrokenBigQueryClient:
        def list_tables(self, dataset_ref):
            raise RuntimeError("permission denied for dataset growth_inputs")

    monkeypatch.setattr(
        "connectors.bigquery_connector.BigQueryConnector._get_client",
        lambda self: BrokenBigQueryClient(),
    )

    tables = client.get("/api/v1/connectors/Warehouse%20Scores/tables")
    assert tables.status_code == 409
    assert "Unable to list BigQuery tables for dataset" in tables.json()["detail"]
    assert "permission denied for dataset growth_inputs" in tables.json()["detail"]


def test_bigquery_connector_row_count_returns_controlled_error(client: TestClient, monkeypatch):
    monkeypatch.setenv("DATA_BACKEND_MODE", "gcp")
    _create_bigquery_connector(
        client,
        name="Warehouse Scores",
        mock_tables={},
    )

    class BrokenBigQueryClient:
        def get_table(self, table_ref):
            raise RuntimeError("permission denied for table prediction_scores")

    monkeypatch.setattr(
        "connectors.bigquery_connector.BigQueryConnector._get_client",
        lambda self: BrokenBigQueryClient(),
    )

    count = client.get("/api/v1/connectors/Warehouse%20Scores/tables/prediction_scores/count")
    assert count.status_code == 409
    assert "Unable to fetch BigQuery row count for table" in count.json()["detail"]
    assert "permission denied for table prediction_scores" in count.json()["detail"]


def test_bigquery_prediction_table_import_materializes_prediction_job_without_mapping_gate(client: TestClient, monkeypatch):
    _create_bigquery_connector(
        client,
        name="Warehouse Scores",
        mock_tables={
            "prediction_scores": [
                {
                    "player_id": "u_1",
                    "email": "u1@example.com",
                    "risk": "medium",
                    "score": 0.55,
                    "scored_at": "2026-04-01T10:00:00",
                },
                {
                    "player_id": "u_2",
                    "email": "u2@example.com",
                    "risk": "high",
                    "score": 0.88,
                    "scored_at": "2026-04-01T10:00:00",
                },
                {
                    "player_id": "u_1",
                    "email": "u1-latest@example.com",
                    "risk": "high",
                    "score": 0.91,
                    "scored_at": "2026-04-02T10:00:00",
                },
            ]
        },
    )

    def _unexpected_mapping_gate(*args, **kwargs):
        raise AssertionError("BigQuery table imports must not invoke mapping coverage gates.")

    monkeypatch.setattr(ImportService, "_mapping_coverage", _unexpected_mapping_gate)

    created = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Warehouse Scores",
            "table_name": "prediction_scores",
            "resource_kind": "external_prediction_scores",
            "column_mapping": {
                "canonical_user_id": "player_id",
                "user_id": "player_id",
                "email": "email",
                "predicted_churn_risk": "risk",
                "score": "score",
                "score_timestamp": "scored_at",
            },
            "start_date": "2026-04-01",
            "end_date": "2026-04-03",
        },
    )
    assert created.status_code == 201, created.text
    job_id = created.json()["id"]

    run = client.post(f"/api/v1/imports/{job_id}/run")
    assert run.status_code == 200, run.text
    assert run.json()["status"] == "completed"
    assert run.json()["progress"]["details"]["mapping_coverage"] == 100.0
    assert run.json()["progress"]["details"]["bigquery_table_import"]["row_count"] == 3
    prediction_job_id = run.json()["progress"]["details"]["linked_prediction_job_id"]
    assert prediction_job_id

    prediction_job = client.get(f"/api/v1/predictions/{prediction_job_id}")
    assert prediction_job.status_code == 200
    assert prediction_job.json()["status"] == "completed"
    assert prediction_job.json()["spec"]["prediction_mode"] == "external"

    results = client.get(f"/api/v1/predictions/{prediction_job_id}/results")
    assert results.status_code == 200
    assert results.json()["total"] == 2
    by_user = {item["user_id"]: item for item in results.json()["items"]}
    assert by_user["u_1"]["email"] == "u1-latest@example.com"
    assert by_user["u_1"]["predicted_churn_risk"] == "high"
    assert by_user["u_2"]["predicted_churn_risk"] == "high"


def test_bigquery_table_import_times_out_when_page_fetch_exceeds_budget(client: TestClient, monkeypatch):
    settings = replace(
        get_settings(),
        import_network_timeout_seconds=0.2,
        import_stop_poll_interval_seconds=0.05,
    )
    client.app.dependency_overrides[get_settings_dependency] = lambda: settings

    _create_bigquery_connector(
        client,
        name="Warehouse Lists",
        mock_tables={"churned_users": []},
    )

    created = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Warehouse Lists",
            "table_name": "churned_users",
            "resource_kind": "churn_list",
            "activate_cohort": True,
            "cohort_name": "slow_bigquery_list",
            "column_mapping": {
                "canonical_user_id": "player_id",
                "user_id": "player_id",
                "email": "email",
                "reason": "reason",
                "segment": "segment",
                "as_of_timestamp": "as_of",
            },
            "start_date": "2026-04-01",
            "end_date": "2026-04-03",
        },
    )
    assert created.status_code == 201, created.text
    job_id = created.json()["id"]

    class SlowBigQueryConnector:
        def fetch_table_rows_page(self, *args, **kwargs):
            time.sleep(0.45)
            return {
                "rows": [
                    {
                        "player_id": "u_1",
                        "email": "u1@example.com",
                        "reason": "inactive_7d",
                        "segment": "vip",
                        "as_of": "2026-04-01T10:00:00",
                    }
                ],
                "total": 1,
                "next_cursor": None,
                "has_more": False,
            }

    monkeypatch.setattr(
        "app.application.imports.create_connector",
        lambda connector_type, config: SlowBigQueryConnector(),
    )

    run = client.post(f"/api/v1/imports/{job_id}/run")
    assert run.status_code == 500, run.text
    assert "timed out" in run.json()["detail"]

    failed_job = client.get(f"/api/v1/imports/{job_id}")
    assert failed_job.status_code == 200
    assert failed_job.json()["status"] == "failed"
    assert failed_job.json()["progress"]["details"]["failure_stage"] == "bigquery_table_import"


def test_bigquery_timed_out_import_can_be_deleted(client: TestClient, monkeypatch):
    settings = replace(
        get_settings(),
        import_network_timeout_seconds=0.2,
        import_stop_poll_interval_seconds=0.05,
    )
    client.app.dependency_overrides[get_settings_dependency] = lambda: settings

    _create_bigquery_connector(
        client,
        name="Warehouse Lists",
        mock_tables={"churned_users": []},
    )

    created = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Warehouse Lists",
            "table_name": "churned_users",
            "resource_kind": "churn_list",
            "activate_cohort": True,
            "cohort_name": "slow_bigquery_list",
            "column_mapping": {
                "canonical_user_id": "player_id",
                "user_id": "player_id",
                "email": "email",
                "reason": "reason",
                "segment": "segment",
                "as_of_timestamp": "as_of",
            },
            "start_date": "2026-04-01",
            "end_date": "2026-04-03",
        },
    )
    assert created.status_code == 201, created.text
    job_id = created.json()["id"]

    class SlowBigQueryConnector:
        def fetch_table_rows_page(self, *args, **kwargs):
            time.sleep(0.45)
            return {
                "rows": [
                    {
                        "player_id": "u_1",
                        "email": "u1@example.com",
                        "reason": "inactive_7d",
                        "segment": "vip",
                        "as_of": "2026-04-01T10:00:00",
                    }
                ],
                "total": 1,
                "next_cursor": None,
                "has_more": False,
            }

    monkeypatch.setattr(
        "app.application.imports.create_connector",
        lambda connector_type, config: SlowBigQueryConnector(),
    )

    run = client.post(f"/api/v1/imports/{job_id}/run")
    assert run.status_code == 500, run.text

    delete_job = client.delete(f"/api/v1/imports/{job_id}")
    assert delete_job.status_code == 204, delete_job.text

    deleted = client.get(f"/api/v1/imports/{job_id}")
    assert deleted.status_code == 404


def test_bigquery_completed_import_clears_stale_failure_details(client: TestClient, monkeypatch):
    _create_bigquery_connector(
        client,
        name="Warehouse Lists",
        mock_tables={"churned_users": []},
    )

    created = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Warehouse Lists",
            "table_name": "churned_users",
            "resource_kind": "churn_list",
            "activate_cohort": True,
            "cohort_name": "warehouse_lists",
            "column_mapping": {
                "canonical_user_id": "player_id",
                "user_id": "player_id",
                "email": "email",
                "reason": "reason",
                "segment": "segment",
                "as_of_timestamp": "as_of",
            },
            "start_date": "2026-04-01",
            "end_date": "2026-04-03",
        },
    )
    assert created.status_code == 201, created.text
    job_id = created.json()["id"]

    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        current_job = repository.get_import_job(job_id)
        assert current_job is not None
        repository.update_import_job(
            job_id,
            {
                "status": "failed",
                "error": "fetch bigquery table page timed out after 0.2s without progress.",
                "progress": {
                    "current": 0,
                    "total": 0,
                    "pct": 0.0,
                    "details": {
                        **dict((current_job.get("progress") or {}).get("details") or {}),
                        "phase": "reading_bigquery_table",
                        "failure_reason": "fetch bigquery table page timed out after 0.2s without progress.",
                        "failure_stage": "bigquery_table_import",
                    },
                },
            },
        )
        session.commit()

    class SuccessfulBigQueryConnector:
        def fetch_table_rows_page(self, *args, **kwargs):
            return {
                "rows": [
                    {
                        "player_id": "u_1",
                        "email": "u1@example.com",
                        "reason": "inactive_7d",
                        "segment": "vip",
                        "as_of": "2026-04-01T10:00:00",
                    }
                ],
                "total": 1,
                "next_cursor": None,
                "has_more": False,
            }

    monkeypatch.setattr(
        "app.application.imports.create_connector",
        lambda connector_type, config: SuccessfulBigQueryConnector(),
    )

    run = client.post(f"/api/v1/imports/{job_id}/run")
    assert run.status_code == 200, run.text
    assert run.json()["status"] == "completed"
    assert run.json()["error"] is None
    assert run.json()["progress"]["details"].get("failure_reason") is None
    assert run.json()["progress"]["details"].get("failure_stage") is None

def test_bigquery_churn_list_import_creates_active_cohort_and_runs_closed_loop(client: TestClient):
    _create_bigquery_connector(
        client,
        name="Warehouse Lists",
        mock_tables={
            "churned_users": [
                {
                    "player_id": "u_1",
                    "email": "u1@example.com",
                    "reason": "inactive_7d",
                    "segment": "vip",
                    "as_of": "2026-04-01T10:00:00",
                },
                {
                    "player_id": "u_2",
                    "email": "u2@example.com",
                    "reason": "inactive_14d",
                    "segment": "casual",
                    "as_of": "2026-04-01T10:00:00",
                },
                {
                    "player_id": "u_2",
                    "email": "u2-latest@example.com",
                    "reason": "inactive_14d",
                    "segment": "casual",
                    "as_of": "2026-04-02T10:00:00",
                },
            ]
        },
    )

    created = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Warehouse Lists",
            "table_name": "churned_users",
            "resource_kind": "churn_list",
            "activate_cohort": True,
            "cohort_name": "bigquery_churn_list",
            "column_mapping": {
                "canonical_user_id": "player_id",
                "user_id": "player_id",
                "email": "email",
                "reason": "reason",
                "segment": "segment",
                "as_of_timestamp": "as_of",
            },
            "start_date": "2026-04-01",
            "end_date": "2026-04-03",
        },
    )
    assert created.status_code == 201, created.text
    job_id = created.json()["id"]

    run = client.post(f"/api/v1/imports/{job_id}/run")
    assert run.status_code == 200, run.text
    cohort_id = run.json()["progress"]["details"]["linked_cohort_id"]
    assert cohort_id

    cohort = client.get(f"/api/v1/cohorts/{cohort_id}")
    assert cohort.status_code == 200
    assert cohort.json()["status"] == "active"
    assert cohort.json()["member_count"] == 2

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=bigquery_churn_list_exp",
        json={
            "enabled": True,
            "primary_metric": "return_rate",
            "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
            "min_sample_size": 1,
            "min_runtime_hours": 0,
            "cohort_id": cohort_id,
            "holdout_pct": 0.0,
            "b_variant_pct": 0.0,
        },
    )
    assert experiment.status_code == 200

    workflow = client.post(
        "/api/v1/workflows",
        json={
            "name": "bigquery_churn_list_workflow",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily"},
            "action": {"channel": "push_notification", "content": "Come back for a reward"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
            "experiment_id": "bigquery_churn_list_exp",
        },
    )
    assert workflow.status_code == 201
    workflow_id = workflow.json()["workflow_id"]

    publish = client.post(f"/api/v1/workflows/{workflow_id}/publish")
    assert publish.status_code == 200

    scheduled = client.post(
        "/api/v1/orchestrator/run-due",
        json={"reference_time": "2026-04-03T10:00:00", "limit_per_workflow": 10},
    )
    assert scheduled.status_code == 200
    assert scheduled.json()["items"][0]["success"] == 2

    deliveries = client.get(f"/api/v1/workflows/{workflow_id}/deliveries")
    assert deliveries.status_code == 200
    assert len(deliveries.json()["items"]) == 2
    delivery = deliveries.json()["items"][0]

    callback = client.post(
        "/api/v1/activation/callbacks/simulator",
        json={
            "callbacks": [
                {
                    "workflow_id": workflow_id,
                    "cohort_id": cohort_id,
                    "delivery_id": delivery["delivery_id"],
                    "action_execution_id": delivery["action_execution_id"],
                    "user_id": delivery["user_id"],
                    "occurred_at": "2026-04-03T11:00:00",
                    "event_id": "evt_bq_churn_returned",
                    "event_type": "returned",
                }
            ]
        },
    )
    assert callback.status_code == 200
    assert callback.json()["outcomes_ingested"] == 1

    summary = client.get("/api/v1/experiments/bigquery_churn_list_exp/summary")
    assert summary.status_code == 200
    assert summary.json()["sample_size"] >= 1
    assert summary.json()["decision"] in {"winner", "neutral", "inconclusive", "invalid"}


def test_bigquery_import_validation_and_missing_table_failure(client: TestClient):
    _create_bigquery_connector(client, name="Warehouse Errors", mock_tables={"prediction_scores": []})

    invalid_kind = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Warehouse Errors",
            "table_name": "prediction_scores",
            "resource_kind": "unknown",
            "column_mapping": {"canonical_user_id": "player_id"},
        },
    )
    assert invalid_kind.status_code == 409

    missing_score_mapping = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Warehouse Errors",
            "table_name": "prediction_scores",
            "resource_kind": "external_prediction_scores",
            "column_mapping": {"canonical_user_id": "player_id"},
        },
    )
    assert missing_score_mapping.status_code == 409

    created = client.post(
        "/api/v1/imports",
        json={
            "source_name": "Warehouse Errors",
            "table_name": "missing_table",
            "resource_kind": "churn_list",
            "column_mapping": {"canonical_user_id": "player_id"},
        },
    )
    assert created.status_code == 201
    failed = client.post(f"/api/v1/imports/{created.json()['id']}/run")
    assert failed.status_code == 500
    assert "missing_table" in failed.json()["detail"]
