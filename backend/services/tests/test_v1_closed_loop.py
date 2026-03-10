from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from app.core import db as db_module
from app.main import create_app
from bigquery_service import clear_shared_bigquery_service_cache, get_shared_bigquery_service


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    clear_shared_bigquery_service_cache()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def _seed_mock_warehouse():
    service = get_shared_bigquery_service()
    service.write_events_staging(
        [
            {
                "job_id": "job_1",
                "source": "adjust",
                "player_id": "u_1",
                "canonical_user_id": "u_1",
                "event_type": "promo_view",
                "event_time": "2026-03-08T10:00:00",
                "event_properties": {"country": "US", "platform": "ios", "campaign": "winback_a"},
                "user_properties": {"email": "u1@example.com"},
            },
            {
                "job_id": "job_1",
                "source": "adjust",
                "player_id": "u_2",
                "canonical_user_id": "u_2",
                "event_type": "item_purchased",
                "event_time": "2026-03-08T11:00:00",
                "event_properties": {"country": "US", "platform": "android", "campaign": "winback_b", "revenue_usd": 9.99},
                "user_properties": {"email": "u2@example.com"},
            },
            {
                "job_id": "job_1",
                "source": "adjust",
                "player_id": "u_3",
                "canonical_user_id": "u_3",
                "event_type": "session_start",
                "event_time": "2026-03-08T12:00:00",
                "event_properties": {"country": "CA", "platform": "ios", "campaign": "winback_a"},
                "user_properties": {"email": "u3@example.com"},
            },
        ],
        job_id="job_1",
    )
    service.run_events_curation(job_id="job_1")
    service.refresh_player_latest_state(job_id="job_1")
    service.append_prediction_results(
        "pred_job_1",
        [
            {
                "prediction_job_id": "pred_job_1",
                "user_id": "u_1",
                "canonical_user_id": "u_1",
                "email": "u1@example.com",
                "churn_state": "active",
                "predicted_churn_risk": "high",
                "prediction_source": "local",
                "suggested_action": "push_notification",
                "completed_at": "2026-03-09T10:00:00",
            },
            {
                "prediction_job_id": "pred_job_1",
                "user_id": "u_2",
                "canonical_user_id": "u_2",
                "email": "u2@example.com",
                "churn_state": "active",
                "predicted_churn_risk": "medium",
                "prediction_source": "local",
                "suggested_action": "email",
                "completed_at": "2026-03-09T10:00:00",
            },
            {
                "prediction_job_id": "pred_job_1",
                "user_id": "u_3",
                "canonical_user_id": "u_3",
                "email": "u3@example.com",
                "churn_state": "churned",
                "predicted_churn_risk": "already_churned",
                "prediction_source": "local",
                "suggested_action": "none",
                "completed_at": "2026-03-09T10:00:00",
            },
        ],
    )


def test_mapping_versioning_and_rollback(client):
    save_resp = client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={
            "mapping": {"canonical_user_id": "event_properties.player_id", "event_time": "timestamp"},
            "scope_type": "source",
            "changed_by": "tester",
        },
    )
    assert save_resp.status_code == 200
    assert save_resp.json()["required_coverage"] >= 66.0

    save_resp_2 = client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={
            "mapping": {"canonical_user_id": "player_id", "event_name": "event_name", "event_time": "timestamp"},
            "scope_type": "source",
            "changed_by": "tester",
        },
    )
    assert save_resp_2.status_code == 200
    assert save_resp_2.json()["required_coverage"] == 100.0

    versions = client.get("/api/v1/mappings/Adjust%20Source/versions")
    assert versions.status_code == 200
    assert len(versions.json()["items"]) == 2

    rollback = client.post("/api/v1/mappings/Adjust%20Source/rollback/1")
    assert rollback.status_code == 200
    assert rollback.json()["mapping"]["canonical_user_id"] == "event_properties.player_id"


def test_closed_loop_sql_cohort_workflow_experiment_and_copilot(client):
    _seed_mock_warehouse()

    health = client.get("/api/v1/health")
    assert health.status_code == 200
    assert health.json()["data_aliases"]["fact_events_unified"] == "events_curated"

    preview = client.post(
        "/api/v1/sql-workspace/preview",
        json={
            "sql": "SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'",
            "limit": 10,
        },
    )
    assert preview.status_code == 200
    assert preview.json()["row_count"] == 1

    saved = client.post(
        "/api/v1/sql-workspace/queries",
        json={
            "name": "High risk users",
            "sql": "SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'",
            "description": "High risk churn rescue audience",
        },
    )
    assert saved.status_code == 201
    query_id = saved.json()["query_id"]

    cohort = client.post(
        f"/api/v1/sql-workspace/queries/{query_id}/cohort",
        json={"name": "churn_rescue_high_risk", "refresh_mode": "daily", "activate": False},
    )
    assert cohort.status_code == 201
    cohort_id = cohort.json()["cohort_id"]
    assert cohort.json()["member_count"] == 1

    activation = client.post(f"/api/v1/cohorts/{cohort_id}/activate")
    assert activation.status_code == 200
    assert activation.json()["status"] == "active"

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=churn_rescue_v1",
        json={
            "enabled": True,
            "primary_metric": "return_rate",
            "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
            "min_sample_size": 1,
            "min_runtime_hours": 0,
            "cohort_id": cohort_id,
            "holdout_pct": 0.0,
            "b_variant_pct": 0.5,
        },
    )
    assert experiment.status_code == 200

    workflow = client.post(
        "/api/v1/workflows",
        json={
            "name": "daily_churn_rescue",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily"},
            "action": {"channel": "push_notification", "content": "Come back for a reward"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
            "experiment_id": "churn_rescue_v1",
            "requires_confirmation": False,
        },
    )
    assert workflow.status_code == 201
    workflow_id = workflow.json()["workflow_id"]

    publish = client.post(f"/api/v1/workflows/{workflow_id}/publish")
    assert publish.status_code == 200
    assert publish.json()["status"] == "published"

    run = client.post(f"/api/v1/workflows/{workflow_id}/test-run", json={"limit": 5, "confirm": True, "sandbox": True})
    assert run.status_code == 200
    assert run.json()["triggered"] == 1
    assert run.json()["success"] == 1

    executions = client.get(f"/api/v1/workflows/{workflow_id}/executions")
    assert executions.status_code == 200
    assert len(executions.json()["items"]) >= 1

    summary = client.get("/api/v1/experiments/churn_rescue_v1/summary")
    assert summary.status_code == 200
    assert summary.json()["decision"] in {"winner", "neutral", "inconclusive", "invalid"}
    assert summary.json()["total_exposures"] >= 1

    exposures = client.get("/api/v1/experiments/churn_rescue_v1/exposures")
    assert exposures.status_code == 200
    assert len(exposures.json()["items"]) >= 1

    outcomes = client.get("/api/v1/experiments/churn_rescue_v1/outcomes")
    assert outcomes.status_code == 200
    assert len(outcomes.json()["items"]) >= 1

    decision = client.post("/api/v1/experiments/churn_rescue_v1/decision", json={"decided_by": "tester"})
    assert decision.status_code == 200
    assert decision.json()["next_step"]

    copilot_query = client.post("/api/v1/copilot/query", json={"question": "how many high risk users do we have in 7d?"})
    assert copilot_query.status_code == 200
    assert "conclusion" in copilot_query.json()

    copilot_explain = client.post("/api/v1/copilot/explain", json={"metric_id": "active_users", "time_window": "7d"})
    assert copilot_explain.status_code == 200
    assert "key_evidence" in copilot_explain.json()

    copilot_recommend = client.post("/api/v1/copilot/recommend", json={"metric_context": {"metric_id": "high_risk_users"}})
    assert copilot_recommend.status_code == 200
    assert copilot_recommend.json()["suggested_action"]["cohort_draft"]["member_count"] >= 0

    copilot_report = client.post("/api/v1/copilot/report", json={"report_type": "daily", "time_window": "7d"})
    assert copilot_report.status_code == 200
    assert copilot_report.json()["methodology"]["report_type"] == "daily"
