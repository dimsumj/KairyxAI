from __future__ import annotations

from fastapi.testclient import TestClient
import pytest
import requests

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


def _seed_prediction_job(prediction_job_id: str = "pred_job_1") -> None:
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        if repository.get_prediction_job(prediction_job_id) is None:
            repository.create_prediction_job(
                {
                    "id": prediction_job_id,
                    "import_job_id": "imp_seeded",
                    "status": "completed",
                    "spec": {"prediction_mode": "local"},
                    "progress": {"current": 3, "total": 3, "pct": 100.0, "details": {}},
                }
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
    _seed_prediction_job()

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

    pre_exposures = client.get("/api/v1/experiments/churn_rescue_v1/exposures")
    assert pre_exposures.status_code == 200
    assert pre_exposures.json()["items"] == []

    scheduled = client.post(
        "/api/v1/orchestrator/run-due",
        json={"reference_time": "2026-03-10T10:00:00", "limit_per_workflow": 5},
    )
    assert scheduled.status_code == 200
    assert len(scheduled.json()["items"]) == 1
    assert scheduled.json()["items"][0]["success"] == 1

    duplicate_run = client.post(
        "/api/v1/orchestrator/run-due",
        json={"reference_time": "2026-03-10T12:00:00", "limit_per_workflow": 5},
    )
    assert duplicate_run.status_code == 200
    assert duplicate_run.json()["items"] == []

    executions = client.get(f"/api/v1/workflows/{workflow_id}/executions")
    assert executions.status_code == 200
    assert len(executions.json()["items"]) >= 1

    deliveries = client.get(f"/api/v1/workflows/{workflow_id}/deliveries")
    assert deliveries.status_code == 200
    assert len(deliveries.json()["items"]) >= 2
    scheduled_delivery = next(item for item in deliveries.json()["items"] if not item.get("sandbox"))

    exposures = client.get("/api/v1/experiments/churn_rescue_v1/exposures")
    assert exposures.status_code == 200
    assert len(exposures.json()["items"]) >= 1

    callback = client.post(
        "/api/v1/activation/callbacks/simulator",
        json={
            "callbacks": [
                {
                    "workflow_id": workflow_id,
                    "cohort_id": cohort_id,
                    "delivery_id": scheduled_delivery["delivery_id"],
                    "action_execution_id": scheduled_delivery["action_execution_id"],
                    "user_id": scheduled_delivery["user_id"],
                    "occurred_at": "2026-03-10T11:00:00",
                    "event_id": "evt_delivery_returned_1",
                    "event_type": "returned",
                    "metadata": {"channel": "push_notification"},
                }
            ]
        },
    )
    assert callback.status_code == 200
    assert callback.json()["ingested"] == 1
    assert callback.json()["outcomes_ingested"] == 1

    duplicate_callback = client.post(
        "/api/v1/activation/callbacks/simulator",
        json={
            "callbacks": [
                {
                    "delivery_id": scheduled_delivery["delivery_id"],
                    "action_execution_id": scheduled_delivery["action_execution_id"],
                    "user_id": scheduled_delivery["user_id"],
                    "occurred_at": "2026-03-10T11:00:00",
                    "event_id": "evt_delivery_returned_1",
                    "event_type": "returned",
                }
            ]
        },
    )
    assert duplicate_callback.status_code == 200
    assert duplicate_callback.json()["duplicates"] == 1

    outcomes = client.get("/api/v1/experiments/churn_rescue_v1/outcomes")
    assert outcomes.status_code == 200
    assert len(outcomes.json()["items"]) >= 1

    summary = client.get("/api/v1/experiments/churn_rescue_v1/summary")
    assert summary.status_code == 200
    assert summary.json()["decision"] in {"winner", "neutral", "inconclusive", "invalid"}
    assert summary.json()["sample_size"] >= 1
    assert summary.json()["srm_status"] in {"ok", "detected"}
    assert summary.json()["decision_reason"]

    decision = client.post("/api/v1/experiments/churn_rescue_v1/decision", json={"decided_by": "tester"})
    assert decision.status_code == 200
    assert decision.json()["next_step"]
    assert decision.json()["decision_reason"]

    counters = client.get(f"/api/v1/workflows/{workflow_id}/policy-counters")
    assert counters.status_code == 200
    assert counters.json()["policy_state"]
    assert counters.json()["budget_state"][0]["consumed"] >= 1

    cohort_metrics = client.get(f"/api/v1/cohorts/{cohort_id}/metrics")
    assert cohort_metrics.status_code == 200
    assert cohort_metrics.json()["member_count"] == 1
    assert cohort_metrics.json()["delivered_users"] == 1
    assert cohort_metrics.json()["conversion_users"] == 1

    compare_versions = client.get(f"/api/v1/cohorts/{cohort_id}/compare?base_version=1&target_version=1")
    assert compare_versions.status_code == 200
    assert compare_versions.json()["base_member_count"] == compare_versions.json()["target_member_count"] == 1

    copilot_query = client.post("/api/v1/copilot/query", json={"question": "how many high risk users do we have in 7d?"})
    assert copilot_query.status_code == 200
    assert "conclusion" in copilot_query.json()
    assert "evidence" in copilot_query.json()
    assert "recommended_action" in copilot_query.json()
    assert copilot_query.json()["query_id"]
    assert copilot_query.json()["audit_id"]

    query_log = client.get(f"/api/v1/copilot/query-logs/{copilot_query.json()['query_id']}", headers={"x-actor-role": "analyst"})
    assert query_log.status_code == 200
    assert query_log.json()["query_id"] == copilot_query.json()["query_id"]

    copilot_explain = client.post(
        "/api/v1/copilot/explain",
        json={"metric_id": "promo_views", "time_window": "7d", "dimensions": ["campaign", "country", "platform"]},
    )
    assert copilot_explain.status_code == 200
    assert "key_evidence" in copilot_explain.json()
    assert copilot_explain.json()["metric_window"] == "7d"
    assert copilot_explain.json()["anomaly_id"]

    anomalies = client.get("/api/v1/copilot/anomalies", headers={"x-actor-role": "analyst"})
    assert anomalies.status_code == 200
    assert len(anomalies.json()["items"]) >= 1

    copilot_recommend = client.post("/api/v1/copilot/recommend", json={"metric_context": {"metric_id": "high_risk_users"}})
    assert copilot_recommend.status_code == 200
    assert copilot_recommend.json()["recommended_action"]["cohort_draft"]["member_count"] >= 0

    copilot_report = client.post("/api/v1/copilot/report", json={"report_type": "daily", "time_window": "7d"})
    assert copilot_report.status_code == 200
    assert copilot_report.json()["methodology"]["report_type"] == "daily"
    assert len(copilot_report.json()["evidence"]) == 3
    assert copilot_report.json()["report_id"]

    reports = client.get("/api/v1/copilot/reports", headers={"x-actor-role": "analyst"})
    assert reports.status_code == 200
    assert len(reports.json()["items"]) >= 1

    health_after = client.get("/api/v1/health")
    assert health_after.status_code == 200
    assert "operational_metrics" in health_after.json()


def test_import_quality_resume_and_replay(client):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={
            "name": "Adjust Source",
            "type": "adjust",
            "config": {"api_token": "adjust-token"},
        },
    )
    assert connector_resp.status_code == 201

    mapping_resp = client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={"mapping": {"canonical_user_id": "player_id", "event_time": "timestamp"}},
    )
    assert mapping_resp.status_code == 200
    assert mapping_resp.json()["required_coverage"] < 95.0

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

    blocked = client.post(import_job["links"]["self"] + "/run")
    assert blocked.status_code == 200
    assert blocked.json()["status"] == "awaiting_mapping"

    quality = client.get(import_job["links"]["self"] + "/quality")
    assert quality.status_code == 200
    assert quality.json()["mapping_coverage"] < 95.0
    assert quality.json()["checkpoint_state"]["total"] == 0
    assert quality.json()["audit_id"]

    mapping_fix = client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={"mapping": {"canonical_user_id": "player_id", "event_name": "event_name", "event_time": "timestamp"}},
    )
    assert mapping_fix.status_code == 200
    assert mapping_fix.json()["required_coverage"] == 100.0

    resumed = client.post(import_job["links"]["self"] + "/resume")
    assert resumed.status_code == 200
    assert resumed.json()["status"] == "completed"
    assert resumed.json()["quality_report"]["required_mapping_coverage"] == 100.0
    assert resumed.json()["audit_id"]

    quality_after_resume = client.get(import_job["links"]["self"] + "/quality")
    assert quality_after_resume.status_code == 200
    assert quality_after_resume.json()["identity_summary"]["source_of_truth_matrix"]
    assert quality_after_resume.json()["quality_report"]["canonical_user_id_coverage"] >= 90.0

    service = get_shared_bigquery_service()
    service.write_pipeline_dead_letters(
        [
            {
                "job_id": import_job["id"],
                "normalized_event": {
                    "job_id": import_job["id"],
                    "job_identifier": import_job["id"],
                    "source": "adjust",
                    "player_id": "replay_u_1",
                    "canonical_user_id": "replay_u_1",
                    "event_type": "session_start",
                    "event_time": "2026-03-10T10:30:00",
                    "event_fingerprint": "fp_replay_u_1",
                    "data_quality_flags": [],
                },
            }
        ],
        job_id=import_job["id"],
    )
    replay = client.post(import_job["links"]["self"] + "/replay", headers={"x-actor-role": "operator"})
    assert replay.status_code == 200
    assert replay.json()["replayed_rows"] == 1
    assert replay.json()["audit_id"]

    replay_denied = client.post(import_job["links"]["self"] + "/replay", headers={"x-actor-role": "analyst"})
    assert replay_denied.status_code == 403

    delete_denied = client.delete(import_job["links"]["self"], headers={"x-actor-role": "operator"})
    assert delete_denied.status_code == 403


def test_export_diagnostics_retry_and_rbac(client, monkeypatch):
    _seed_mock_warehouse()
    _seed_prediction_job()

    create_denied = client.post(
        "/api/v1/exports",
        json={
            "prediction_job_id": "pred_job_1",
            "provider": "webhook",
            "channel": "email",
            "audience_name": "churn_rescue",
            "webhook_url": "https://example.test/export",
        },
        headers={"x-actor-role": "analyst"},
    )
    assert create_denied.status_code == 403

    created = client.post(
        "/api/v1/exports",
        json={
            "prediction_job_id": "pred_job_1",
            "provider": "webhook",
            "channel": "email",
            "audience_name": "churn_rescue",
            "webhook_url": "https://example.test/export",
        },
        headers={"x-actor-role": "operator"},
    )
    assert created.status_code == 201
    export_job_id = created.json()["id"]

    class FakeResponse:
        def __init__(self, status_code: int, text: str = "ok"):
            self.status_code = status_code
            self.text = text

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(self.text)

    monkeypatch.setattr("app.application.exports.requests.post", lambda *args, **kwargs: FakeResponse(500, "webhook failed"))
    failed_run = client.post(f"/api/v1/exports/{export_job_id}/run", headers={"x-actor-role": "operator"})
    assert failed_run.status_code == 200
    assert failed_run.json()["status"] == "failed"

    diagnostics = client.get(f"/api/v1/exports/{export_job_id}/diagnostics", headers={"x-actor-role": "operator"})
    assert diagnostics.status_code == 200
    assert diagnostics.json()["items"][0]["status"] == "failed"

    monkeypatch.setattr("app.application.exports.requests.post", lambda *args, **kwargs: FakeResponse(202, "accepted"))
    retried = client.post(f"/api/v1/exports/{export_job_id}/retry", headers={"x-actor-role": "operator"})
    assert retried.status_code == 200
    assert retried.json()["status"] == "completed"

    diagnostics_after_retry = client.get(f"/api/v1/exports/{export_job_id}/diagnostics", headers={"x-actor-role": "operator"})
    assert diagnostics_after_retry.status_code == 200
    assert len(diagnostics_after_retry.json()["items"]) == 2
    assert any(item["status"] == "completed" for item in diagnostics_after_retry.json()["items"])

    query_denied = client.post(
        "/api/v1/copilot/query",
        json={"question": "how many high risk users do we have?"},
        headers={"x-actor-role": "operator"},
    )
    assert query_denied.status_code == 403


def test_cohort_lifecycle_and_failed_daily_refresh_auto_pause(client, monkeypatch):
    _seed_mock_warehouse()

    cohort = client.post(
        "/api/v1/cohorts",
        json={
            "name": "phase2_daily_cohort",
            "type": "sql",
            "definition": {"sql": "SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'"},
            "refresh_mode": "daily",
        },
    )
    assert cohort.status_code == 201
    cohort_id = cohort.json()["cohort_id"]

    versions = client.get(f"/api/v1/cohorts/{cohort_id}/versions")
    assert versions.status_code == 200
    assert len(versions.json()["items"]) == 1

    rollback = client.post(f"/api/v1/cohorts/{cohort_id}/rollback?version=1")
    assert rollback.status_code == 200
    assert rollback.json()["version_id"] == 2

    archived = client.post(f"/api/v1/cohorts/{cohort_id}/archive")
    assert archived.status_code == 200
    assert archived.json()["status"] == "archived"

    restored = client.post(f"/api/v1/cohorts/{cohort_id}/restore")
    assert restored.status_code == 200
    assert restored.json()["status"] == "draft"

    service = get_shared_bigquery_service()

    def fail_query(*args, **kwargs):
        raise RuntimeError("sql workspace unavailable")

    monkeypatch.setattr(service, "run_readonly_query", fail_query)

    first_refresh = client.post(f"/api/v1/cohorts/{cohort_id}/refresh")
    assert first_refresh.status_code == 409

    second_refresh = client.post(f"/api/v1/cohorts/{cohort_id}/refresh")
    assert second_refresh.status_code == 409

    final_state = client.get(f"/api/v1/cohorts/{cohort_id}")
    assert final_state.status_code == 200
    assert final_state.json()["status"] == "paused"
