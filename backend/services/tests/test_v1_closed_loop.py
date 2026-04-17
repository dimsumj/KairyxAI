from __future__ import annotations

from datetime import datetime

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
    monkeypatch.setenv("SCHEDULER_ENABLED", "false")
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
    report_id = copilot_report.json()["report_id"]

    report_detail = client.get(f"/api/v1/copilot/reports/{report_id}", headers={"x-actor-role": "analyst"})
    assert report_detail.status_code == 200
    assert report_detail.json()["run_count"] >= 1
    assert any(item["resource_type"] == "copilot_query_log" for item in report_detail.json()["linked_resources"])

    report_runs = client.get(f"/api/v1/copilot/reports/{report_id}/runs", headers={"x-actor-role": "analyst"})
    assert report_runs.status_code == 200
    assert len(report_runs.json()["items"]) >= 1

    report_review = client.post(
        f"/api/v1/copilot/reports/{report_id}/review",
        json={"disposition": "acknowledged", "notes": "Reviewed for operator handoff"},
        headers={"x-actor-role": "analyst", "x-actor-id": "qa_reviewer"},
    )
    assert report_review.status_code == 200
    assert report_review.json()["review"]["status"] == "acknowledged"

    reports = client.get("/api/v1/copilot/reports", headers={"x-actor-role": "analyst"})
    assert reports.status_code == 200
    assert len(reports.json()["items"]) >= 1
    assert len({item["report_id"] for item in reports.json()["items"]}) == len(reports.json()["items"])

    copilot_overview = client.get("/api/v1/copilot/overview", headers={"x-actor-role": "analyst"})
    assert copilot_overview.status_code == 200
    assert copilot_overview.json()["report_counts"]["total"] >= 1

    health_after = client.get("/api/v1/health")
    assert health_after.status_code == 200
    assert "operational_metrics" in health_after.json()

    cohort_overview = client.get(f"/api/v1/cohorts/{cohort_id}/overview", headers={"x-actor-role": "analyst"})
    assert cohort_overview.status_code == 200
    assert cohort_overview.json()["metrics"]["measurement_state"]["delivery_signal_status"] == "ready"
    assert cohort_overview.json()["linked_workflows"][0]["workflow_id"] == workflow_id


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


def test_data_core_identity_conflicts_and_sql_guardrails(client):
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.create_import_job(
            {
                "id": "imp_identity_1",
                "source_name": "seeded",
                "status": "completed",
                "spec": {"source_name": "seeded"},
                "progress": {"current": 0, "total": 0, "pct": 100.0, "details": {}},
            }
        )

    service = get_shared_bigquery_service()
    service.write_events_staging(
        [
            {
                "job_id": "imp_identity_1",
                "source": "analytics_sdk",
                "player_id": "p_1",
                "event_type": "promo_view",
                "event_time": "2026-03-08T08:00:00",
                "event_properties": {"campaign": "spring_a", "media_source": "sdk", "platform": "ios", "country": "US"},
                "user_properties": {"email": "p1@example.com"},
            },
            {
                "job_id": "imp_identity_1",
                "source": "adjust",
                "player_id": "p_1",
                "event_type": "promo_view",
                "event_time": "2026-03-08T08:05:00",
                "event_properties": {"campaign": "spring_b", "media_source": "mmp", "platform": "ios", "country": "US"},
                "user_properties": {"email": "p1@example.com"},
            },
        ],
        job_id="imp_identity_1",
    )
    service.run_events_curation(job_id="imp_identity_1")
    service.refresh_player_latest_state(job_id="imp_identity_1")
    service.write_pipeline_dead_letters(
        [
            {
                "job_id": "imp_identity_1",
                "rejection_reason": "missing_player_id",
                "normalized_event": {
                    "job_id": "imp_identity_1",
                    "event_type": "session_start",
                    "event_time": "2026-03-08T09:00:00",
                    "data_quality_flags": ["missing_player_id"],
                    "event_fingerprint": "dead_1",
                },
            }
        ],
        job_id="imp_identity_1",
    )

    quality = client.get("/api/v1/imports/imp_identity_1/quality", headers={"x-actor-role": "analyst"})
    assert quality.status_code == 200
    assert quality.json()["quality_report"]["top20_field_coverage"]["fields"]["campaign"]["coverage"] >= 50.0
    assert quality.json()["source_of_truth"]
    assert quality.json()["conflict_summary"]["count"] >= 1

    identity_links = client.get("/api/v1/imports/imp_identity_1/identity-links", headers={"x-actor-role": "analyst"})
    assert identity_links.status_code == 200
    assert identity_links.json()["items"]

    conflicts = client.get("/api/v1/imports/imp_identity_1/conflicts", headers={"x-actor-role": "analyst"})
    assert conflicts.status_code == 200
    assert conflicts.json()["items"][0]["field"] in {"campaign", "media_source", "channel", "adset"}

    rejected = client.get("/api/v1/imports/imp_identity_1/rejected", headers={"x-actor-role": "operator"})
    assert rejected.status_code == 200
    assert rejected.json()["items"][0]["reason"] == "missing_player_id"

    suggestions = client.get("/api/v1/mappings/seeded/suggestions", headers={"x-actor-role": "operator"})
    assert suggestions.status_code == 200
    assert suggestions.json()["suggestions"]

    preview_blocked = client.post(
        "/api/v1/sql-workspace/preview",
        json={"sql": "DELETE FROM prediction_results", "limit": 10},
        headers={"x-actor-role": "analyst"},
    )
    assert preview_blocked.status_code == 400

    preview_scan_blocked = client.post(
        "/api/v1/sql-workspace/preview",
        json={"sql": "SELECT * FROM fact_events_unified", "limit": 10, "scan_limit_rows": 1},
        headers={"x-actor-role": "analyst"},
    )
    assert preview_scan_blocked.status_code == 400


def test_import_operations_and_backfill_control_plane(client):
    service = get_shared_bigquery_service()

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.upsert_connector("Adjust Source", "adjust", {"api_token": "adjust-token"})
        repository.create_import_job(
            {
                "id": "imp_ops_1",
                "source_name": "Adjust Source",
                "status": "completed",
                "spec": {"source_name": "Adjust Source", "start_date": "20260301", "end_date": "20260302"},
                "progress": {
                    "current": 1,
                    "total": 1,
                    "pct": 100.0,
                    "details": {
                        "mapping_coverage": 100.0,
                        "checkpoint_state": {"total": 1, "processed": 1, "pending": 0, "counts": {"processed": 1}},
                    },
                },
            }
        )
        repository.upsert_checkpoint(
            {
                "job_id": "imp_ops_1",
                "shard_index": 0,
                "source_name": "Adjust Source",
                "status": "processed",
                "cursor": "0",
                "gcs_uri": "gs://mock/raw/source=adjust/job=imp_ops_1/part-000001.jsonl.gz",
                "message_id": "msg-1",
                "manifest": {"job_id": "imp_ops_1", "source": "adjust", "shard_index": 0},
                "event_count": 1,
            }
        )
        repository.create_import_job(
            {
                "id": "imp_ops_2",
                "source_name": "Adjust Source",
                "status": "awaiting_mapping",
                "spec": {"source_name": "Adjust Source", "start_date": "20260301", "end_date": "20260303"},
                "progress": {
                    "current": 0,
                    "total": 0,
                    "pct": 0.0,
                    "details": {
                        "mapping_coverage": 60.0,
                        "checkpoint_state": {"total": 0, "processed": 0, "pending": 0, "counts": {}},
                    },
                },
            }
        )

    service.write_events_staging(
        [
            {
                "job_id": "imp_ops_1",
                "source": "adjust",
                "player_id": "u_ops_1",
                "canonical_user_id": "u_ops_1",
                "event_type": "session_start",
                "event_time": "2026-03-08T10:00:00",
                "event_fingerprint": "fp_existing",
                "event_properties": {"campaign": "ops_campaign"},
                "user_properties": {"email": "ops1@example.com"},
            }
        ],
        job_id="imp_ops_1",
    )
    service.run_events_curation(job_id="imp_ops_1")
    service.refresh_player_latest_state(job_id="imp_ops_1")
    service.write_pipeline_dead_letters(
        [
            {
                "job_id": "imp_ops_1",
                "rejection_reason": "missing_campaign_mapping",
                "normalized_event": {
                    "job_id": "imp_ops_1",
                    "source": "adjust",
                    "player_id": "u_ops_1",
                    "canonical_user_id": "u_ops_1",
                    "event_type": "promo_view",
                    "event_time": "2026-03-08T11:00:00",
                    "event_fingerprint": "fp_replay_1",
                    "data_quality_flags": [],
                    "event_properties": {"campaign": "ops_campaign"},
                    "user_properties": {"email": "ops1@example.com"},
                },
            }
        ],
        job_id="imp_ops_1",
    )

    operations = client.get("/api/v1/imports/imp_ops_1/operations", headers={"x-actor-role": "analyst"})
    assert operations.status_code == 200
    assert operations.json()["processing_contract"]["mode"] == "manifest-driven"
    assert operations.json()["dead_letters"]["count"] == 1
    assert operations.json()["dead_letters"]["replayable_count"] == 1
    assert operations.json()["remediation"]["recommended_action"] == "replay_rejected_rows"
    assert operations.json()["schema_contracts"]

    manifests = client.get("/api/v1/imports/imp_ops_1/manifests", headers={"x-actor-role": "analyst"})
    assert manifests.status_code == 200
    assert manifests.json()["items"][0]["manifest_id"] == "imp_ops_1:0"

    schema_contract = client.get("/api/v1/imports/schema-contracts/standardized", headers={"x-actor-role": "analyst"})
    assert schema_contract.status_code == 200
    assert schema_contract.json()["alias"] == "standardized"
    assert "player_id" in schema_contract.json()["required_fields"]

    schema_contracts = client.get("/api/v1/imports/schema-contracts", headers={"x-actor-role": "analyst"})
    assert schema_contracts.status_code == 200
    assert any(item["alias"] == "fact_events_unified" for item in schema_contracts.json()["items"])

    backfill = client.post(
        "/api/v1/imports/backfills",
        headers={"x-actor-role": "operator"},
        json={"source_name": "Adjust Source", "start_date": "20260301", "end_date": "20260303"},
    )
    assert backfill.status_code == 200
    assert backfill.json()["matched_jobs"] == 2
    assert any(item["job_id"] == "imp_ops_1" and item["status"] == "completed" for item in backfill.json()["items"])
    assert any(item["job_id"] == "imp_ops_2" and item["status"] == "blocked" for item in backfill.json()["items"])

    listed = client.get("/api/v1/imports/backfills", headers={"x-actor-role": "analyst"})
    assert listed.status_code == 200
    assert any(item["backfill_id"] == backfill.json()["backfill_id"] for item in listed.json()["items"])

    fetched = client.get(f"/api/v1/imports/backfills/{backfill.json()['backfill_id']}", headers={"x-actor-role": "analyst"})
    assert fetched.status_code == 200
    assert fetched.json()["backfill_id"] == backfill.json()["backfill_id"]
    assert fetched.json()["completed_jobs"] == 1


def test_health_snapshot_includes_data_core_lag_alerts(client):
    service = get_shared_bigquery_service()

    service.write_events_staging(
        [
            {
                "job_id": "lag_seed",
                "source": "adjust",
                "player_id": "lag_user",
                "canonical_user_id": "lag_user",
                "event_type": "session_start",
                "event_time": "2026-03-08T10:00:00",
                "event_properties": {"platform": "ios"},
                "user_properties": {},
            }
        ],
        job_id="lag_seed",
    )
    service.run_events_curation(job_id="lag_seed")
    service.refresh_player_latest_state(job_id="lag_seed")

    service.write_events_staging(
        [
            {
                "job_id": "lag_curated",
                "source": "adjust",
                "player_id": "lag_user",
                "canonical_user_id": "lag_user",
                "event_type": "promo_view",
                "event_time": "2026-03-09T10:00:00",
                "event_properties": {"platform": "ios"},
                "user_properties": {},
            }
        ],
        job_id="lag_curated",
    )
    service.run_events_curation(job_id="lag_curated")

    service.write_events_staging(
        [
            {
                "job_id": "lag_pending",
                "source": "adjust",
                "player_id": "lag_user",
                "canonical_user_id": "lag_user",
                "event_type": "item_purchased",
                "event_time": "2026-03-10T10:00:00",
                "event_properties": {"platform": "ios", "revenue_usd": 1.99},
                "user_properties": {},
                "unexpected_field": "schema_drift",
            }
        ],
        job_id="lag_pending",
    )

    health = client.get("/api/v1/health")
    assert health.status_code == 200
    assert health.json()["operational_metrics"]["staging_to_curated_lag_seconds"] > 0
    assert health.json()["operational_metrics"]["aggregate_refresh_lag_seconds"] > 0
    assert health.json()["operational_metrics"]["schema_drift_count"] > 0
    alert_codes = {item["code"] for item in health.json()["alerts"]}
    assert "curation_lag_present" in alert_codes
    assert "aggregate_refresh_lag_present" in alert_codes
    assert "schema_drift_present" in alert_codes


def test_workflow_event_threshold_confirmation_and_experiment_extensions(client):
    _seed_mock_warehouse()
    _seed_prediction_job()

    cohort = client.post(
        "/api/v1/cohorts",
        json={
            "name": "event_trigger_cohort",
            "type": "list",
            "definition": {"members": [{"canonical_user_id": "u_1", "email": "u1@example.com"}]},
            "refresh_mode": "manual",
            "activate": True,
        },
    )
    assert cohort.status_code == 201
    cohort_id = cohort.json()["cohort_id"]

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=event_trigger_exp",
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

    event_workflow = client.post(
        "/api/v1/workflows",
        json={
            "name": "event_trigger_workflow",
            "cohort_id": cohort_id,
            "trigger": {"type": "event_trigger", "event_type": "promo_view"},
            "action": {"channel": "push_notification", "content": "Event based winback"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
            "experiment_id": "event_trigger_exp",
            "requires_confirmation": True,
        },
    )
    assert event_workflow.status_code == 201
    workflow_id = event_workflow.json()["workflow_id"]

    updated = client.put(
        f"/api/v1/workflows/{workflow_id}",
        json={"action": {"channel": "push_notification", "content": "Updated event winback"}},
    )
    assert updated.status_code == 200
    assert updated.json()["current_version"] == 2

    versions = client.get(f"/api/v1/workflows/{workflow_id}/versions")
    assert versions.status_code == 200
    assert len(versions.json()["items"]) >= 2

    publish = client.post(f"/api/v1/workflows/{workflow_id}/publish")
    assert publish.status_code == 200

    no_token = client.post(
        "/api/v1/orchestrator/events:ingest",
        json={"event_type": "promo_view", "user_ids": ["u_1"]},
    )
    assert no_token.status_code == 409

    confirmation = client.post(f"/api/v1/workflows/{workflow_id}/confirm", json={"note": "approve high risk", "valid_for_hours": 24})
    assert confirmation.status_code == 200
    token = confirmation.json()["confirmation_token"]

    event_run = client.post(
        "/api/v1/orchestrator/events:ingest",
        json={"event_type": "promo_view", "user_ids": ["u_1"], "confirmation_tokens": {workflow_id: token}},
    )
    assert event_run.status_code == 200
    assert len(event_run.json()["items"]) == 1
    assert event_run.json()["items"][0]["success"] == 1

    assignments = client.get("/api/v1/experiments/event_trigger_exp/assignments")
    assert assignments.status_code == 200
    assert assignments.json()["items"][0]["group"] in {"treatment_a", "treatment_b", "holdout"}

    rollout = client.get("/api/v1/experiments/event_trigger_exp/rollout-suggestion")
    assert rollout.status_code == 200
    assert rollout.json()["suggestion"]

    threshold_workflow = client.post(
        "/api/v1/workflows",
        json={
            "name": "threshold_trigger_workflow",
            "cohort_id": cohort_id,
            "trigger": {"type": "threshold_trigger", "metric_id": "high_risk_users", "operator": ">=", "threshold": 1},
            "action": {
                "channel": "webhook",
                "content": "Threshold fallback",
                "webhook_url": "http://127.0.0.1:9/unreachable",
                "retry_policy": {"max_retries": 2, "base_backoff_seconds": 1},
            },
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
            "experiment_id": "event_trigger_exp",
            "requires_confirmation": False,
        },
    )
    assert threshold_workflow.status_code == 201
    threshold_workflow_id = threshold_workflow.json()["workflow_id"]
    assert client.post(f"/api/v1/workflows/{threshold_workflow_id}/publish").status_code == 200

    threshold_run = client.post(
        "/api/v1/orchestrator/thresholds:evaluate",
        json={"metric_id": "high_risk_users", "value": 3},
    )
    assert threshold_run.status_code == 200
    assert threshold_run.json()["items"][0]["failures"] >= 1

    deliveries = client.get(f"/api/v1/workflows/{threshold_workflow_id}/deliveries", headers={"x-actor-role": "operator"})
    assert deliveries.status_code == 200
    assert deliveries.json()["items"][0]["delivery_diagnostics"]["attempt_count"] == 3
    assert deliveries.json()["items"][0]["provider_mode"] == "live"

    diagnostics = client.get(f"/api/v1/workflows/{threshold_workflow_id}/delivery-diagnostics", headers={"x-actor-role": "operator"})
    assert diagnostics.status_code == 200
    assert diagnostics.json()["by_status"]["failed"] >= 1
    assert diagnostics.json()["failure_classifications"]["provider_error"] >= 1


def test_workflow_lifecycle_guards_return_locked(client):
    _seed_mock_warehouse()
    _seed_prediction_job()

    cohort = client.post(
        "/api/v1/cohorts",
        json={
            "name": "locked_cohort",
            "type": "sql",
            "definition": {"sql": "SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'"},
            "refresh_mode": "manual",
            "activate": True,
        },
    )
    assert cohort.status_code == 201
    cohort_id = cohort.json()["cohort_id"]

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=locked_exp",
        json={
            "enabled": True,
            "status": "active",
            "primary_metric": "return_rate",
            "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
            "min_sample_size": 1,
            "min_runtime_hours": 0,
            "cohort_id": cohort_id,
        },
    )
    assert experiment.status_code == 200

    workflow = client.post(
        "/api/v1/workflows",
        json={
            "name": "locked_workflow",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily_schedule", "hour": 0, "minute": 0},
            "action": {"channel": "email", "content": "Locked lifecycle test"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
            "experiment_id": "locked_exp",
        },
    )
    assert workflow.status_code == 201
    workflow_id = workflow.json()["workflow_id"]
    assert client.post(f"/api/v1/workflows/{workflow_id}/publish").status_code == 200

    archive = client.post(f"/api/v1/cohorts/{cohort_id}/archive", headers={"x-actor-role": "operator"})
    assert archive.status_code == 423
    assert "locked by published workflows" in archive.json()["detail"]

    paused = client.post(f"/api/v1/cohorts/{cohort_id}/pause", headers={"x-actor-role": "operator"})
    assert paused.status_code == 200

    locked_run = client.post(
        f"/api/v1/workflows/{workflow_id}/test-run",
        json={"limit": 10, "confirm": True, "sandbox": True},
        headers={"x-actor-role": "operator"},
    )
    assert locked_run.status_code == 423
    assert "locked for workflow execution" in locked_run.json()["detail"]

    delete = client.delete(f"/api/v1/cohorts/{cohort_id}/permanent", headers={"x-actor-role": "admin"})
    assert delete.status_code == 423
    assert "locked by workflows" in delete.json()["detail"]

    assert client.post(f"/api/v1/cohorts/{cohort_id}/activate", headers={"x-actor-role": "operator"}).status_code == 200
    assert client.post("/api/v1/experiments/locked_exp/stop", headers={"x-actor-role": "operator"}).status_code == 200

    stopped_run = client.post(
        f"/api/v1/workflows/{workflow_id}/test-run",
        json={"limit": 10, "confirm": True, "sandbox": False},
        headers={"x-actor-role": "operator"},
    )
    assert stopped_run.status_code == 423
    assert "Experiment 'locked_exp' is stopped" in stopped_run.json()["detail"]

    missing_experiment_workflow = client.post(
        "/api/v1/workflows",
        json={
            "name": "missing_experiment_workflow",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily_schedule", "hour": 0, "minute": 0},
            "action": {"channel": "email", "content": "Missing experiment"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
            "experiment_id": "missing_exp",
        },
    )
    assert missing_experiment_workflow.status_code == 201
    publish_missing = client.post(f"/api/v1/workflows/{missing_experiment_workflow.json()['workflow_id']}/publish")
    assert publish_missing.status_code == 404
    assert "Experiment 'missing_exp'" in publish_missing.json()["detail"]


def test_workflow_runtime_summary_archive_and_delete(client):
    cohort = client.post(
        "/api/v1/cohorts",
        headers={"x-actor-role": "operator"},
        json={
            "name": "workflow_studio_cohort",
            "type": "list",
            "definition": {
                "members": [
                    {"canonical_user_id": "u_100", "email": "u100@example.com"},
                    {"canonical_user_id": "u_101", "email": "u101@example.com"},
                ]
            },
            "refresh_mode": "manual",
            "activate": True,
        },
    )
    assert cohort.status_code == 201
    cohort_id = cohort.json()["cohort_id"]

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=workflow_studio_exp",
        headers={"x-actor-role": "operator"},
        json={
            "enabled": True,
            "status": "active",
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

    draft_workflow = client.post(
        "/api/v1/workflows",
        headers={"x-actor-role": "operator"},
        json={
            "name": "draft_delete_workflow",
            "cohort_id": cohort_id,
            "trigger": {"type": "daily_schedule", "hour": 9, "minute": 30},
            "action": {"channel": "email", "content": "Draft delete workflow"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
            "experiment_id": "workflow_studio_exp",
        },
    )
    assert draft_workflow.status_code == 201
    draft_workflow_id = draft_workflow.json()["workflow_id"]

    draft_delete = client.delete(
        f"/api/v1/workflows/{draft_workflow_id}",
        headers={"x-actor-role": "operator"},
    )
    assert draft_delete.status_code == 204
    assert client.get(f"/api/v1/workflows/{draft_workflow_id}").status_code == 404

    workflow = client.post(
        "/api/v1/workflows",
        headers={"x-actor-role": "operator"},
        json={
            "name": "workflow_studio_push",
            "cohort_id": cohort_id,
            "trigger": {"type": "daily_schedule", "hour": 10, "minute": 15},
            "action": {"channel": "email", "content": "Workflow studio live run"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 0},
            "experiment_id": "workflow_studio_exp",
        },
    )
    assert workflow.status_code == 201
    workflow_id = workflow.json()["workflow_id"]

    publish = client.post(f"/api/v1/workflows/{workflow_id}/publish", headers={"x-actor-role": "operator"})
    assert publish.status_code == 200

    sandbox_run = client.post(
        f"/api/v1/workflows/{workflow_id}/test-run",
        headers={"x-actor-role": "operator"},
        json={"limit": 10, "confirm": True, "sandbox": True, "reference_time": "2026-03-10T09:45:00"},
    )
    assert sandbox_run.status_code == 200
    assert sandbox_run.json()["success"] == 2

    live_run = client.post(
        "/api/v1/orchestrator/run-due",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-10T10:15:00", "limit_per_workflow": 10},
    )
    assert live_run.status_code == 200
    assert len(live_run.json()["items"]) == 1
    assert live_run.json()["items"][0]["success"] == 2

    workflow_detail = client.get(f"/api/v1/workflows/{workflow_id}")
    assert workflow_detail.status_code == 200
    runtime_summary = workflow_detail.json()["runtime_summary"]
    assert runtime_summary["last_run_at"] == "2026-03-10T10:15:00"
    assert runtime_summary["last_test_run_at"] == "2026-03-10T09:45:00"
    next_run_at = datetime.fromisoformat(runtime_summary["next_run_at"])
    assert (next_run_at.hour, next_run_at.minute) == (10, 15)
    assert next_run_at > datetime.utcnow()
    assert runtime_summary["last_result"]["success"] == 2
    assert runtime_summary["last_result"]["trigger_type"] == "daily_schedule"
    assert runtime_summary["totals"]["runs"] == 1
    assert runtime_summary["totals"]["test_runs"] == 1
    assert runtime_summary["totals"]["success"] == 2
    assert runtime_summary["totals"]["triggered"] == 2

    listed = client.get("/api/v1/workflows")
    assert listed.status_code == 200
    listed_item = next(item for item in listed.json()["items"] if item["workflow_id"] == workflow_id)
    listed_next_run_at = datetime.fromisoformat(listed_item["runtime_summary"]["next_run_at"])
    assert (listed_next_run_at.hour, listed_next_run_at.minute) == (10, 15)
    assert listed_next_run_at > datetime.utcnow()

    non_draft_delete = client.delete(
        f"/api/v1/workflows/{workflow_id}",
        headers={"x-actor-role": "operator"},
    )
    assert non_draft_delete.status_code == 409
    assert "draft workflows" in non_draft_delete.json()["detail"].lower()

    archive = client.post(
        f"/api/v1/workflows/{workflow_id}/archive",
        headers={"x-actor-role": "operator"},
    )
    assert archive.status_code == 200
    assert archive.json()["status"] == "archived"
    assert archive.json()["archived_at"] is not None

    archived_pause = client.post(
        f"/api/v1/workflows/{workflow_id}/pause",
        headers={"x-actor-role": "operator"},
    )
    assert archived_pause.status_code == 409
    assert "archived" in archived_pause.json()["detail"].lower()

    archived_test_run = client.post(
        f"/api/v1/workflows/{workflow_id}/test-run",
        headers={"x-actor-role": "operator"},
        json={"limit": 10, "confirm": True, "sandbox": True},
    )
    assert archived_test_run.status_code == 409
    assert "archived" in archived_test_run.json()["detail"].lower()

    skipped_due = client.post(
        "/api/v1/orchestrator/run-due",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-11T10:15:00", "limit_per_workflow": 10},
    )
    assert skipped_due.status_code == 200
    assert skipped_due.json()["items"] == []


def test_audience_copilot_weekly_report_and_permanent_delete(client):
    _seed_mock_warehouse()
    _seed_prediction_job()

    cohort = client.post(
        "/api/v1/cohorts",
        json={
            "name": "audience_patch_test",
            "type": "sql",
            "definition": {"sql": "SELECT user_id AS canonical_user_id, email FROM prediction_results WHERE predicted_churn_risk = 'high'"},
            "refresh_mode": "daily",
            "activate": True,
        },
    )
    assert cohort.status_code == 201
    cohort_id = cohort.json()["cohort_id"]

    patched = client.patch(
        f"/api/v1/cohorts/{cohort_id}",
        json={"description": "Updated cohort", "tags": ["churn", "weekly"], "definition": {"sql": "SELECT user_id AS canonical_user_id, email FROM prediction_results"}},
        headers={"x-actor-role": "operator"},
    )
    assert patched.status_code == 200
    assert patched.json()["description"] == "Updated cohort"
    assert patched.json()["version_id"] == 2
    assert client.post(f"/api/v1/cohorts/{cohort_id}/refresh").status_code == 200

    refresh_jobs = client.get(f"/api/v1/cohorts/{cohort_id}/refresh-jobs", headers={"x-actor-role": "analyst"})
    assert refresh_jobs.status_code == 200
    assert refresh_jobs.json()["items"]

    compare = client.get(f"/api/v1/cohorts/{cohort_id}/compare?base_version=1&target_version=2", headers={"x-actor-role": "analyst"})
    assert compare.status_code == 200
    assert "definition_diff" in compare.json()
    assert "metrics_delta" in compare.json()

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=weekly_report_exp",
        json={
            "enabled": True,
            "primary_metric": "return_rate",
            "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
            "min_sample_size": 1,
            "min_runtime_hours": 0,
            "cohort_id": cohort_id,
            "holdout_pct": 0.0,
        },
    )
    assert experiment.status_code == 200

    workflow = client.post(
        "/api/v1/workflows",
        json={
            "name": "weekly_report_workflow",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily"},
            "action": {"channel": "push_notification", "content": "Weekly report seed"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 1},
            "experiment_id": "weekly_report_exp",
        },
    )
    assert workflow.status_code == 201
    workflow_id = workflow.json()["workflow_id"]
    assert client.post(f"/api/v1/workflows/{workflow_id}/publish").status_code == 200
    due = client.post("/api/v1/orchestrator/run-due", json={"reference_time": "2026-03-10T10:00:00", "limit_per_workflow": 10})
    assert due.status_code == 200
    delivery = client.get(f"/api/v1/workflows/{workflow_id}/deliveries", headers={"x-actor-role": "operator"})
    assert delivery.status_code == 200
    scheduled_delivery = next(item for item in delivery.json()["items"] if not item.get("sandbox"))
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
                    "event_id": "weekly_report_event_1",
                    "event_type": "returned",
                }
            ]
        },
    )
    assert callback.status_code == 200

    integrity = client.get("/api/v1/experiments/weekly_report_exp/integrity", headers={"x-actor-role": "analyst"})
    assert integrity.status_code == 200
    assert integrity.json()["outcome_count"] >= 1
    assert integrity.json()["orphan_outcomes"] == 0

    metrics = client.get("/api/v1/copilot/metrics", headers={"x-actor-role": "analyst"})
    assert metrics.status_code == 200
    assert len(metrics.json()["items"]) >= 20

    explain = client.post(
        "/api/v1/copilot/explain",
        json={"metric_id": "promo_views", "time_window": "7d", "dimensions": ["campaign", "country", "platform"]},
    )
    assert explain.status_code == 200
    anomaly_id = explain.json()["anomaly_id"]

    anomaly = client.get(f"/api/v1/copilot/anomalies/{anomaly_id}", headers={"x-actor-role": "analyst"})
    assert anomaly.status_code == 200
    assert anomaly.json()["baseline_windows"]["7d"] >= 0

    weekly_report = client.post("/api/v1/copilot/report", json={"report_type": "weekly", "time_window": "7d"})
    assert weekly_report.status_code == 200
    report_id = weekly_report.json()["report_id"]

    retry = client.post(f"/api/v1/copilot/reports/{report_id}/retry", headers={"x-actor-role": "analyst"})
    assert retry.status_code == 200
    assert retry.json()["report_id"]
    retry_report_id = retry.json()["report_id"]

    report_detail = client.get(f"/api/v1/copilot/reports/{report_id}", headers={"x-actor-role": "analyst"})
    assert report_detail.status_code == 200
    assert report_detail.json()["latest_retry_report_id"] == retry_report_id

    report_runs = client.get(f"/api/v1/copilot/reports/{report_id}/runs", headers={"x-actor-role": "analyst"})
    assert report_runs.status_code == 200
    assert len(report_runs.json()["items"]) >= 2

    reports = client.get("/api/v1/copilot/reports", headers={"x-actor-role": "analyst"})
    assert reports.status_code == 200
    assert any(item.get("report_type") == "weekly" for item in reports.json()["items"])

    archived = client.post(f"/api/v1/cohorts/{cohort_id}/archive")
    assert archived.status_code == 423

    denied_delete = client.delete(f"/api/v1/cohorts/{cohort_id}/permanent", headers={"x-actor-role": "operator"})
    assert denied_delete.status_code == 423

    locked_delete = client.delete(f"/api/v1/cohorts/{cohort_id}/permanent", headers={"x-actor-role": "admin"})
    assert locked_delete.status_code == 423

    disposable = client.post(
        "/api/v1/cohorts",
        json={
            "name": "delete_safe_cohort",
            "type": "list",
            "definition": {"members": [{"canonical_user_id": "delete_u_1", "email": "delete_u_1@example.com"}]},
            "refresh_mode": "manual",
            "activate": True,
        },
    )
    assert disposable.status_code == 201
    disposable_id = disposable.json()["cohort_id"]
    assert client.post(f"/api/v1/cohorts/{disposable_id}/archive").status_code == 200

    deleted = client.delete(f"/api/v1/cohorts/{disposable_id}/permanent", headers={"x-actor-role": "admin"})
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True


def test_health_audit_templates_and_workflow_builder(client):
    connector_resp = client.post(
        "/api/v1/connectors",
        json={"name": "Adjust Source", "type": "adjust", "config": {"api_token": "adjust-token"}},
    )
    assert connector_resp.status_code == 201

    weak_mapping = client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={"mapping": {"canonical_user_id": "player_id"}},
    )
    assert weak_mapping.status_code == 200

    create_import = client.post(
        "/api/v1/imports",
        json={"source_name": "Adjust Source", "start_date": "20260301", "end_date": "20260302"},
    )
    assert create_import.status_code == 201
    blocked = client.post(create_import.json()["links"]["self"] + "/run")
    assert blocked.status_code == 200
    assert blocked.json()["status"] == "awaiting_mapping"

    alerts = client.get("/api/v1/health/alerts")
    assert alerts.status_code == 200
    assert any(item["code"] == "awaiting_mapping" for item in alerts.json()["items"])

    modules = client.get("/api/v1/health/modules")
    assert modules.status_code == 200
    assert any(item["module"] == "data_core" for item in modules.json()["items"])

    audit = client.get("/api/v1/audit/actions?resource_type=import_job&tenant_id=default", headers={"x-actor-role": "operator"})
    assert audit.status_code == 200
    assert audit.json()["summary"]["returned"] >= 1
    assert audit.json()["tenant_id"] == "default"

    _seed_mock_warehouse()
    _seed_prediction_job()

    mapping_suggestions = client.get("/api/v1/mappings/Adjust%20Source/suggestions")
    assert mapping_suggestions.status_code == 200
    assert any(item.get("sample_values") for item in mapping_suggestions.json()["suggestions"])

    templates = client.get("/api/v1/templates", headers={"x-actor-role": "operator"})
    assert templates.status_code == 200
    assert len(templates.json()["items"]) >= 3

    instance = client.post(
        "/api/v1/templates/onboarding_activation/instantiate",
        headers={"x-actor-role": "operator", "x-tenant-id": "studio-a"},
        json={"owner": "ops", "activate_cohort": True, "publish_workflow": True},
    )
    assert instance.status_code == 201
    assert instance.json()["workflow"]["status"] == "published"
    assert instance.json()["tenant_id"] == "studio-a"

    builder_cohort = client.post(
        "/api/v1/cohorts",
        headers={"x-actor-role": "operator"},
        json={
            "name": "builder_cohort",
            "type": "list",
            "definition": {
                "members": [
                    {"canonical_user_id": "builder_u1", "email": "builder1@example.com", "country": "US", "platform": "ios"},
                    {"canonical_user_id": "builder_u2", "email": "builder2@example.com", "country": "CA", "platform": "android"},
                ]
            },
            "refresh_mode": "manual",
            "activate": True,
        },
    )
    assert builder_cohort.status_code == 201
    cohort_id = builder_cohort.json()["cohort_id"]

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=builder_exp",
        headers={"x-actor-role": "operator"},
        json={
            "enabled": True,
            "primary_metric": "return_rate",
            "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
            "min_sample_size": 1,
            "min_runtime_hours": 0,
            "cohort_id": cohort_id,
            "holdout_pct": 0.0,
            "b_variant_pct": 0.0,
            "rollout_policy": "balanced",
            "multiple_comparisons_method": "holm_bonferroni",
        },
    )
    assert experiment.status_code == 200

    workflow = client.post(
        "/api/v1/workflows",
        headers={"x-actor-role": "operator"},
        json={
            "name": "builder_flow",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily"},
            "action": {"channel": "push_notification", "content": "fallback"},
            "policy": {"global_daily_limit": 5, "channel_daily_limit": 5, "cooldown_hours": 0},
            "experiment_id": "builder_exp",
            "steps": [
                {"type": "filter", "conditions": [{"field": "country", "op": "=", "value": "US"}]},
                {"type": "wait", "seconds": 60},
                {
                    "type": "if_else",
                    "condition": {"field": "platform", "op": "=", "value": "ios"},
                    "then": {"action": {"channel": "push_notification", "content": "iOS offer"}},
                    "else": {"action": {"channel": "push_notification", "content": "fallback offer"}},
                },
            ],
        },
    )
    assert workflow.status_code == 201
    workflow_id = workflow.json()["workflow_id"]

    publish = client.post(f"/api/v1/workflows/{workflow_id}/publish", headers={"x-actor-role": "operator"})
    assert publish.status_code == 200

    run = client.post(
        f"/api/v1/workflows/{workflow_id}/test-run",
        headers={"x-actor-role": "operator"},
        json={"limit": 10, "confirm": True, "sandbox": True},
    )
    assert run.status_code == 200
    assert run.json()["success"] == 1
    assert run.json()["filtered_out"] == 1

    deliveries = client.get(f"/api/v1/workflows/{workflow_id}/deliveries", headers={"x-actor-role": "operator"})
    assert deliveries.status_code == 200
    sandbox_delivery = next(item for item in deliveries.json()["items"] if item.get("sandbox"))
    assert sandbox_delivery["provider_request"]["content"] == "iOS offer"

    summary = client.get("/api/v1/experiments/builder_exp/summary")
    assert summary.status_code == 200
    assert summary.json()["multiple_comparisons_method"] == "holm_bonferroni"
    assert summary.json()["multiple_comparisons_note"]
    assert summary.json()["confidence_hint"] in {"low", "medium", "high"}
    assert summary.json()["significance_hint"]

    rollout = client.get("/api/v1/experiments/builder_exp/rollout-suggestion")
    assert rollout.status_code == 200
    assert rollout.json()["rollout_policy"] == "balanced"


def test_scheduler_tick_persistent_alerts_and_ai_mapping_suggestions(client, monkeypatch):
    class FakeGeminiClient:
        model_name = "gemini-test"

        def __init__(self, *args, **kwargs):
            pass

        def get_ai_response(self, prompt):
            return """
            {
              "suggestions": [
                {
                  "field": "campaign",
                  "suggested_path": "event_properties.campaign",
                  "confidence": 0.98,
                  "rationale": "Nested campaign field best matches the current source payload."
                }
              ]
            }
            """

    monkeypatch.setenv("GOOGLE_API_KEY", "mock-key")
    monkeypatch.setattr("app.application.text_model_runtime.GeminiClient", FakeGeminiClient)

    client.post(
        "/api/v1/connectors",
        json={"name": "Gemini Mapping", "type": "google", "config": {"api_key": "mock-key", "model_name": "gemini-test"}},
    )
    client.post(
        "/api/v1/connectors",
        json={"name": "Adjust Source", "type": "adjust", "config": {"api_token": "adjust-token"}},
    )
    client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={"mapping": {"canonical_user_id": "player_id"}},
    )
    create_import = client.post(
        "/api/v1/imports",
        json={"source_name": "Adjust Source", "start_date": "20260301", "end_date": "20260302"},
    )
    assert create_import.status_code == 201
    job_id = create_import.json()["id"]
    blocked = client.post(f"/api/v1/imports/{job_id}/run")
    assert blocked.status_code == 200
    assert blocked.json()["status"] == "awaiting_mapping"

    open_alerts = client.get("/api/v1/health/alerts?include_resolved=true")
    assert open_alerts.status_code == 200
    awaiting_mapping = next(item for item in open_alerts.json()["items"] if item["code"] == "awaiting_mapping")
    assert awaiting_mapping["status"] == "open"

    save_mapping = client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={"mapping": {"canonical_user_id": "player_id", "event_name": "event_type", "event_time": "event_time"}},
    )
    assert save_mapping.status_code == 200
    resumed = client.post(f"/api/v1/imports/{job_id}/resume", headers={"x-actor-role": "operator"})
    assert resumed.status_code == 200
    assert resumed.json()["status"] in {"completed", "running"}

    tick = client.post(
        "/api/v1/health/scheduler/tick",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-10T10:00:00"},
    )
    assert tick.status_code == 200
    assert any(item["job_id"] == "health_refresh" for item in tick.json()["items"])

    scheduler_jobs = client.get("/api/v1/health/scheduler", headers={"x-actor-role": "analyst"})
    assert scheduler_jobs.status_code == 200
    assert any(item["job_id"] == "daily_copilot_report" for item in scheduler_jobs.json()["items"])

    alerts = client.get("/api/v1/health/alerts?include_resolved=true")
    assert alerts.status_code == 200
    resolved_alert = next(item for item in alerts.json()["items"] if item["code"] == "awaiting_mapping")
    assert resolved_alert["status"] == "resolved"

    suggestions = client.get("/api/v1/mappings/Adjust%20Source/suggestions")
    assert suggestions.status_code == 200
    assert suggestions.json()["engine"] == "ai_assisted"
    assert suggestions.json()["model_name"] == "gemini-test"
    campaign_suggestion = next(item for item in suggestions.json()["suggestions"] if item["field"] == "campaign")
    assert campaign_suggestion["suggested_path"] == "event_properties.campaign"


def test_mapping_suggestions_use_default_openai_model_profile(client, monkeypatch):
    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": """
                            {
                              "suggestions": [
                                {
                                  "field": "campaign",
                                  "suggested_path": "event_properties.openai_campaign",
                                  "confidence": 0.91,
                                  "rationale": "OpenAI-compatible model selected the nested field."
                                }
                              ]
                            }
                            """,
                        }
                    }
                ]
            }

    captured = {}

    def _fake_post(url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers or {}
        captured["json"] = json or {}
        return _FakeResponse()

    monkeypatch.setattr("app.application.text_model_runtime.requests.post", _fake_post)

    headers = {"x-actor-role": "operator"}
    profile = client.post(
        "/api/v1/copilot/agent/model-profiles",
        headers=headers,
        json={
            "name": "Ollama Mapping",
            "provider": "openai",
            "model_name": "llama3.1",
            "config": {
                "base_url": "http://127.0.0.1:11434/v1",
                "runtime_preset": "ollama",
            },
            "is_default": True,
        },
    )
    assert profile.status_code == 201, profile.text

    client.post(
        "/api/v1/connectors",
        json={"name": "Adjust Source", "type": "adjust", "config": {"api_token": "adjust-token"}},
    )
    client.put(
        "/api/v1/mappings/Adjust%20Source",
        json={"mapping": {"canonical_user_id": "player_id"}},
    )

    suggestions = client.get("/api/v1/mappings/Adjust%20Source/suggestions")
    assert suggestions.status_code == 200
    assert suggestions.json()["engine"] == "ai_assisted"
    assert suggestions.json()["model_name"] == "llama3.1"
    campaign_suggestion = next(item for item in suggestions.json()["suggestions"] if item["field"] == "campaign")
    assert campaign_suggestion["suggested_path"] == "event_properties.openai_campaign"
    assert captured["url"] == "http://127.0.0.1:11434/v1/chat/completions"
    assert "Authorization" not in captured["headers"]


def test_copilot_comparison_and_experiment_statistics(client):
    _seed_mock_warehouse()
    _seed_prediction_job()

    copilot_query = client.post(
        "/api/v1/copilot/query",
        json={"question": "compare promo views ios vs android in 7d"},
    )
    assert copilot_query.status_code == 200
    assert "ios vs android" in copilot_query.json()["conclusion"].lower()
    assert copilot_query.json()["methodology"]["sql_summary"]["parsed_intent"]["comparison"]["dimension"] == "platform"

    copilot_explain = client.post(
        "/api/v1/copilot/explain",
        json={"metric_id": "promo_views", "time_window": "7d", "dimensions": ["campaign", "country", "platform"]},
    )
    assert copilot_explain.status_code == 200
    assert len(copilot_explain.json()["evidence"]) >= 2
    assert "impact_score" in copilot_explain.json()["evidence"][0]

    members = [
        {"canonical_user_id": f"exp_u_{index:02d}", "email": f"exp{index:02d}@example.com", "country": "US", "platform": "ios"}
        for index in range(1, 31)
    ]
    cohort = client.post(
        "/api/v1/cohorts",
        headers={"x-actor-role": "operator"},
        json={
            "name": "experiment_stats_cohort",
            "type": "list",
            "definition": {"members": members},
            "refresh_mode": "manual",
            "activate": True,
        },
    )
    assert cohort.status_code == 201
    cohort_id = cohort.json()["cohort_id"]

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=stats_exp",
        headers={"x-actor-role": "operator"},
        json={
            "enabled": True,
            "primary_metric": "return_rate",
            "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
            "min_sample_size": 1,
            "min_runtime_hours": 0,
            "cohort_id": cohort_id,
            "holdout_pct": 0.2,
            "b_variant_pct": 0.4,
            "multiple_comparisons_method": "holm_bonferroni",
            "rollout_policy": "aggressive",
        },
    )
    assert experiment.status_code == 200

    workflow = client.post(
        "/api/v1/workflows",
        headers={"x-actor-role": "operator"},
        json={
            "name": "stats_flow",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily", "hour": 10, "minute": 0},
            "action": {"channel": "push_notification", "content": "stats offer"},
            "policy": {"global_daily_limit": 50, "channel_daily_limit": 50, "cooldown_hours": 0},
            "experiment_id": "stats_exp",
        },
    )
    assert workflow.status_code == 201
    workflow_id = workflow.json()["workflow_id"]
    assert client.post(f"/api/v1/workflows/{workflow_id}/publish", headers={"x-actor-role": "operator"}).status_code == 200

    run = client.post(
        "/api/v1/orchestrator/run-due",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-10T10:00:00", "limit_per_workflow": 100},
    )
    assert run.status_code == 200
    assert run.json()["items"][0]["triggered"] == 30

    assignments = client.get("/api/v1/experiments/stats_exp/assignments")
    assert assignments.status_code == 200
    groups = assignments.json()["items"]
    assert any(item["group"] == "holdout" for item in groups)
    assert any(item["group"] == "treatment_a" for item in groups)
    assert any(item["group"] == "treatment_b" for item in groups)

    outcomes = []
    for item in groups:
        if item["group"] == "holdout":
            continue
        if item["group"] == "treatment_a" or item["user_id"].endswith(("1", "3")):
            outcomes.append(
                {
                    "workflow_id": workflow_id,
                    "cohort_id": cohort_id,
                    "experiment_id": "stats_exp",
                    "user_id": item["user_id"],
                    "group": item["group"],
                    "occurred_at": "2026-03-10T11:00:00",
                    "outcome_name": "returned",
                    "source": "internal_writeback",
                }
            )
    ingest = client.post(
        "/api/v1/experiments/stats_exp/outcomes:ingest",
        headers={"x-actor-role": "operator"},
        json={"outcomes": outcomes},
    )
    assert ingest.status_code == 200
    assert ingest.json()["ingested"] == len(outcomes)

    summary = client.get("/api/v1/experiments/stats_exp/summary")
    assert summary.status_code == 200
    assert summary.json()["multiple_comparisons_method"] == "holm_bonferroni"
    assert len(summary.json()["comparisons"]) >= 2
    assert any("adjusted_p_value" in item for item in summary.json()["comparisons"])
    assert summary.json()["winner_group"] in {"treatment_a", "treatment_b"}

    rollout = client.get("/api/v1/experiments/stats_exp/rollout-suggestion")
    assert rollout.status_code == 200
    assert rollout.json()["winner_group"] == summary.json()["winner_group"]
    assert rollout.json()["suggestion"].startswith("expand_")
