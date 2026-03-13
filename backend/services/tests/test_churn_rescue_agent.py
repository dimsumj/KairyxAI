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
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    monkeypatch.setenv("SCHEDULER_ENABLED", "false")
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    clear_shared_bigquery_service_cache()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def _seed_players(job_id: str = "imp_churn_agent") -> None:
    service = get_shared_bigquery_service()
    rows = []
    for index in range(20):
        user_id = f"u_{index:02d}"
        email = f"{user_id}@example.com"
        if index < 10:
            event_times = [
                "2026-03-06T08:00:00",
                "2026-03-08T09:00:00",
                "2026-03-09T09:30:00",
            ]
        else:
            event_times = [
                "2026-02-05T08:00:00",
                "2026-02-15T09:00:00",
                "2026-02-20T09:30:00",
            ]
        for event_time in event_times:
            rows.append(
                {
                    "job_id": job_id,
                    "source": "adjust",
                    "player_id": user_id,
                    "canonical_user_id": user_id,
                    "event_type": "session_start",
                    "event_time": event_time,
                    "event_properties": {"campaign": "seeded", "platform": "ios"},
                    "user_properties": {"email": email},
                }
            )
        if index % 4 == 0:
            rows.append(
                {
                    "job_id": job_id,
                    "source": "adjust",
                    "player_id": user_id,
                    "canonical_user_id": user_id,
                    "event_type": "item_purchased",
                    "event_time": event_times[-1],
                    "event_properties": {"campaign": "seeded", "platform": "ios", "revenue_usd": 4.99},
                    "user_properties": {"email": email},
                }
            )
    service.write_events_staging(rows, job_id=job_id)
    service.run_events_curation(job_id=job_id)
    service.refresh_player_latest_state(job_id=job_id)

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        if repository.get_import_job(job_id) is None:
            repository.create_import_job(
                {
                    "id": job_id,
                    "source_name": "adjust",
                    "status": "completed",
                    "spec": {"job_id": job_id},
                    "progress": {"current": len(rows), "total": len(rows), "pct": 100.0, "details": {}},
                }
            )


def _append_return_events(user_ids: list[str], occurred_at: str, job_id: str = "imp_churn_agent") -> None:
    if not user_ids:
        return
    service = get_shared_bigquery_service()
    rows = []
    for user_id in user_ids:
        rows.append(
            {
                "job_id": job_id,
                "source": "adjust",
                "player_id": user_id,
                "canonical_user_id": user_id,
                "event_type": "session_start",
                "event_time": occurred_at,
                "event_properties": {"campaign": "return", "platform": "ios"},
                "user_properties": {"email": f"{user_id}@example.com"},
            }
        )
    service.write_events_staging(rows, job_id=job_id)
    service.run_events_curation(job_id=job_id)
    service.refresh_player_latest_state(job_id=job_id)


def test_guarded_closed_loop_churn_rescue_agent(client):
    _seed_players()

    query = client.post(
        "/api/v1/sql-workspace/queries",
        headers={"x-actor-role": "operator"},
        json={
            "name": "All players",
            "sql": "SELECT canonical_user_id, email, days_since_last_seen FROM mart_user_daily",
            "description": "All active and inactive players for churn rescue testing",
        },
    )
    assert query.status_code == 201
    query_id = query.json()["query_id"]

    cohort = client.post(
        f"/api/v1/sql-workspace/queries/{query_id}/cohort",
        headers={"x-actor-role": "operator"},
        json={"name": "all_players_daily", "refresh_mode": "daily", "activate": True},
    )
    assert cohort.status_code == 201
    cohort_id = cohort.json()["cohort_id"]

    experiment = client.post(
        "/api/v1/experiments/config?experiment_id=churn_agent_v1",
        headers={"x-actor-role": "operator"},
        json={
            "enabled": True,
            "primary_metric": "return_rate",
            "guardrail_metrics": ["engagement_rate", "policy_block_rate"],
            "min_sample_size": 4,
            "min_runtime_hours": 0,
            "cohort_id": cohort_id,
            "holdout_pct": 0.2,
            "b_variant_pct": 0.5,
            "scenario_type": "churn_rescue",
            "optimization_mode": "fixed_ab",
            "holdout_floor_pct": 0.1,
            "max_daily_shift_pct": 0.1,
            "approved_variants": [
                {
                    "variant_id": "treatment_a",
                    "template_id": "churn_rescue_push_a",
                    "channel": "push_notification",
                    "content": "Return today for a win-back reward.",
                    "send_window": {"hour": 10, "minute": 0},
                },
                {
                    "variant_id": "treatment_b",
                    "template_id": "churn_rescue_push_b",
                    "channel": "push_notification",
                    "content": "Come back now to claim your comeback bonus.",
                    "send_window": {"hour": 11, "minute": 0},
                },
            ],
        },
    )
    assert experiment.status_code == 200

    workflow = client.post(
        "/api/v1/workflows",
        headers={"x-actor-role": "operator"},
        json={
            "name": "daily_churn_agent",
            "cohort_id": cohort_id,
            "schedule": {"type": "daily", "hour": 10, "minute": 0},
            "action": {"channel": "push_notification", "content": "Return today for a win-back reward."},
            "policy": {
                "global_daily_limit": 100,
                "channel_daily_limit": 100,
                "cooldown_hours": 0,
                "quiet_hours": {"start": 23, "end": 6},
            },
            "experiment_id": "churn_agent_v1",
            "requires_confirmation": False,
        },
    )
    assert workflow.status_code == 201
    workflow_id = workflow.json()["workflow_id"]
    assert client.post(f"/api/v1/workflows/{workflow_id}/publish", headers={"x-actor-role": "operator"}).status_code == 200

    run_day_one = client.post(
        "/api/v1/orchestrator/run-due",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-10T10:00:00", "limit_per_workflow": 100},
    )
    assert run_day_one.status_code == 200
    assert run_day_one.json()["items"][0]["triggered"] == 20

    run_day_two = client.post(
        "/api/v1/orchestrator/run-due",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-11T10:00:00", "limit_per_workflow": 100},
    )
    assert run_day_two.status_code == 200
    assert run_day_two.json()["items"][0]["triggered"] == 20

    exposures = client.get("/api/v1/experiments/churn_agent_v1/exposures")
    assert exposures.status_code == 200
    exposure_items = exposures.json()["items"]
    assert len(exposure_items) >= 32

    holdout_returns = []
    treatment_a_returns = []
    treatment_b_returns = []
    outcomes = []
    for index, exposure in enumerate(exposure_items):
        user_id = str(exposure["user_id"])
        occurred_at = "2026-03-11T12:00:00" if str(exposure.get("exposed_at") or "").startswith("2026-03-10") else "2026-03-12T12:00:00"
        should_return = False
        group = str(exposure.get("group") or "")
        if group == "holdout":
            should_return = index % 2 == 0
            if should_return:
                holdout_returns.append(user_id)
        elif group == "treatment_b":
            should_return = True
            treatment_b_returns.append(user_id)
        elif group == "treatment_a":
            should_return = index % 4 == 0
            if should_return:
                treatment_a_returns.append(user_id)
        if should_return:
            outcomes.append(
                {
                    "workflow_id": workflow_id,
                    "cohort_id": cohort_id,
                    "experiment_id": "churn_agent_v1",
                    "user_id": user_id,
                    "group": group,
                    "action_execution_id": exposure.get("action_execution_id"),
                    "delivery_id": exposure.get("delivery_id"),
                    "provider_callback_id": f"cb_{user_id}_{group}",
                    "occurred_at": occurred_at,
                    "outcome_name": "returned",
                    "product_outcome_type": "return",
                    "attribution_window_days": 7,
                    "variant_id": exposure.get("variant_id"),
                    "template_id": exposure.get("template_id"),
                    "source": "internal_writeback",
                }
            )

    _append_return_events(sorted(set(holdout_returns + treatment_a_returns + treatment_b_returns)), "2026-03-12T13:00:00")

    ingest = client.post(
        "/api/v1/experiments/churn_agent_v1/outcomes:ingest",
        headers={"x-actor-role": "operator"},
        json={"outcomes": outcomes},
    )
    assert ingest.status_code == 200
    assert ingest.json()["ingested"] == len(outcomes)

    train = client.post(
        "/api/v1/predictions/models/train",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-12T08:00:00", "min_rows": 6},
    )
    assert train.status_code == 200
    assert train.json()["model"]["status"] == "active"
    assert train.json()["model"]["metrics"]["validation_accuracy"] >= train.json()["model"]["metrics"]["heuristic_accuracy"]

    optimizer = client.post(
        "/api/v1/experiments/churn_agent_v1/optimizer/run",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-12T08:00:00", "apply_changes": True},
    )
    assert optimizer.status_code == 200
    optimizer_body = optimizer.json()
    assert optimizer_body["applied"] is True
    assert optimizer_body["policy_snapshot"]["winner_group"] == "treatment_b"
    assert optimizer_body["config_after"]["holdout_pct"] >= 0.1
    assert optimizer_body["config_after"]["b_variant_pct"] > 0.5

    optimizer_state = client.get(
        "/api/v1/experiments/churn_agent_v1/optimizer",
        headers={"x-actor-role": "operator"},
    )
    assert optimizer_state.status_code == 200
    assert optimizer_state.json()["policy_snapshot"]["recommended_variant_id"] == "treatment_b"

    not_due = client.post(
        "/api/v1/orchestrator/run-due",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-12T10:00:00", "limit_per_workflow": 100},
    )
    assert not_due.status_code == 200
    assert not_due.json()["items"] == []

    due_after_override = client.post(
        "/api/v1/orchestrator/run-due",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-12T11:00:00", "limit_per_workflow": 100},
    )
    assert due_after_override.status_code == 200
    assert due_after_override.json()["items"][0]["triggered"] == 20

    latest_model = client.get("/api/v1/predictions/models/latest", headers={"x-actor-role": "analyst"})
    assert latest_model.status_code == 200
    assert latest_model.json()["model"]["model_version"].startswith("crm_")

    prediction_job = client.post(
        "/api/v1/predictions",
        json={"import_job_id": "imp_churn_agent", "prediction_mode": "local"},
    )
    assert prediction_job.status_code == 201
    job_id = prediction_job.json()["id"]

    run_prediction = client.post(f"/api/v1/predictions/{job_id}/run")
    assert run_prediction.status_code == 200
    assert run_prediction.json()["status"] == "completed"

    results = client.get(f"/api/v1/predictions/{job_id}/results")
    assert results.status_code == 200
    items = results.json()["items"]
    assert items
    assert any(item.get("model_version", "").startswith("crm_") for item in items)
    assert any(item.get("recommended_variant") == "treatment_b" for item in items)
    assert any(item.get("recommended_template_id") == "churn_rescue_push_b" for item in items)

    scheduler = client.post(
        "/api/v1/health/scheduler/tick",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2026-03-13T08:00:00"},
    )
    assert scheduler.status_code == 200
    assert any(item["job_id"] == "daily_churn_rescue_optimizer" for item in scheduler.json()["items"])

