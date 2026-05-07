from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.application.ai_evaluations import AIEvaluationService, MAX_LIST_LIMIT
from app.core import db as db_module
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from app.main import create_app


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    db_module.clear_runtime_database_fallback()
    db_module.get_engine.cache_clear()
    db_module.get_session_factory.cache_clear()
    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
    db_module.clear_runtime_database_fallback()


def _evaluation_payload() -> dict:
    return {
        "evaluation_type": "retrieval_quality",
        "target_type": "knowledge_retrieval",
        "target_id": "kret_example",
        "outcome": "useful",
        "score": 0.82,
        "dimensions": {
            "citation_coverage": 1.0,
            "answer_relevance": 0.75,
            "retrieval_quality": 0.8,
        },
        "citation_ids": ["C1", "C2", "C1"],
        "artifact_ids": ["kret_example"],
        "prompt_summary": "Find winback push guidance.",
        "response_summary": "Returned two cited lifecycle playbook chunks.",
        "comments": "Useful evidence for campaign drafting.",
        "source": "operator",
        "metadata": {"channel": "push", "revision": 1},
        "evaluated_by": "growth_operator",
    }


def test_ai_evaluation_records_summary_and_export(client):
    created = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "analyst"},
        json=_evaluation_payload(),
    )
    assert created.status_code == 201
    payload = created.json()
    assert payload["evaluation_id"].startswith("aieval_")
    assert payload["evaluation_type"] == "retrieval_quality"
    assert payload["score"] == 0.82
    assert payload["score_source"] == "explicit"
    assert payload["citation_ids"] == ["C1", "C2"]
    assert payload["export"]["format"] == "ai_evaluation_record.v1"
    assert payload["audit_id"] > 0

    edited = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "operator"},
        json={
            "evaluation_type": "campaign_copy_usefulness",
            "target_type": "email_campaign_draft",
            "target_id": "email_123",
            "outcome": "edited",
            "dimensions": {"copy_usefulness": 0.6, "prompt_to_artifact_completion": 1.0},
        },
    )
    assert edited.status_code == 201
    assert edited.json()["score_source"] == "dimensions"

    listed = client.get("/api/v1/experiments/ai-evaluations", headers={"x-actor-role": "analyst"})
    assert listed.status_code == 200
    assert len(listed.json()["items"]) == 2

    filtered = client.get(
        "/api/v1/experiments/ai-evaluations?evaluation_type=retrieval_quality",
        headers={"x-actor-role": "analyst"},
    )
    assert filtered.status_code == 200
    assert [item["evaluation_type"] for item in filtered.json()["items"]] == ["retrieval_quality"]

    summary = client.get("/api/v1/experiments/ai-evaluations/summary", headers={"x-actor-role": "analyst"})
    assert summary.status_code == 200
    summary_payload = summary.json()
    assert summary_payload["total_records"] == 2
    assert summary_payload["positive_rate"] == 0.5
    assert summary_payload["edited_rate"] == 0.5
    assert summary_payload["dimension_averages"]["citation_coverage"] == 1.0

    evaluation_id = payload["evaluation_id"]
    exported = client.get(f"/api/v1/experiments/ai-evaluations/{evaluation_id}/export", headers={"x-actor-role": "analyst"})
    assert exported.status_code == 200
    export_payload = exported.json()
    assert export_payload["format"] == "ai_evaluation_record.v1"
    assert export_payload["evaluation"]["evaluation_id"] == evaluation_id


def test_ai_evaluation_auto_grader_records_retrieval_generation_and_artifact_scores(client):
    graded = client.post(
        "/api/v1/experiments/ai-evaluations/grade",
        headers={"x-actor-role": "analyst"},
        json={
            "target_type": "push_copy_draft",
            "target_id": "draft_push_123",
            "prompt": "Draft a winback push with a saved checkpoint reward and schedule handoff.",
            "response": "Title: Your checkpoint is waiting. Body: Come back for your saved reward. Evidence: [C1].",
            "citations": [
                {
                    "citation_id": "C1",
                    "score": 2.4,
                    "snippet": "Winback push should mention saved checkpoints, returning rewards, and low-friction play.",
                }
            ],
            "artifacts": [
                {"resource_type": "knowledge_retrieval", "resource_id": "kret_auto_123"},
                {"resource_type": "workflow", "resource_id": "wf_draft_123"},
            ],
            "expected_artifact_type": "workflow",
            "generated_title": "Your checkpoint is waiting",
            "generated_body": "Come back for your saved reward and keep playing from where you left off.",
            "metadata": {"channel": "push"},
        },
    )
    assert graded.status_code == 201, graded.text
    payload = graded.json()
    assert payload["grading_id"].startswith("aigrade_")
    assert payload["target_type"] == "push_copy_draft"
    assert payload["summary"]["average_score"] > 0.65
    assert payload["export"]["format"] == "ai_evaluation_grading.v1"

    by_type = {item["evaluation_type"]: item for item in payload["evaluations"]}
    assert set(by_type) == {
        "answer_relevance",
        "campaign_copy_usefulness",
        "citation_coverage",
        "prompt_to_artifact_completion",
        "retrieval_quality",
    }
    assert by_type["retrieval_quality"]["source"] == "auto_grader"
    assert by_type["retrieval_quality"]["evaluated_by"] == "deterministic_ai_grader_v1"
    assert by_type["retrieval_quality"]["metadata"]["grading_id"] == payload["grading_id"]
    assert by_type["citation_coverage"]["dimensions"]["citation_coverage"] == 1.0
    assert by_type["answer_relevance"]["dimensions"]["hallucination_risk"] < 0.5
    assert by_type["prompt_to_artifact_completion"]["score"] == 1.0
    assert by_type["campaign_copy_usefulness"]["citation_ids"] == ["C1"]
    assert by_type["campaign_copy_usefulness"]["artifact_ids"] == ["kret_auto_123", "wf_draft_123"]

    listed = client.get(
        "/api/v1/experiments/ai-evaluations?target_id=draft_push_123",
        headers={"x-actor-role": "analyst"},
    )
    assert listed.status_code == 200
    assert len(listed.json()["items"]) == 5

    summary = client.get(
        "/api/v1/experiments/ai-evaluations/summary?target_type=push_copy_draft",
        headers={"x-actor-role": "analyst"},
    )
    assert summary.status_code == 200
    summary_payload = summary.json()
    assert summary_payload["total_records"] == 5
    assert summary_payload["dimension_averages"]["citation_coverage"] == 1.0


def test_ai_evaluation_judge_run_records_external_model_scores_and_updates_monitor(client):
    run = client.post(
        "/api/v1/experiments/ai-evaluations/judge-runs",
        headers={"x-actor-role": "operator"},
        json={
            "run_type": "model_judge",
            "run_label": "Copy judge smoke",
            "rubric": {"goal": "Judge whether the copy is useful for a winback push."},
            "items": [
                {
                    "evaluation_type": "campaign_copy_usefulness",
                    "target_type": "push_copy_draft",
                    "target_id": "draft_judged_push",
                    "prompt": "Draft a push that calls players back to the game.",
                    "response": "Title: Your reward is waiting. Body: Come back and keep playing from your saved checkpoint.",
                    "score": 0.88,
                    "dimensions": {"campaign_copy_usefulness": 0.88, "action_clarity": 1.0},
                    "citation_ids": ["C_WINBACK"],
                    "artifact_ids": ["draft_judged_push"],
                }
            ],
            "metadata": {"sample": "operator_review"},
        },
    )
    assert run.status_code == 201, run.text
    payload = run.json()
    assert payload["run_id"].startswith("aijudge_")
    assert payload["run_type"] == "model_judge"
    assert payload["summary"]["evaluation_count"] == 1
    evaluation = payload["evaluations"][0]
    assert evaluation["source"] == "model_judge_provider"
    assert evaluation["evaluated_by"] == "model_judge_adapter_v1:external"
    assert evaluation["metadata"]["judge_run_id"] == payload["run_id"]
    assert evaluation["metadata"]["score_origin"] == "provided"
    assert evaluation["metadata"]["offline_eval"] is False
    assert evaluation["outcome"] == "useful"
    assert payload["tenant_id"] == "default"
    assert payload["project_id"] == "default"
    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        stored_run = repository.get_resource("ai_evaluation_judge_run", payload["run_id"])
        assert stored_run is not None
        assert stored_run["payload"]["run_id"] == payload["run_id"]

    audit = client.get(
        f"/api/v1/audit/actions?action_type=ai_evaluations_judge_run_recorded&resource_type=ai_evaluation_judge_run&resource_id={payload['run_id']}",
        headers={"x-actor-role": "analyst"},
    )
    assert audit.status_code == 200
    assert audit.json()["summary"]["returned"] == 1

    monitor = client.get("/api/v1/experiments/ai-quality-monitor", headers={"x-actor-role": "analyst"})
    assert monitor.status_code == 200
    readiness = monitor.json()["judge_readiness"]
    assert readiness["model_judge_records"] == 1
    assert readiness["offline_eval_records"] == 0


def test_ai_evaluation_judge_run_records_offline_batches_without_runtime(client):
    run = client.post(
        "/api/v1/experiments/ai-evaluations/judge-runs",
        headers={"x-actor-role": "operator"},
        json={
            "run_type": "offline_eval",
            "run_label": "Weekly recall sample",
            "items": [
                {
                    "evaluation_type": "retrieval_quality",
                    "target_type": "knowledge_retrieval",
                    "target_id": "kret_offline_sample",
                    "score": 0.72,
                    "dimensions": {"retrieval_quality": 0.72, "citation_relevance": 0.8},
                    "comments": "Offline benchmark sample passed but needs better evidence order.",
                    "metadata": {"benchmark": "winback_recall"},
                }
            ],
        },
    )
    assert run.status_code == 201, run.text
    payload = run.json()
    evaluation = payload["evaluations"][0]
    assert payload["run_type"] == "offline_eval"
    assert evaluation["source"] == "offline_eval_batch"
    assert evaluation["evaluated_by"] == "offline_eval_adapter_v1"
    assert evaluation["metadata"]["offline_eval"] is True
    assert evaluation["metadata"]["adapter"] == "offline_eval_adapter_v1"

    monitor = client.get("/api/v1/experiments/ai-quality-monitor", headers={"x-actor-role": "analyst"})
    assert monitor.status_code == 200
    readiness = monitor.json()["judge_readiness"]
    assert readiness["offline_eval_records"] == 1
    assert readiness["offline_average"] == 0.72


def test_ai_evaluation_judge_run_uses_configured_runtime_for_unscored_items(client, monkeypatch):
    calls = []

    class FakeRuntime:
        def request_text(self, payload):
            calls.append(payload)
            return '{"score": 0.91, "dimensions": {"answer_relevance": 0.91, "citation_coverage": 0.86}, "outcome": "useful", "comments": "Grounded and actionable."}'

    monkeypatch.setattr(
        "app.application.ai_evaluations.TextModelRuntimeResolver.resolve",
        lambda self, requested_model_profile_id=None: SimpleNamespace(
            model_profile_id="profile_judge",
            provider="gemini",
            model_name="gemini-flash-latest",
            selection_source="profile",
            runtime=FakeRuntime(),
        ),
    )

    run = client.post(
        "/api/v1/experiments/ai-evaluations/judge-runs",
        headers={"x-actor-role": "operator"},
        json={
            "run_type": "model_judge",
            "model_profile_id": "profile_judge",
            "rubric": {
                "criteria": {
                    "evidence": {"weight": 0.6, "description": "Use cited lifecycle evidence."},
                    "actionability": {"weight": 0.4},
                }
            },
            "items": [
                {
                    "evaluation_type": "answer_relevance",
                    "target_type": "email_campaign_draft",
                    "target_id": "email_judge_runtime",
                    "prompt": "Draft a winback email using the cited playbook.",
                    "response": "Come back for your saved reward. Evidence: [C1].",
                    "citations": [{"citation_id": "C1", "snippet": "Winback copy should mention saved rewards."}],
                }
            ],
        },
    )
    assert run.status_code == 201, run.text
    payload = run.json()
    assert calls and calls[0]["evaluation_type"] == "answer_relevance"
    assert calls[0]["rubric"]["criteria"]["evidence"]["weight"] == 0.6
    assert calls[0]["rubric"]["criteria"]["evidence"]["description"] == "Use cited lifecycle evidence."
    evaluation = payload["evaluations"][0]
    assert evaluation["score"] == 0.91
    assert evaluation["metadata"]["score_origin"] == "runtime"
    assert evaluation["metadata"]["model_profile_id"] == "profile_judge"
    assert evaluation["evaluated_by"] == "model_judge_adapter_v1:gemini:gemini-flash-latest"


def test_ai_evaluation_judge_run_rejects_unscored_items_without_runtime(client):
    rejected = client.post(
        "/api/v1/experiments/ai-evaluations/judge-runs",
        headers={"x-actor-role": "operator"},
        json={
            "run_type": "model_judge",
            "items": [
                {
                    "evaluation_type": "answer_relevance",
                    "target_type": "push_copy_draft",
                    "target_id": "missing_runtime_judge",
                    "prompt": "Judge this push copy.",
                    "response": "Come back now.",
                }
            ],
        },
    )
    assert rejected.status_code == 400
    assert "Ask AI runtime" in rejected.json()["detail"]

    listed = client.get(
        "/api/v1/experiments/ai-evaluations?target_id=missing_runtime_judge",
        headers={"x-actor-role": "analyst"},
    )
    assert listed.status_code == 200
    assert listed.json()["items"] == []


def test_ai_evaluation_judge_run_rejects_unknown_model_profile_without_writing_records(client):
    rejected = client.post(
        "/api/v1/experiments/ai-evaluations/judge-runs",
        headers={"x-actor-role": "operator"},
        json={
            "run_type": "model_judge",
            "model_profile_id": "missing_profile",
            "items": [
                {
                    "evaluation_type": "answer_relevance",
                    "target_type": "email_campaign_draft",
                    "target_id": "missing_profile_judge",
                    "score": 0.9,
                    "dimensions": {"answer_relevance": 0.9},
                }
            ],
        },
    )
    assert rejected.status_code == 400
    assert "missing_profile" in rejected.json()["detail"]

    listed = client.get(
        "/api/v1/experiments/ai-evaluations?target_id=missing_profile_judge",
        headers={"x-actor-role": "analyst"},
    )
    assert listed.status_code == 200
    assert listed.json()["items"] == []


def test_ai_quality_scheduler_tick_records_alert_check_and_monitor_latest_check(client):
    low_grade = client.post(
        "/api/v1/experiments/ai-evaluations/grade",
        headers={"x-actor-role": "operator"},
        json={
            "target_type": "push_copy_draft",
            "target_id": "scheduled_alert_low_quality",
            "prompt": "Draft a cited winback push.",
            "response": "Generic message without evidence.",
            "citations": [],
            "artifacts": [],
            "expected_artifact_type": "workflow",
            "generated_title": "Hi",
            "generated_body": "Come back.",
        },
    )
    assert low_grade.status_code == 201, low_grade.text

    scheduler = client.get("/api/v1/health/scheduler", headers={"x-actor-role": "analyst"})
    assert scheduler.status_code == 200
    assert any(item["job_id"] == "ai_quality_monitor" for item in scheduler.json()["items"])

    tick = client.post(
        "/api/v1/health/scheduler/tick",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2099-03-10T10:00:00"},
    )
    assert tick.status_code == 200, tick.text
    ai_quality_job = next(item for item in tick.json()["items"] if item["job_id"] == "ai_quality_monitor")
    assert ai_quality_job["status"] in {"warning", "critical"}
    assert ai_quality_job["result_summary"]["alert_count"] >= 1
    check_id = ai_quality_job["result_summary"]["check_id"]
    assert check_id.startswith("aiqcheck_")

    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        stored_check = repository.get_resource("ai_quality_alert_check", check_id)
        assert stored_check is not None
        assert stored_check["payload"]["format"] == "ai_quality_alert_check.v1"
        assert stored_check["payload"]["alert_count"] >= 1
        assert stored_check["payload"]["evaluated_at"] == "2099-03-10T10:00:00"
        assert stored_check["payload"]["monitor"]["generated_at"] == "2099-03-10T10:00:00"
        assert stored_check["payload"]["monitor"]["format"] == "ai_quality_monitor.v1"
        open_alerts = [
            item
            for item in repository.list_resources("ai_quality_alert")
            if item["payload"]["status"] == "open"
        ]
        assert any(item["payload"]["code"] == "low_average_score" for item in open_alerts)

    monitor = client.get("/api/v1/experiments/ai-quality-monitor", headers={"x-actor-role": "analyst"})
    assert monitor.status_code == 200
    latest = monitor.json()["latest_alert_check"]
    assert latest["check_id"] == check_id
    assert latest["alert_count"] >= 1
    assert latest["export"]["format"] == "ai_quality_alert_check.v1"
    exported = client.get(
        f"/api/v1/experiments/ai-quality-alert-checks/{check_id}/export",
        headers={"x-actor-role": "analyst"},
    )
    assert exported.status_code == 200
    assert exported.json()["format"] == "ai_quality_alert_check.v1"
    assert exported.json()["check"]["check_id"] == check_id

    same_day_tick = client.post(
        "/api/v1/health/scheduler/tick",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2099-03-10T12:00:00"},
    )
    assert same_day_tick.status_code == 200
    same_day_job = next(item for item in same_day_tick.json()["items"] if item["job_id"] == "ai_quality_monitor")
    assert same_day_job["status"] == "skipped"
    assert same_day_job["reason"] == "not_due"
    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        assert len(repository.list_resources("ai_quality_alert_check")) == 1

    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        for item in low_grade.json()["evaluations"]:
            assert repository.delete_resource("ai_evaluation_record", item["evaluation_id"]) is True
        session.commit()

    second_tick = client.post(
        "/api/v1/health/scheduler/tick",
        headers={"x-actor-role": "operator"},
        json={"reference_time": "2099-03-11T10:00:00"},
    )
    assert second_tick.status_code == 200
    second_job = next(item for item in second_tick.json()["items"] if item["job_id"] == "ai_quality_monitor")
    assert second_job["status"] == "critical"
    second_check_id = second_job["result_summary"]["check_id"]

    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        stored_second_check = repository.get_resource("ai_quality_alert_check", second_check_id)
        assert stored_second_check is not None
        assert stored_second_check["payload"]["evaluated_at"] == "2099-03-11T10:00:00"
        assert stored_second_check["payload"]["monitor"]["generated_at"] == "2099-03-11T10:00:00"
        resolved_low_average = repository.get_resource("ai_quality_alert", "ai_quality:low_average_score")
        assert resolved_low_average is not None
        assert resolved_low_average["payload"]["status"] == "resolved"
        resolved_low_average_payload = dict(resolved_low_average["payload"])
        no_records_alert = repository.get_resource("ai_quality_alert", "ai_quality:no_evaluation_records")
        assert no_records_alert is not None
        no_records_payload = dict(no_records_alert["payload"])
        assert repository.get_resource("ai_quality_alert", "ai_quality:high_negative_feedback") is None
        session.commit()

    late_grade = client.post(
        "/api/v1/experiments/ai-evaluations/grade",
        headers={"x-actor-role": "operator"},
        json={
            "target_type": "push_copy_draft",
            "target_id": "late_negative_feedback_target",
            "prompt": "Draft a cited winback push.",
            "response": "Return for a timed reward. Source: segment A churn model.",
            "citations": [{"id": "src_segment_a"}],
            "artifacts": [{"id": "artifact_push_draft"}],
            "expected_artifact_type": "workflow",
            "generated_title": "Reward waiting",
            "generated_body": "Come back today for a limited reward based on your recent play history.",
        },
    )
    assert late_grade.status_code == 201, late_grade.text
    late_feedback = client.post(
        "/api/v1/experiments/ai-feedback",
        headers={"x-actor-role": "operator"},
        json={
            "feedback_type": "operator_edit",
            "target_type": "push_copy_draft",
            "target_id": "late_negative_feedback_target",
            "comments": "The offer still needed stronger personalization.",
        },
    )
    assert late_feedback.status_code == 201, late_feedback.text

    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        for item in late_grade.json()["evaluations"]:
            record = repository.get_resource("ai_evaluation_record", item["evaluation_id"])
            assert record is not None
            payload = {**record["payload"], "recorded_at": "2099-03-10T18:00:00"}
            repository.upsert_resource("ai_evaluation_record", item["evaluation_id"], status="recorded", name=record["name"], payload=payload)
        feedback_id = late_feedback.json()["feedback_id"]
        feedback_record = repository.get_resource("ai_feedback_record", feedback_id)
        assert feedback_record is not None
        feedback_payload = {**feedback_record["payload"], "recorded_at": "2099-03-10T18:00:00"}
        repository.upsert_resource("ai_feedback_record", feedback_id, status="recorded", name=feedback_record["name"], payload=feedback_payload)
        repository.upsert_resource(
            "ai_evaluation_record",
            "aieval_historical_backfill",
            status="recorded",
            name="answer_relevance",
            payload={
                "evaluation_id": "aieval_historical_backfill",
                "evaluation_type": "answer_relevance",
                "target_type": "push_copy_draft",
                "target_id": "historical_backfill",
                "outcome": "accepted",
                "score": 0.95,
                "score_source": "manual",
                "dimensions": {
                    "answer_relevance": 0.95,
                    "campaign_copy_usefulness": 0.95,
                    "citation_coverage": 0.95,
                    "prompt_to_artifact_completion": 0.95,
                    "retrieval_quality": 0.95,
                },
                "citation_ids": ["historical_source"],
                "artifact_ids": ["historical_artifact"],
                "prompt_summary": "Historical check record.",
                "response_summary": "Historical response.",
                "comments": "",
                "source": "fixture",
                "metadata": {},
                "evaluated_by": "fixture",
                "status": "recorded",
                "recorded_at": "2099-03-08T10:00:00",
                "export": {"format": "ai_evaluation_record.v1", "resource_id": "aieval_historical_backfill"},
            },
        )
        repository.upsert_resource(
            "ai_feedback_record",
            "aifb_historical_monitor",
            status="recorded",
            name="operator_approval",
            payload={
                "feedback_id": "aifb_historical_monitor",
                "feedback_type": "operator_approval",
                "target_type": "push_copy_draft",
                "target_id": "historical_backfill",
                "sentiment": "positive",
                "weight": 1.0,
                "rating": None,
                "citation_ids": [],
                "artifact_ids": [],
                "related_evaluation_id": "aieval_historical_backfill",
                "change_summary": "",
                "outcome_metrics": {},
                "comments": "Historical monitor feedback.",
                "source": "fixture",
                "metadata": {},
                "recorded_by": "fixture",
                "status": "recorded",
                "recorded_at": "2099-03-08T12:00:00",
                "export": {"format": "ai_feedback_record.v1", "resource_id": "aifb_historical_monitor"},
            },
        )
        for index in range(MAX_LIST_LIMIT + 5):
            evaluation_id = f"aieval_future_noise_{index}"
            repository.upsert_resource(
                "ai_evaluation_record",
                evaluation_id,
                status="recorded",
                name="answer_relevance",
                payload={
                    "evaluation_id": evaluation_id,
                    "evaluation_type": "answer_relevance",
                    "target_type": "push_copy_draft",
                    "target_id": f"future_noise_{index}",
                    "outcome": "accepted",
                    "score": 0.99,
                    "score_source": "manual",
                    "dimensions": {"answer_relevance": 0.99},
                    "citation_ids": [],
                    "artifact_ids": [],
                    "prompt_summary": "Future noise.",
                    "response_summary": "Future response.",
                    "comments": "",
                    "source": "fixture",
                    "metadata": {},
                    "evaluated_by": "fixture",
                    "status": "recorded",
                    "recorded_at": "2099-03-10T19:00:00",
                    "export": {"format": "ai_evaluation_record.v1", "resource_id": evaluation_id},
                },
            )
            old_evaluation_noise_id = f"aieval_old_monitor_noise_{index}"
            repository.upsert_resource(
                "ai_evaluation_record",
                old_evaluation_noise_id,
                status="recorded",
                name="answer_relevance",
                payload={
                    "evaluation_id": old_evaluation_noise_id,
                    "evaluation_type": "answer_relevance",
                    "target_type": "push_copy_draft",
                    "target_id": f"old_monitor_noise_{index}",
                    "outcome": "accepted",
                    "score": 0.99,
                    "score_source": "manual",
                    "dimensions": {
                        "answer_relevance": 0.99,
                        "campaign_copy_usefulness": 0.99,
                        "citation_coverage": 0.99,
                        "prompt_to_artifact_completion": 0.99,
                        "retrieval_quality": 0.99,
                    },
                    "citation_ids": [],
                    "artifact_ids": [],
                    "prompt_summary": "Older monitor noise.",
                    "response_summary": "Older response.",
                    "comments": "",
                    "source": "fixture",
                    "metadata": {},
                    "evaluated_by": "fixture",
                    "status": "recorded",
                    "recorded_at": "2099-03-07T10:00:00",
                    "export": {"format": "ai_evaluation_record.v1", "resource_id": old_evaluation_noise_id},
                },
            )
            feedback_noise_id = f"aifb_non_monitor_noise_{index}"
            repository.upsert_resource(
                "ai_feedback_record",
                feedback_noise_id,
                status="recorded",
                name="operator_edit",
                payload={
                    "feedback_id": feedback_noise_id,
                    "feedback_type": "operator_edit",
                    "target_type": "support_ticket",
                    "target_id": f"non_monitor_noise_{index}",
                    "sentiment": "negative",
                    "weight": -1.0,
                    "rating": None,
                    "citation_ids": [],
                    "artifact_ids": [],
                    "related_evaluation_id": "",
                    "change_summary": "",
                    "outcome_metrics": {},
                    "comments": "Non-monitor feedback noise.",
                    "source": "fixture",
                    "metadata": {},
                    "recorded_by": "fixture",
                    "status": "recorded",
                    "recorded_at": "2099-03-08T13:00:00",
                    "export": {"format": "ai_feedback_record.v1", "resource_id": feedback_noise_id},
                },
            )
            monitor_feedback_noise_id = f"aifb_old_monitor_noise_{index}"
            repository.upsert_resource(
                "ai_feedback_record",
                monitor_feedback_noise_id,
                status="recorded",
                name="operator_approval",
                payload={
                    "feedback_id": monitor_feedback_noise_id,
                    "feedback_type": "operator_approval",
                    "target_type": "push_copy_draft",
                    "target_id": f"old_monitor_noise_{index}",
                    "sentiment": "positive",
                    "weight": 1.0,
                    "rating": None,
                    "citation_ids": [],
                    "artifact_ids": [],
                    "related_evaluation_id": "",
                    "change_summary": "",
                    "outcome_metrics": {},
                    "comments": "Older monitor feedback noise.",
                    "source": "fixture",
                    "metadata": {},
                    "recorded_by": "fixture",
                    "status": "recorded",
                    "recorded_at": "2099-03-07T12:00:00",
                    "export": {"format": "ai_feedback_record.v1", "resource_id": monitor_feedback_noise_id},
                },
            )
        session.commit()

    with db_module.get_session_factory()() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        backfill = AIEvaluationService(repository).run_alert_check(reference_time="2099-03-09T10:00:00")
        assert backfill["check_id"] != second_check_id
        assert not any(alert["code"] == "high_negative_feedback" for alert in backfill["alerts"])
        assert backfill["summary"]["total_records"] == MAX_LIST_LIMIT
        assert backfill["monitor"]["recent_records"][0]["evaluation_id"] == "aieval_historical_backfill"
        assert backfill["monitor"]["feedback_summary"]["total_records"] == MAX_LIST_LIMIT
        assert backfill["monitor"]["feedback_summary"]["positive_rate"] == 1.0
        assert any(
            item["target_id"] == "historical_backfill"
            for item in backfill["monitor"]["feedback_learning"]["top_positive_targets"]
        )
        assert repository.get_resource("ai_quality_alert", "ai_quality:low_average_score")["payload"] == resolved_low_average_payload
        assert repository.get_resource("ai_quality_alert", "ai_quality:no_evaluation_records")["payload"] == no_records_payload
        assert repository.get_resource("ai_quality_alert", "ai_quality:high_negative_feedback") is None
        session.commit()

    latest_after_backfill = client.get("/api/v1/experiments/ai-quality-monitor", headers={"x-actor-role": "analyst"})
    assert latest_after_backfill.status_code == 200
    assert latest_after_backfill.json()["latest_alert_check"]["check_id"] == second_check_id


def test_ai_quality_monitor_returns_alerts_diagnostics_and_export(client):
    low_grade = client.post(
        "/api/v1/experiments/ai-evaluations/grade",
        headers={"x-actor-role": "operator"},
        json={
            "target_type": "push_copy_draft",
            "target_id": "draft_low_quality",
            "prompt": "Draft a winback push with evidence.",
            "response": "Generic message without evidence.",
            "citations": [],
            "artifacts": [],
            "expected_artifact_type": "workflow",
            "generated_title": "Hi",
            "generated_body": "Come back.",
        },
    )
    assert low_grade.status_code == 201, low_grade.text
    feedback = client.post(
        "/api/v1/experiments/ai-feedback",
        headers={"x-actor-role": "operator"},
        json={
            "feedback_type": "operator_edit",
            "target_type": "push_copy_draft",
            "target_id": "draft_low_quality",
            "comments": "Needed evidence and a clearer offer.",
        },
    )
    assert feedback.status_code == 201, feedback.text

    monitor = client.get("/api/v1/experiments/ai-quality-monitor", headers={"x-actor-role": "analyst"})
    assert monitor.status_code == 200, monitor.text
    payload = monitor.json()
    assert payload["format"] == "ai_quality_monitor.v1"
    assert payload["status"] in {"warning", "critical"}
    assert payload["summary"]["total_records"] >= 4
    assert payload["feedback_summary"]["negative_rate"] == 1.0
    assert payload["feedback_learning"]["recommendations"]
    assert payload["dimension_cards"]
    assert payload["recent_records"][0]["evaluation_id"].startswith("aieval_")
    assert any(alert["code"] in {"high_negative_feedback", "low_average_score"} for alert in payload["alerts"])
    assert payload["export"]["format"] == "ai_quality_monitor.v1"

    exported = client.get("/api/v1/experiments/ai-quality-monitor/export", headers={"x-actor-role": "analyst"})
    assert exported.status_code == 200, exported.text
    export_payload = exported.json()
    assert export_payload["format"] == "ai_quality_monitor.v1"
    assert export_payload["monitor"]["recent_records"]


def test_ai_quality_monitor_is_project_scoped(client):
    created = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "operator", "x-project-id": "project-a"},
        json={
            **_evaluation_payload(),
            "target_type": "knowledge_retrieval",
            "target_id": "kret_project_a",
        },
    )
    assert created.status_code == 201

    same_project = client.get(
        "/api/v1/experiments/ai-quality-monitor",
        headers={"x-actor-role": "analyst", "x-project-id": "project-a"},
    )
    assert same_project.status_code == 200
    assert same_project.json()["summary"]["total_records"] == 1

    other_project = client.get(
        "/api/v1/experiments/ai-quality-monitor",
        headers={"x-actor-role": "analyst", "x-project-id": "project-b"},
    )
    assert other_project.status_code == 200
    assert other_project.json()["summary"]["total_records"] == 0
    assert any(alert["code"] == "no_evaluation_records" for alert in other_project.json()["alerts"])


def test_ai_quality_monitor_ignores_unrelated_targets_and_detects_source_model_judge(client):
    unrelated = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "operator"},
        json={
            **_evaluation_payload(),
            "target_type": "support_chat_summary",
            "target_id": "unrelated_eval",
            "score": 1.0,
            "dimensions": {"answer_relevance": 1.0, "citation_coverage": 1.0, "retrieval_quality": 1.0},
        },
    )
    assert unrelated.status_code == 201
    unrelated_feedback = client.post(
        "/api/v1/experiments/ai-feedback",
        headers={"x-actor-role": "operator"},
        json={
            "feedback_type": "operator_edit",
            "target_type": "support_chat_summary",
            "target_id": "unrelated_feedback",
            "comments": "This unrelated edit must not affect growth quality.",
        },
    )
    assert unrelated_feedback.status_code == 201

    empty_monitor = client.get("/api/v1/experiments/ai-quality-monitor", headers={"x-actor-role": "analyst"})
    assert empty_monitor.status_code == 200
    empty_payload = empty_monitor.json()
    assert empty_payload["summary"]["total_records"] == 0
    assert empty_payload["feedback_summary"]["total_records"] == 0
    assert empty_payload["scope"]["ignored_non_monitor_records"] == 1
    assert any(alert["code"] == "no_evaluation_records" for alert in empty_payload["alerts"])

    deterministic = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "operator"},
        json={
            "evaluation_type": "answer_relevance",
            "target_type": "push_copy_draft",
            "target_id": "judge_target",
            "score": 0.9,
            "dimensions": {"answer_relevance": 0.9},
            "source": "auto_grader",
            "evaluated_by": "deterministic_ai_grader_v1",
        },
    )
    assert deterministic.status_code == 201
    model_judge = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "operator"},
        json={
            "evaluation_type": "answer_relevance",
            "target_type": "push_copy_draft",
            "target_id": "judge_target",
            "score": 0.4,
            "dimensions": {"answer_relevance": 0.4},
            "source": "model_judge_provider",
        },
    )
    assert model_judge.status_code == 201

    monitor = client.get("/api/v1/experiments/ai-quality-monitor", headers={"x-actor-role": "analyst"})
    assert monitor.status_code == 200
    payload = monitor.json()
    assert payload["summary"]["total_records"] == 2
    assert payload["feedback_summary"]["total_records"] == 0
    assert payload["judge_readiness"]["deterministic_grader_records"] == 1
    assert payload["judge_readiness"]["model_judge_records"] == 1
    assert any(alert["code"] == "model_judge_drift" for alert in payload["alerts"])


def test_ai_evaluation_auto_grader_requires_meaningful_prompt_and_response(client):
    missing_prompt = client.post(
        "/api/v1/experiments/ai-evaluations/grade",
        headers={"x-actor-role": "operator"},
        json={"target_type": "knowledge_retrieval", "response": "Evidence: [C1]."},
    )
    assert missing_prompt.status_code == 400
    assert "prompt" in missing_prompt.json()["detail"]

    denied = client.post(
        "/api/v1/experiments/ai-evaluations/grade",
        headers={"x-actor-role": "invalid"},
        json={
            "target_type": "knowledge_retrieval",
            "prompt": "Find winback evidence.",
            "response": "Evidence: [C1].",
        },
    )
    assert denied.status_code == 403


def test_ai_evaluation_validation_permissions_and_project_scope(client):
    invalid_type = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "analyst"},
        json={**_evaluation_payload(), "evaluation_type": "unknown"},
    )
    assert invalid_type.status_code == 400

    invalid_dimension = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "operator"},
        json={**_evaluation_payload(), "dimensions": {"citation_coverage": 1.1}},
    )
    assert invalid_dimension.status_code == 400

    created = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "operator", "x-project-id": "project-a"},
        json=_evaluation_payload(),
    )
    assert created.status_code == 201
    evaluation_id = created.json()["evaluation_id"]

    same_project = client.get(
        f"/api/v1/experiments/ai-evaluations/{evaluation_id}",
        headers={"x-actor-role": "analyst", "x-project-id": "project-a"},
    )
    assert same_project.status_code == 200

    other_project = client.get(
        f"/api/v1/experiments/ai-evaluations/{evaluation_id}",
        headers={"x-actor-role": "analyst", "x-project-id": "project-b"},
    )
    assert other_project.status_code == 404

    denied = client.post(
        "/api/v1/experiments/ai-evaluations",
        headers={"x-actor-role": "invalid"},
        json=_evaluation_payload(),
    )
    assert denied.status_code == 403
