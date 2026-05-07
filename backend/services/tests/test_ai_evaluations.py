from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.core import db as db_module
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
