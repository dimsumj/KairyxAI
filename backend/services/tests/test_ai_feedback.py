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


def _create_doc(client, title: str) -> dict:
    created = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator"},
        json={
            "title": title,
            "content": "Winback push reward guidance: mention saved progress and a fast comeback bonus.",
            "source_type": "playbook",
            "tags": ["push", "winback"],
        },
    )
    assert created.status_code == 201
    return created.json()


def test_ai_feedback_records_summary_export_and_boosts_knowledge_retrieval(client):
    alpha = _create_doc(client, "Alpha Winback Playbook")
    beta = _create_doc(client, "Beta Winback Playbook")

    baseline = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst"},
        json={"query": "winback push reward", "top_k": 2},
    )
    assert baseline.status_code == 201
    assert baseline.json()["citations"][0]["document_id"] == alpha["document_id"]

    beta_chunks = client.get(
        f"/api/v1/knowledge/documents/{beta['document_id']}/chunks",
        headers={"x-actor-role": "analyst"},
    )
    assert beta_chunks.status_code == 200
    beta_chunk_id = beta_chunks.json()["items"][0]["chunk_id"]

    feedback = client.post(
        "/api/v1/experiments/ai-feedback",
        headers={"x-actor-role": "analyst"},
        json={
            "feedback_type": "operator_approval",
            "target_type": "knowledge_chunk",
            "target_id": beta_chunk_id,
            "citation_ids": ["C1"],
            "artifact_ids": [beta["document_id"]],
            "comments": "This source had the clearest winback push guidance.",
            "outcome_metrics": {"copy_acceptance_rate": 0.9},
        },
    )
    assert feedback.status_code == 201
    feedback_payload = feedback.json()
    assert feedback_payload["feedback_id"].startswith("aifb_")
    assert feedback_payload["sentiment"] == "positive"
    assert feedback_payload["weight"] == 1.0
    assert feedback_payload["export"]["format"] == "ai_feedback_record.v1"

    boosted = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst"},
        json={"query": "winback push reward", "top_k": 2},
    )
    assert boosted.status_code == 201
    top = boosted.json()["citations"][0]
    assert top["document_id"] == beta["document_id"]
    assert top["feedback_boost"] == 1.0
    assert top["ranking_signals"]["feedback_boost"] == 1.0

    summary = client.get("/api/v1/experiments/ai-feedback/summary", headers={"x-actor-role": "analyst"})
    assert summary.status_code == 200
    summary_payload = summary.json()
    assert summary_payload["total_records"] == 1
    assert summary_payload["positive_rate"] == 1.0
    assert summary_payload["target_weight_scores"][f"knowledge_chunk:{beta_chunk_id}"] == 1.0
    assert summary_payload["metric_averages"]["copy_acceptance_rate"] == 0.9

    exported = client.get(
        f"/api/v1/experiments/ai-feedback/{feedback_payload['feedback_id']}/export",
        headers={"x-actor-role": "analyst"},
    )
    assert exported.status_code == 200
    assert exported.json()["format"] == "ai_feedback_record.v1"


def test_ai_feedback_validation_and_project_scope(client):
    invalid = client.post(
        "/api/v1/experiments/ai-feedback",
        headers={"x-actor-role": "operator"},
        json={"feedback_type": "unknown", "target_type": "knowledge_chunk"},
    )
    assert invalid.status_code == 400

    invalid_weight = client.post(
        "/api/v1/experiments/ai-feedback",
        headers={"x-actor-role": "operator"},
        json={"feedback_type": "rating", "target_type": "knowledge_chunk", "weight": 2},
    )
    assert invalid_weight.status_code == 422

    created = client.post(
        "/api/v1/experiments/ai-feedback",
        headers={"x-actor-role": "operator", "x-project-id": "project-a"},
        json={
            "feedback_type": "rating",
            "target_type": "knowledge_document",
            "target_id": "kdoc_example",
            "rating": 0.2,
        },
    )
    assert created.status_code == 201
    feedback_id = created.json()["feedback_id"]
    assert created.json()["sentiment"] == "negative"

    same_project = client.get(
        f"/api/v1/experiments/ai-feedback/{feedback_id}",
        headers={"x-actor-role": "analyst", "x-project-id": "project-a"},
    )
    assert same_project.status_code == 200

    other_project = client.get(
        f"/api/v1/experiments/ai-feedback/{feedback_id}",
        headers={"x-actor-role": "analyst", "x-project-id": "project-b"},
    )
    assert other_project.status_code == 404
