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


def _knowledge_payload() -> dict:
    body = "\n\n".join(
        [
            "Winback campaign brief: target lapsed players with a clear reason to return and a low-friction reward.",
            "Voice guidance: direct, useful, and specific. Avoid pressure language. Mention the saved game state.",
            "Prior learning: weekend evening reminders performed better than weekday morning reminders for returning players.",
            "Experiment note: holdout comparisons should keep churn-risk and VIP-level guardrails attached.",
        ]
        * 8
    )
    return {
        "title": "Q2 Winback Playbook",
        "content": body,
        "source_type": "playbook",
        "source_name": "Lifecycle Team",
        "source_uri": "internal://growth/q2-winback",
        "tags": ["Winback", "Push", "winback"],
        "visibility": "project",
        "provenance": {"owner": "growth", "approved": True},
        "metadata": {"channel": "push"},
    }


def test_knowledge_document_ingestion_chunks_archive_and_export(client):
    create = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator"},
        json=_knowledge_payload(),
    )
    assert create.status_code == 201
    document = create.json()
    assert document["document_id"].startswith("kdoc_")
    assert document["title"] == "Q2 Winback Playbook"
    assert document["source_type"] == "playbook"
    assert document["tags"] == ["winback", "push"]
    assert document["chunk_count"] > 1
    assert len(document["content_hash"]) == 64
    assert document["ingestion_job"]["status"] == "completed"
    assert document["export"]["format"] == "knowledge_document.v1"
    assert document["audit_id"] > 0

    document_id = document["document_id"]
    listed = client.get("/api/v1/knowledge/documents", headers={"x-actor-role": "analyst"})
    assert listed.status_code == 200
    assert [item["document_id"] for item in listed.json()["items"]] == [document_id]

    chunks = client.get(f"/api/v1/knowledge/documents/{document_id}/chunks", headers={"x-actor-role": "analyst"})
    assert chunks.status_code == 200
    chunk_items = chunks.json()["items"]
    assert len(chunk_items) == document["chunk_count"]
    assert chunk_items[0]["ordinal"] == 1
    assert chunk_items[0]["embedding"]["status"] == "ready"
    assert chunk_items[0]["embedding"]["model"] == "local_semantic_hash_v1"
    assert chunk_items[0]["embedding"]["dimensions"] == 1024
    assert "Winback campaign brief" in chunk_items[0]["text"]

    exported = client.get(f"/api/v1/knowledge/documents/{document_id}/export", headers={"x-actor-role": "operator"})
    assert exported.status_code == 200
    export_payload = exported.json()
    assert export_payload["format"] == "knowledge_document.v1"
    assert export_payload["document"]["document_id"] == document_id
    assert len(export_payload["chunks"]) == document["chunk_count"]

    archived = client.post(f"/api/v1/knowledge/documents/{document_id}/archive", headers={"x-actor-role": "operator"})
    assert archived.status_code == 200
    assert archived.json()["status"] == "archived"

    active_only = client.get("/api/v1/knowledge/documents", headers={"x-actor-role": "analyst"})
    assert active_only.status_code == 200
    assert active_only.json()["items"] == []

    with_archived = client.get(
        "/api/v1/knowledge/documents?include_archived=true",
        headers={"x-actor-role": "analyst"},
    )
    assert with_archived.status_code == 200
    assert with_archived.json()["items"][0]["status"] == "archived"


def test_knowledge_write_requires_operator_permissions(client):
    denied = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "analyst"},
        json=_knowledge_payload(),
    )
    assert denied.status_code == 403


def test_knowledge_documents_are_project_scoped(client):
    created = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator", "x-project-id": "project-a"},
        json=_knowledge_payload(),
    )
    assert created.status_code == 201
    document_id = created.json()["document_id"]

    same_project = client.get(
        f"/api/v1/knowledge/documents/{document_id}",
        headers={"x-actor-role": "analyst", "x-project-id": "project-a"},
    )
    assert same_project.status_code == 200

    other_project = client.get(
        f"/api/v1/knowledge/documents/{document_id}",
        headers={"x-actor-role": "analyst", "x-project-id": "project-b"},
    )
    assert other_project.status_code == 404


def test_knowledge_retrieval_returns_citations_and_exportable_evidence_pack(client):
    first = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator"},
        json=_knowledge_payload(),
    )
    assert first.status_code == 201
    second = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator"},
        json={
            "title": "VIP Loyalty Notes",
            "content": "VIP concierge email copy should focus on status benefits, account manager access, and premium event windows.",
            "source_type": "campaign_brief",
            "tags": ["vip", "email"],
        },
    )
    assert second.status_code == 201

    retrieval = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst"},
        json={"query": "weekend evening push reminder saved game", "top_k": 3, "tags": ["push"]},
    )
    assert retrieval.status_code == 201
    payload = retrieval.json()
    assert payload["retrieval_id"].startswith("kret_")
    assert payload["retrieval_mode"] == "lexical_v1"
    assert payload["result_count"] >= 1
    assert payload["export"]["format"] == "knowledge_evidence_pack.v1"
    assert payload["context_pack"]["format"] == "knowledge_context_pack.v1"
    assert payload["context_pack"]["citation_count"] == payload["result_count"]
    assert payload["citations"][0]["citation_id"] == "C1"
    assert payload["citations"][0]["citation"].startswith("[C1] Q2 Winback Playbook chunk")
    assert payload["citations"][0]["document_title"] == "Q2 Winback Playbook"
    assert "weekend" in payload["citations"][0]["match_terms"]
    assert "weekend evening reminders" in payload["citations"][0]["snippet"]

    retrieval_id = payload["retrieval_id"]
    listed = client.get("/api/v1/knowledge/retrievals", headers={"x-actor-role": "analyst"})
    assert listed.status_code == 200
    assert listed.json()["items"][0]["retrieval_id"] == retrieval_id

    exported = client.get(f"/api/v1/knowledge/retrievals/{retrieval_id}/export", headers={"x-actor-role": "analyst"})
    assert exported.status_code == 200
    export_payload = exported.json()
    assert export_payload["format"] == "knowledge_evidence_pack.v1"
    assert export_payload["retrieval"]["retrieval_id"] == retrieval_id
    assert export_payload["retrieval"]["citations"][0]["chunk_id"] == payload["citations"][0]["chunk_id"]


def test_knowledge_hybrid_retrieval_uses_semantic_vectors_and_reranking(client):
    first = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator"},
        json={
            "title": "Lapsed Player Winback Playbook",
            "content": (
                "Lapsed players respond when the campaign names their saved checkpoint, "
                "shows the returning reward, and reminds them that progress is waiting."
            ),
            "source_type": "playbook",
            "tags": ["push", "winback"],
        },
    )
    assert first.status_code == 201
    chunks = client.get(
        f"/api/v1/knowledge/documents/{first.json()['document_id']}/chunks",
        headers={"x-actor-role": "analyst"},
    )
    assert chunks.status_code == 200
    embedding = chunks.json()["items"][0]["embedding"]
    assert embedding["status"] == "ready"
    assert embedding["model"] == "local_semantic_hash_v1"
    assert embedding["vector_ref"].startswith("inline:local_semantic_hash_v1:")

    second = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator"},
        json={
            "title": "Subscription Billing FAQ",
            "content": "Invoice receipts, refund timing, tax handling, and payment retry notes for account support.",
            "source_type": "faq",
            "tags": ["billing"],
        },
    )
    assert second.status_code == 201

    lexical = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst"},
        json={"query": "reactivation bonus progression", "top_k": 3, "retrieval_mode": "lexical_v1"},
    )
    assert lexical.status_code == 201
    assert lexical.json()["retrieval_mode"] == "lexical_v1"
    assert lexical.json()["result_count"] == 0

    hybrid = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst"},
        json={"query": "reactivation bonus progression", "top_k": 3, "retrieval_mode": "hybrid_v1"},
    )
    assert hybrid.status_code == 201
    payload = hybrid.json()
    assert payload["retrieval_mode"] == "hybrid_v1"
    assert payload["context_pack"]["retrieval_mode"] == "hybrid_v1"
    assert payload["result_count"] == 1
    citation = payload["citations"][0]
    assert citation["document_title"] == "Lapsed Player Winback Playbook"
    assert citation["ranking_signals"]["semantic_score"] > 0
    assert citation["ranking_signals"]["rerank_score"] == citation["score"]
    assert citation["ranking_signals"]["vector_model"] == "local_semantic_hash_v1"


def test_knowledge_retrieval_rejects_unknown_retrieval_mode(client):
    created = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator"},
        json=_knowledge_payload(),
    )
    assert created.status_code == 201

    retrieval = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst"},
        json={"query": "weekend evening push", "retrieval_mode": "unsupported"},
    )
    assert retrieval.status_code == 400
    assert "retrieval_mode" in retrieval.json()["detail"]


def test_knowledge_retrieval_is_project_scoped_and_excludes_archived_by_default(client):
    created = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator", "x-project-id": "project-a"},
        json=_knowledge_payload(),
    )
    assert created.status_code == 201
    document_id = created.json()["document_id"]

    other_project = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst", "x-project-id": "project-b"},
        json={"query": "weekend evening push"},
    )
    assert other_project.status_code == 201
    assert other_project.json()["result_count"] == 0

    archived = client.post(
        f"/api/v1/knowledge/documents/{document_id}/archive",
        headers={"x-actor-role": "operator", "x-project-id": "project-a"},
    )
    assert archived.status_code == 200

    active_only = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst", "x-project-id": "project-a"},
        json={"query": "weekend evening push"},
    )
    assert active_only.status_code == 201
    assert active_only.json()["result_count"] == 0

    with_archived = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst", "x-project-id": "project-a"},
        json={"query": "weekend evening push", "include_archived": True},
    )
    assert with_archived.status_code == 201
    assert with_archived.json()["result_count"] > 0
