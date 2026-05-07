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
    assert chunk_items[0]["embedding"]["status"] == "pending"
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
