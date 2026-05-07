from __future__ import annotations

from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from app.application.knowledge import KNOWLEDGE_VECTOR_RECORD_RESOURCE_TYPE
from app.core import db as db_module
from app.core.db import session_scope
from app.main import create_app
from app.core.settings import Settings, validate_runtime_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository


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


@contextmanager
def _client_with_env(monkeypatch, tmp_path, **env):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATA_BACKEND_MODE", "mock")
    monkeypatch.setenv("CONTROL_PLANE_DATABASE_URL", f"sqlite:///{tmp_path / 'control_plane.db'}")
    monkeypatch.setenv("KAIRYX_LOCAL_DB_PATH", str(tmp_path / "local_jobs.db"))
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))
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
    assert chunk_items[0]["embedding"]["provider"] == "local_hash"
    assert chunk_items[0]["embedding"]["vector_store"] == "control_plane"
    assert chunk_items[0]["embedding"]["vector_index_id"] == "kairyx_knowledge_default"
    assert chunk_items[0]["embedding"]["vector_record_id"].endswith(chunk_items[0]["chunk_id"])
    assert chunk_items[0]["embedding"]["dimensions"] == 1024
    assert chunk_items[0]["embedding"]["adapter"]["adapter_kind"] == "control_plane"
    assert chunk_items[0]["embedding"]["adapter"]["sync_status"] == "control_plane_synced"
    assert "Winback campaign brief" in chunk_items[0]["text"]

    indexes = client.get("/api/v1/knowledge/vector-indexes", headers={"x-actor-role": "analyst"})
    assert indexes.status_code == 200
    index = indexes.json()["items"][0]
    assert index["index_id"] == "kairyx_knowledge_default"
    assert index["embedding_provider"] == "local_hash"
    assert index["vector_store"] == "control_plane"
    assert index["adapter_kind"] == "control_plane"
    assert index["readiness_status"] == "ready"
    assert index["record_count"] == document["chunk_count"]

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
    assert payload["vector_index"]["index_id"] == "kairyx_knowledge_default"
    assert payload["vector_index"]["record_count"] == 2
    assert payload["result_count"] == 1
    citation = payload["citations"][0]
    assert citation["document_title"] == "Lapsed Player Winback Playbook"
    assert citation["ranking_signals"]["semantic_score"] > 0
    assert citation["ranking_signals"]["rerank_score"] == citation["score"]
    assert citation["ranking_signals"]["vector_status"] == "ready"
    assert citation["ranking_signals"]["vector_model"] == "local_semantic_hash_v1"
    assert citation["ranking_signals"]["vector_store"] == "control_plane"


def test_knowledge_hybrid_retrieval_reports_recomputed_fallback_when_vector_record_missing(client):
    created = client.post(
        "/api/v1/knowledge/documents",
        headers={"x-actor-role": "operator"},
        json={
            "title": "Legacy Knowledge Playbook",
            "content": "Legacy winback copy mentions saved progress and a short return reminder.",
            "source_type": "playbook",
            "tags": ["push", "winback"],
        },
    )
    assert created.status_code == 201
    vector_record_id = created.json()["chunks"][0]["embedding"]["vector_record_id"]
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        assert repository.delete_resource(KNOWLEDGE_VECTOR_RECORD_RESOURCE_TYPE, vector_record_id) is True

    retrieval = client.post(
        "/api/v1/knowledge/retrievals",
        headers={"x-actor-role": "analyst"},
        json={"query": "saved progress return reminder", "retrieval_mode": "hybrid_v1"},
    )
    assert retrieval.status_code == 201
    payload = retrieval.json()
    assert payload["result_count"] == 1
    assert payload["vector_index"]["index_id"] == "recomputed_fallback"
    assert payload["vector_index"]["status"] == "fallback"
    assert payload["vector_index"]["storage_mode"] == "recomputed_fallback"
    assert payload["vector_index"]["record_count"] == 0
    signals = payload["citations"][0]["ranking_signals"]
    assert signals["vector_status"] == "recomputed_fallback"
    assert signals["vector_index_id"] is None


def test_knowledge_provider_vector_config_materializes_exportable_shadow_index(monkeypatch, tmp_path):
    with _client_with_env(
        monkeypatch,
        tmp_path,
        KNOWLEDGE_EMBEDDING_PROVIDER="openai",
        KNOWLEDGE_EMBEDDING_MODEL="text-embedding-3-small",
        KNOWLEDGE_VECTOR_STORE="pgvector",
        KNOWLEDGE_VECTOR_INDEX="growth_playbooks",
        KNOWLEDGE_VECTOR_NAMESPACE="lifecycle",
        KNOWLEDGE_VECTOR_SECRET_REF="secret://knowledge/vector",
    ) as client:
        created = client.post(
            "/api/v1/knowledge/documents",
            headers={"x-actor-role": "operator"},
            json={
                "title": "VIP Reactivation Playbook",
                "content": (
                    "VIP reactivation copy should reference account status, saved progression, "
                    "and a concierge reward without creating pressure."
                ),
                "source_type": "playbook",
                "tags": ["vip", "push"],
            },
        )
        assert created.status_code == 201
        document = created.json()
        embedding = document["chunks"][0]["embedding"]
        assert embedding["provider"] == "openai"
        assert embedding["model"] == "text-embedding-3-small"
        assert embedding["vector_store"] == "pgvector"
        assert embedding["vector_index_id"] == "growth_playbooks"
        assert embedding["vector_namespace"] == "lifecycle"
        assert embedding["status"] == "ready"
        assert embedding["secret_ref_configured"] is True
        assert embedding["vector_ref"].startswith("pgvector://growth_playbooks/")
        assert embedding["adapter"]["adapter_kind"] == "pgvector_adapter"
        assert embedding["adapter"]["sync_status"] == "external_shadow_prepared"
        assert embedding["adapter"]["readiness_status"] == "ready_for_live_sync"
        assert embedding["adapter"]["external_vector_ref"].startswith("pgvector://growth_playbooks/lifecycle/")

        retrieval = client.post(
            "/api/v1/knowledge/retrievals",
            headers={"x-actor-role": "analyst"},
            json={"query": "premium return bonus progression", "retrieval_mode": "hybrid_v1"},
        )
        assert retrieval.status_code == 201
        payload = retrieval.json()
        assert payload["result_count"] == 1
        assert payload["vector_index"]["embedding_provider"] == "openai"
        assert payload["vector_index"]["vector_store"] == "pgvector"
        signals = payload["citations"][0]["ranking_signals"]
        assert signals["vector_status"] == "ready"
        assert signals["embedding_provider"] == "openai"
        assert signals["vector_store"] == "pgvector"
        assert signals["vector_index_id"] == "growth_playbooks"
        assert signals["vector_adapter_kind"] == "pgvector_adapter"
        assert signals["vector_sync_status"] == "external_shadow_prepared"
        assert signals["vector_readiness_status"] == "ready_for_live_sync"

        exported = client.get("/api/v1/knowledge/vector-indexes/growth_playbooks/export", headers={"x-actor-role": "analyst"})
        assert exported.status_code == 200
        export_payload = exported.json()
        assert export_payload["format"] == "knowledge_vector_index.v1"
        assert export_payload["index"]["secret_ref_configured"] is True
        assert export_payload["index"]["adapter_kind"] == "pgvector_adapter"
        assert export_payload["index"]["sync_status"] == "external_shadow_prepared"
        assert export_payload["index"]["readiness_status"] == "ready_for_live_sync"
        assert export_payload["records"][0]["vector_hash"]
        assert export_payload["records"][0]["embedding"]["status"] == "ready"
        assert export_payload["records"][0]["adapter"]["adapter_kind"] == "pgvector_adapter"
        assert "vector" not in export_payload["records"][0]
        assert "secret_ref" not in export_payload["records"][0]["embedding"]


def test_knowledge_control_plane_store_remains_local_with_provider_embedding(monkeypatch, tmp_path):
    with _client_with_env(
        monkeypatch,
        tmp_path,
        KNOWLEDGE_EMBEDDING_PROVIDER="openai",
        KNOWLEDGE_EMBEDDING_MODEL="text-embedding-3-small",
        KNOWLEDGE_VECTOR_STORE="control_plane",
        KNOWLEDGE_VECTOR_INDEX="control_plane_openai",
    ) as client:
        created = client.post(
            "/api/v1/knowledge/documents",
            headers={"x-actor-role": "operator"},
            json={
                "title": "Control Plane Provider Embedding",
                "content": "Control-plane storage can still label provider embeddings without requiring a vector-store secret ref.",
                "source_type": "playbook",
                "tags": ["push"],
            },
        )
        assert created.status_code == 201
        embedding = created.json()["chunks"][0]["embedding"]
        assert embedding["provider"] == "openai"
        assert embedding["vector_store"] == "control_plane"
        assert embedding["vector_ref"].startswith("inline:text-embedding-3-small:")
        assert embedding["adapter"]["adapter_kind"] == "control_plane"
        assert embedding["adapter"]["sync_status"] == "control_plane_synced"
        assert embedding["adapter"]["readiness_status"] == "ready"


def test_knowledge_legacy_external_vector_index_infers_adapter_status(client):
    from app.application.knowledge import KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE

    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        repository.upsert_resource(
            KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE,
            "legacy_pgvector",
            status="active",
            name="legacy_pgvector",
            payload={
                "index_id": "legacy_pgvector",
                "format": "knowledge_vector_index.v1",
                "embedding_provider": "openai",
                "embedding_model": "text-embedding-3-small",
                "vector_store": "pgvector",
                "vector_namespace": "lifecycle",
                "dimensions": 1024,
                "record_count": 0,
                "document_count": 0,
                "storage_mode": "external_vector_store_shadow_index",
                "secret_ref_configured": True,
            },
        )

    indexes = client.get("/api/v1/knowledge/vector-indexes", headers={"x-actor-role": "analyst"})
    assert indexes.status_code == 200
    index = next(item for item in indexes.json()["items"] if item["index_id"] == "legacy_pgvector")
    assert index["adapter_kind"] == "pgvector_adapter"
    assert index["sync_status"] == "external_shadow_prepared"
    assert index["readiness_status"] == "ready_for_live_sync"


def test_knowledge_archive_updates_original_vector_index_after_runtime_index_change(monkeypatch, tmp_path):
    with _client_with_env(monkeypatch, tmp_path, KNOWLEDGE_VECTOR_INDEX="index_a") as client:
        created = client.post(
            "/api/v1/knowledge/documents",
            headers={"x-actor-role": "operator"},
            json={
                "title": "Dormant Subscriber Playbook",
                "content": "Dormant subscribers should receive a concise reminder and a clear return reward.",
                "source_type": "playbook",
                "tags": ["email", "winback"],
            },
        )
        assert created.status_code == 201
        document_id = created.json()["document_id"]
        exported = client.get("/api/v1/knowledge/vector-indexes/index_a/export", headers={"x-actor-role": "analyst"})
        assert exported.status_code == 200
        assert exported.json()["index"]["record_count"] == 1

    with _client_with_env(monkeypatch, tmp_path, KNOWLEDGE_VECTOR_INDEX="index_b") as client:
        retrieval = client.post(
            "/api/v1/knowledge/retrievals",
            headers={"x-actor-role": "analyst"},
            json={"query": "dormant return reward", "retrieval_mode": "hybrid_v1"},
        )
        assert retrieval.status_code == 201
        retrieval_payload = retrieval.json()
        assert retrieval_payload["result_count"] == 1
        assert retrieval_payload["vector_index"]["index_id"] == "index_a"
        assert retrieval_payload["citations"][0]["ranking_signals"]["vector_status"] == "ready"
        assert retrieval_payload["citations"][0]["ranking_signals"]["vector_index_id"] == "index_a"

        archived = client.post(f"/api/v1/knowledge/documents/{document_id}/archive", headers={"x-actor-role": "operator"})
        assert archived.status_code == 200
        exported = client.get("/api/v1/knowledge/vector-indexes/index_a/export", headers={"x-actor-role": "analyst"})
        assert exported.status_code == 200
        payload = exported.json()
        assert payload["index"]["record_count"] == 0
        assert payload["index"]["document_count"] == 0
        assert payload["records"] == []


def test_knowledge_archive_surfaces_external_adapter_receipt(monkeypatch, tmp_path):
    with _client_with_env(
        monkeypatch,
        tmp_path,
        KNOWLEDGE_EMBEDDING_PROVIDER="openai",
        KNOWLEDGE_EMBEDDING_MODEL="text-embedding-3-small",
        KNOWLEDGE_VECTOR_STORE="pgvector",
        KNOWLEDGE_VECTOR_INDEX="archive_playbooks",
        KNOWLEDGE_VECTOR_SECRET_REF="secret://knowledge/vector",
    ) as client:
        created = client.post(
            "/api/v1/knowledge/documents",
            headers={"x-actor-role": "operator"},
            json={
                "title": "Archive Adapter Playbook",
                "content": "Archive receipts should remain visible after the last vector record is archived.",
                "source_type": "playbook",
            },
        )
        assert created.status_code == 201
        document_id = created.json()["document_id"]

        archived = client.post(f"/api/v1/knowledge/documents/{document_id}/archive", headers={"x-actor-role": "operator"})
        assert archived.status_code == 200
        exported = client.get("/api/v1/knowledge/vector-indexes/archive_playbooks/export", headers={"x-actor-role": "analyst"})
        assert exported.status_code == 200
        index = exported.json()["index"]
        assert index["record_count"] == 0
        assert index["last_adapter_operation"]["operation"] == "archive"
        assert index["last_adapter_operation"]["sync_status"] == "external_shadow_archive_prepared"


def test_knowledge_vector_index_reports_mixed_provider_when_config_reuses_index(monkeypatch, tmp_path):
    with _client_with_env(
        monkeypatch,
        tmp_path,
        KNOWLEDGE_EMBEDDING_PROVIDER="openai",
        KNOWLEDGE_EMBEDDING_MODEL="text-embedding-3-small",
        KNOWLEDGE_VECTOR_STORE="pgvector",
        KNOWLEDGE_VECTOR_INDEX="growth_playbooks",
        KNOWLEDGE_VECTOR_SECRET_REF="secret://knowledge/vector",
    ) as client:
        created = client.post(
            "/api/v1/knowledge/documents",
            headers={"x-actor-role": "operator"},
            json={
                "title": "OpenAI Indexed Playbook",
                "content": "OpenAI-indexed lifecycle copy emphasizes saved progress and a friendly return path.",
                "source_type": "playbook",
                "tags": ["push"],
            },
        )
        assert created.status_code == 201

    with _client_with_env(
        monkeypatch,
        tmp_path,
        KNOWLEDGE_EMBEDDING_PROVIDER="voyage",
        KNOWLEDGE_EMBEDDING_MODEL="voyage-large-2",
        KNOWLEDGE_VECTOR_STORE="pgvector",
        KNOWLEDGE_VECTOR_INDEX="growth_playbooks",
        KNOWLEDGE_VECTOR_SECRET_REF="secret://knowledge/vector",
    ) as client:
        created = client.post(
            "/api/v1/knowledge/documents",
            headers={"x-actor-role": "operator"},
            json={
                "title": "Voyage Indexed Playbook",
                "content": "Voyage-indexed lifecycle copy mentions loyalty level and renewed session value.",
                "source_type": "playbook",
                "tags": ["push"],
            },
        )
        assert created.status_code == 201
        exported = client.get("/api/v1/knowledge/vector-indexes/growth_playbooks/export", headers={"x-actor-role": "analyst"})
        assert exported.status_code == 200
        payload = exported.json()
        assert payload["index"]["embedding_provider"] == "mixed"
        assert payload["index"]["embedding_model"] == "mixed"
        assert payload["index"]["vector_store"] == "pgvector"
        assert payload["index"]["record_count"] == 2
        assert {record["embedding"]["provider"] for record in payload["records"]} == {"openai", "voyage"}


def test_prod_external_knowledge_vector_backend_requires_secret_ref():
    settings = Settings(
        app_env="prod",
        control_plane_database_url="postgresql://example.com/kairyx",
        warehouse_backend="bigquery",
        object_storage_backend="gcs",
        message_backend="pubsub",
        legacy_header_auth_enabled=False,
        cors_allowed_origins=("https://app.example.com",),
        scheduler_enabled=False,
        oidc_issuer="https://accounts.example.com",
        oidc_audience="kairyx",
        oidc_jwks_url="https://accounts.example.com/jwks",
        knowledge_embedding_provider="openai",
        knowledge_vector_store="pgvector",
        knowledge_vector_secret_ref="",
    )
    with pytest.raises(RuntimeError, match="KNOWLEDGE_VECTOR_SECRET_REF"):
        validate_runtime_settings(settings)


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
