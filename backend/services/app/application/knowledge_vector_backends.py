from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Dict


CONTROL_PLANE_VECTOR_STORE = "control_plane"
CONTROL_PLANE_EMBEDDING_PROVIDER = "local_hash"
MANAGED_VECTOR_STORES = {
    "pgvector",
    "pinecone",
    "qdrant",
    "weaviate",
    "milvus",
    "opensearch",
    "bigquery_vector",
    "custom",
}


@dataclass(frozen=True)
class KnowledgeVectorBackend:
    config: Dict[str, Any]

    @property
    def adapter_kind(self) -> str:
        return "control_plane"

    @property
    def capabilities(self) -> list[str]:
        return ["upsert", "query", "archive", "export_shadow"]

    def vector_ref(self, *, vector_hash: str) -> str:
        return f"inline:{self.config['embedding_model']}:{vector_hash}"

    def materialization_receipt(self, *, vector_hash: str) -> Dict[str, Any]:
        return {
            "adapter_kind": self.adapter_kind,
            "operation": "upsert",
            "sync_status": "control_plane_synced",
            "readiness_status": "ready",
            "vector_store": self.config["vector_store"],
            "index_id": self.config["index_id"],
            "vector_namespace": self.config["vector_namespace"],
            "external_vector_ref": "",
            "secret_ref_configured": bool(self.config.get("secret_ref_configured")),
            "capabilities": list(self.capabilities),
            "warnings": [],
            "receipt_id": _receipt_id(self.config, vector_hash=vector_hash, operation="upsert"),
            "recorded_at": _utcnow_iso(),
        }

    def index_payload_patch(self) -> Dict[str, Any]:
        return {
            "adapter_kind": self.adapter_kind,
            "sync_status": "control_plane_synced",
            "readiness_status": "ready",
            "capabilities": list(self.capabilities),
            "warnings": [],
        }


@dataclass(frozen=True)
class ManagedExternalVectorBackend(KnowledgeVectorBackend):
    @property
    def adapter_kind(self) -> str:
        return f"{self.config['vector_store']}_adapter"

    @property
    def capabilities(self) -> list[str]:
        capabilities = ["upsert", "query", "archive", "export_shadow", "secret_ref_required"]
        if str(self.config.get("vector_store") or "").strip().lower() == "pgvector":
            capabilities.append("live_sync")
        return capabilities

    def vector_ref(self, *, vector_hash: str) -> str:
        namespace = str(self.config.get("vector_namespace") or "default").strip() or "default"
        return f"{self.config['vector_store']}://{self.config['index_id']}/{namespace}/{vector_hash}"

    def materialization_receipt(self, *, vector_hash: str) -> Dict[str, Any]:
        secret_ref_configured = bool(self.config.get("secret_ref_configured"))
        sync_status = "external_shadow_prepared" if secret_ref_configured else "blocked_missing_secret_ref"
        readiness_status = "ready_for_live_sync" if secret_ref_configured else "needs_secret_ref"
        warnings = [] if secret_ref_configured else ["Configure KNOWLEDGE_VECTOR_SECRET_REF before production live sync."]
        return {
            "adapter_kind": self.adapter_kind,
            "operation": "upsert",
            "sync_status": sync_status,
            "readiness_status": readiness_status,
            "vector_store": self.config["vector_store"],
            "index_id": self.config["index_id"],
            "vector_namespace": self.config["vector_namespace"],
            "external_vector_ref": self.vector_ref(vector_hash=vector_hash),
            "secret_ref_configured": secret_ref_configured,
            "capabilities": list(self.capabilities),
            "warnings": warnings,
            "receipt_id": _receipt_id(self.config, vector_hash=vector_hash, operation="upsert"),
            "recorded_at": _utcnow_iso(),
        }

    def index_payload_patch(self) -> Dict[str, Any]:
        secret_ref_configured = bool(self.config.get("secret_ref_configured"))
        return {
            "adapter_kind": self.adapter_kind,
            "sync_status": "external_shadow_prepared" if secret_ref_configured else "blocked_missing_secret_ref",
            "readiness_status": "ready_for_live_sync" if secret_ref_configured else "needs_secret_ref",
            "capabilities": list(self.capabilities),
            "warnings": [] if secret_ref_configured else ["Configure KNOWLEDGE_VECTOR_SECRET_REF before production live sync."],
        }


def build_knowledge_vector_backend(config: Dict[str, Any]) -> KnowledgeVectorBackend:
    vector_store = str(config.get("vector_store") or "").strip().lower()
    if vector_store == CONTROL_PLANE_VECTOR_STORE:
        return KnowledgeVectorBackend(dict(config))
    if vector_store in MANAGED_VECTOR_STORES:
        return ManagedExternalVectorBackend(dict(config))
    return ManagedExternalVectorBackend(dict(config))


def archived_adapter_receipt(embedding: Dict[str, Any], *, archived_at: str) -> Dict[str, Any]:
    adapter = dict(embedding.get("adapter") or {})
    adapter_kind = str(adapter.get("adapter_kind") or "control_plane")
    is_managed = adapter_kind != "control_plane"
    sync_status = "external_shadow_archive_prepared" if is_managed and adapter.get("secret_ref_configured") else "control_plane_archived"
    if is_managed and not adapter.get("secret_ref_configured"):
        sync_status = "archive_blocked_missing_secret_ref"
    return {
        **adapter,
        "operation": "archive",
        "sync_status": sync_status,
        "readiness_status": "archived",
        "archived_at": archived_at,
        "receipt_id": _receipt_id(adapter, vector_hash=str(embedding.get("vector_ref") or ""), operation="archive"),
        "recorded_at": _utcnow_iso(),
    }


def _receipt_id(config: Dict[str, Any], *, vector_hash: str, operation: str) -> str:
    basis = "|".join(
        [
            str(config.get("vector_store") or ""),
            str(config.get("index_id") or ""),
            str(config.get("vector_namespace") or ""),
            str(vector_hash or ""),
            str(operation or ""),
        ]
    )
    return "kvadp_" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:20]


def _utcnow_iso() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat()
