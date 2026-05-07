from __future__ import annotations

import hashlib
import re
import uuid
from datetime import datetime
from typing import Any, Dict


KNOWLEDGE_DOCUMENT_RESOURCE_TYPE = "knowledge_document"
KNOWLEDGE_CHUNK_RESOURCE_TYPE = "knowledge_chunk"
KNOWLEDGE_INGESTION_JOB_RESOURCE_TYPE = "knowledge_ingestion_job"
KNOWLEDGE_SOURCE_RESOURCE_TYPE = "knowledge_source"

MAX_CHUNK_CHARS = 1600
MAX_DOCUMENT_CHARS = 250_000


class KnowledgeService:
    def __init__(self, repository):
        self.repository = repository

    def create_document(
        self,
        *,
        title: str,
        content: str,
        source_type: str = "markdown",
        source_id: str | None = None,
        source_name: str | None = None,
        source_uri: str | None = None,
        tags: list[str] | None = None,
        visibility: str = "workspace",
        provenance: Dict[str, Any] | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        normalized_title = _required_text(title, "title", max_length=180)
        normalized_content = _required_text(content, "content", max_length=MAX_DOCUMENT_CHARS)
        normalized_source_type = _normalize_source_type(source_type)
        normalized_visibility = _normalize_visibility(visibility)
        normalized_tags = _normalize_tags(tags or [])
        normalized_provenance = dict(provenance or {})
        normalized_metadata = dict(metadata or {})
        document_id = f"kdoc_{uuid.uuid4().hex[:20]}"
        resolved_source_id = _normalize_optional_id(source_id) or f"ksrc_{uuid.uuid4().hex[:16]}"
        resolved_source_name = _clean_text(source_name, max_length=120) or "Manual Knowledge"
        content_hash = _hash_text(normalized_content)
        chunks = _chunk_text(normalized_content)
        if not chunks:
            raise ValueError("content must include at least one non-empty chunk.")
        source_payload = {
            "source_id": resolved_source_id,
            "name": resolved_source_name,
            "source_type": normalized_source_type,
            "visibility": normalized_visibility,
            "tags": normalized_tags,
            "latest_document_id": document_id,
            "latest_content_hash": content_hash,
        }
        self.repository.upsert_resource(
            KNOWLEDGE_SOURCE_RESOURCE_TYPE,
            resolved_source_id,
            status="active",
            name=resolved_source_name,
            payload=source_payload,
        )
        document_payload = {
            "document_id": document_id,
            "source_id": resolved_source_id,
            "source_name": resolved_source_name,
            "title": normalized_title,
            "source_type": normalized_source_type,
            "source_uri": _clean_text(source_uri, max_length=500),
            "visibility": normalized_visibility,
            "tags": normalized_tags,
            "status": "active",
            "content_hash": content_hash,
            "content_preview": _preview(normalized_content),
            "character_count": len(normalized_content),
            "chunk_count": len(chunks),
            "provenance": normalized_provenance,
            "metadata": normalized_metadata,
            "export": {
                "format": "knowledge_document.v1",
                "includes": ["document", "chunks", "provenance"],
            },
        }
        document_record = self.repository.upsert_resource(
            KNOWLEDGE_DOCUMENT_RESOURCE_TYPE,
            document_id,
            status="active",
            name=normalized_title,
            payload=document_payload,
        )
        self.repository.create_resource_version(
            KNOWLEDGE_DOCUMENT_RESOURCE_TYPE,
            document_id,
            version=1,
            payload=document_payload,
        )
        chunk_payloads = self._persist_chunks(
            document_id=document_id,
            title=normalized_title,
            source_id=resolved_source_id,
            source_name=resolved_source_name,
            source_type=normalized_source_type,
            tags=normalized_tags,
            visibility=normalized_visibility,
            chunks=chunks,
        )
        ingestion_job = self._record_ingestion_job(
            document_id=document_id,
            source_id=resolved_source_id,
            chunk_count=len(chunk_payloads),
            character_count=len(normalized_content),
            content_hash=content_hash,
            warnings=[],
        )
        self.repository.record_resource_event(
            KNOWLEDGE_DOCUMENT_RESOURCE_TYPE,
            document_id,
            event_type="knowledge_document_ingested",
            payload={
                "document_id": document_id,
                "source_id": resolved_source_id,
                "chunk_count": len(chunk_payloads),
                "ingestion_job_id": ingestion_job["ingestion_job_id"],
            },
        )
        return {
            **self._resource_to_document(document_record),
            "ingestion_job": ingestion_job,
            "chunks": chunk_payloads,
        }

    def list_documents(self, *, include_archived: bool = False) -> list[Dict[str, Any]]:
        documents = [self._resource_to_document(record) for record in self.repository.list_resources(KNOWLEDGE_DOCUMENT_RESOURCE_TYPE)]
        if include_archived:
            return documents
        return [document for document in documents if document.get("status") != "archived"]

    def get_document(self, document_id: str, *, include_chunks: bool = False) -> Dict[str, Any] | None:
        record = self.repository.get_resource(KNOWLEDGE_DOCUMENT_RESOURCE_TYPE, document_id)
        if record is None:
            return None
        document = self._resource_to_document(record)
        if include_chunks:
            document["chunks"] = self.list_chunks(document_id, include_archived=True)
        return document

    def list_chunks(self, document_id: str, *, include_archived: bool = False) -> list[Dict[str, Any]]:
        chunks = [
            self._resource_to_chunk(record)
            for record in self.repository.list_resources(KNOWLEDGE_CHUNK_RESOURCE_TYPE, name=document_id)
        ]
        chunks.sort(key=lambda chunk: int(chunk.get("ordinal", 0)))
        if include_archived:
            return chunks
        return [chunk for chunk in chunks if chunk.get("status") != "archived"]

    def archive_document(self, document_id: str) -> Dict[str, Any]:
        current = self.get_document(document_id, include_chunks=False)
        if current is None:
            raise KeyError(document_id)
        archived_at = datetime.utcnow().isoformat()
        archived_payload = {**current, "status": "archived", "archived_at": archived_at}
        document_record = self.repository.upsert_resource(
            KNOWLEDGE_DOCUMENT_RESOURCE_TYPE,
            document_id,
            status="archived",
            name=current.get("title") or document_id,
            payload=archived_payload,
        )
        for chunk in self.list_chunks(document_id, include_archived=True):
            chunk_payload = {**chunk, "status": "archived", "archived_at": archived_at}
            self.repository.upsert_resource(
                KNOWLEDGE_CHUNK_RESOURCE_TYPE,
                chunk["chunk_id"],
                status="archived",
                name=document_id,
                payload=chunk_payload,
            )
        self.repository.record_resource_event(
            KNOWLEDGE_DOCUMENT_RESOURCE_TYPE,
            document_id,
            event_type="knowledge_document_archived",
            payload={"document_id": document_id, "archived_at": archived_at},
        )
        return self._resource_to_document(document_record)

    def export_document(self, document_id: str) -> Dict[str, Any] | None:
        document = self.get_document(document_id, include_chunks=False)
        if document is None:
            return None
        chunks = self.list_chunks(document_id, include_archived=True)
        return {
            "format": "knowledge_document.v1",
            "document": document,
            "chunks": chunks,
        }

    def _persist_chunks(
        self,
        *,
        document_id: str,
        title: str,
        source_id: str,
        source_name: str,
        source_type: str,
        tags: list[str],
        visibility: str,
        chunks: list[str],
    ) -> list[Dict[str, Any]]:
        payloads: list[Dict[str, Any]] = []
        for index, chunk_text in enumerate(chunks, start=1):
            chunk_id = f"{document_id}:chunk_{index:04d}"
            payload = {
                "chunk_id": chunk_id,
                "document_id": document_id,
                "source_id": source_id,
                "source_name": source_name,
                "source_type": source_type,
                "source_title": title,
                "ordinal": index,
                "text": chunk_text,
                "summary": _preview(chunk_text, max_length=180),
                "content_hash": _hash_text(chunk_text),
                "character_count": len(chunk_text),
                "token_estimate": _estimate_tokens(chunk_text),
                "tags": list(tags),
                "visibility": visibility,
                "status": "active",
                "embedding": {
                    "status": "pending",
                    "model": None,
                    "vector_ref": None,
                },
            }
            record = self.repository.upsert_resource(
                KNOWLEDGE_CHUNK_RESOURCE_TYPE,
                chunk_id,
                status="active",
                name=document_id,
                payload=payload,
            )
            payloads.append(self._resource_to_chunk(record))
        return payloads

    def _record_ingestion_job(
        self,
        *,
        document_id: str,
        source_id: str,
        chunk_count: int,
        character_count: int,
        content_hash: str,
        warnings: list[str],
    ) -> Dict[str, Any]:
        job_id = f"kjob_{uuid.uuid4().hex[:20]}"
        payload = {
            "ingestion_job_id": job_id,
            "document_id": document_id,
            "source_id": source_id,
            "status": "completed",
            "chunk_count": chunk_count,
            "rejected_section_count": 0,
            "character_count": character_count,
            "content_hash": content_hash,
            "warnings": warnings,
            "errors": [],
        }
        record = self.repository.upsert_resource(
            KNOWLEDGE_INGESTION_JOB_RESOURCE_TYPE,
            job_id,
            status="completed",
            name=document_id,
            payload=payload,
        )
        return dict(record.get("payload") or payload)

    @staticmethod
    def _resource_to_document(record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        return {
            **payload,
            "document_id": payload.get("document_id") or record.get("resource_id"),
            "title": payload.get("title") or record.get("name") or "",
            "status": payload.get("status") or record.get("status") or "active",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
        }

    @staticmethod
    def _resource_to_chunk(record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        return {
            **payload,
            "chunk_id": payload.get("chunk_id") or record.get("resource_id"),
            "status": payload.get("status") or record.get("status") or "active",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
        }


def _required_text(value: str, field_name: str, *, max_length: int) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required.")
    if len(normalized) > max_length:
        raise ValueError(f"{field_name} exceeds {max_length} characters.")
    return normalized


def _clean_text(value: str | None, *, max_length: int) -> str:
    normalized = str(value or "").strip()
    if len(normalized) > max_length:
        return normalized[:max_length]
    return normalized


def _normalize_optional_id(value: str | None) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(value or "").strip()).strip("_")
    return normalized[:80]


def _normalize_source_type(value: str) -> str:
    normalized = str(value or "markdown").strip().lower().replace("-", "_")
    allowed = {"markdown", "text", "campaign_brief", "sop", "report", "faq", "playbook"}
    if normalized not in allowed:
        raise ValueError(f"source_type must be one of: {', '.join(sorted(allowed))}.")
    return normalized


def _normalize_visibility(value: str) -> str:
    normalized = str(value or "workspace").strip().lower().replace("-", "_")
    allowed = {"workspace", "project", "private"}
    if normalized not in allowed:
        raise ValueError(f"visibility must be one of: {', '.join(sorted(allowed))}.")
    return normalized


def _normalize_tags(tags: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        cleaned = re.sub(r"\s+", " ", str(tag or "").strip().lower())
        if not cleaned or cleaned in seen:
            continue
        normalized.append(cleaned[:40])
        seen.add(cleaned)
    return normalized[:20]


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _preview(value: str, *, max_length: int = 260) -> str:
    compact = re.sub(r"\s+", " ", value).strip()
    if len(compact) <= max_length:
        return compact
    return f"{compact[: max_length - 1].rstrip()}..."


def _estimate_tokens(value: str) -> int:
    return max(1, len(value) // 4)


def _chunk_text(content: str) -> list[str]:
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", content.replace("\r\n", "\n")) if part.strip()]
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        parts = _split_long_paragraph(paragraph)
        for part in parts:
            candidate = f"{current}\n\n{part}".strip() if current else part
            if len(candidate) <= MAX_CHUNK_CHARS:
                current = candidate
                continue
            if current:
                chunks.append(current)
            current = part
    if current:
        chunks.append(current)
    return chunks


def _split_long_paragraph(paragraph: str) -> list[str]:
    if len(paragraph) <= MAX_CHUNK_CHARS:
        return [paragraph]
    words = paragraph.split()
    if not words:
        return []
    parts: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip() if current else word
        if len(candidate) <= MAX_CHUNK_CHARS:
            current = candidate
            continue
        if current:
            parts.append(current)
        current = word[:MAX_CHUNK_CHARS]
    if current:
        parts.append(current)
    return parts
