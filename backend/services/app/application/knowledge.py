from __future__ import annotations

import hashlib
import math
import re
import uuid
from collections import Counter
from datetime import datetime
from typing import Any, Dict

from app.application.ai_feedback import AI_FEEDBACK_RESOURCE_TYPE


KNOWLEDGE_DOCUMENT_RESOURCE_TYPE = "knowledge_document"
KNOWLEDGE_CHUNK_RESOURCE_TYPE = "knowledge_chunk"
KNOWLEDGE_INGESTION_JOB_RESOURCE_TYPE = "knowledge_ingestion_job"
KNOWLEDGE_RETRIEVAL_RESOURCE_TYPE = "knowledge_retrieval"
KNOWLEDGE_SOURCE_RESOURCE_TYPE = "knowledge_source"

MAX_CHUNK_CHARS = 1600
MAX_DOCUMENT_CHARS = 250_000
MAX_RETRIEVAL_QUERY_CHARS = 500
MAX_RETRIEVAL_RESULTS = 20
LEXICAL_RETRIEVAL_MODE = "lexical_v1"
HYBRID_RETRIEVAL_MODE = "hybrid_v1"
LOCAL_SEMANTIC_VECTOR_MODEL = "local_semantic_hash_v1"
SEMANTIC_VECTOR_DIMENSIONS = 1024
HYBRID_MIN_SEMANTIC_SCORE = 0.08
STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "i",
    "in",
    "is",
    "it",
    "me",
    "of",
    "on",
    "or",
    "our",
    "shall",
    "should",
    "the",
    "this",
    "to",
    "we",
    "with",
    "you",
}
SEMANTIC_EQUIVALENCE_GROUPS = (
    ("reactivation", "reactivate", "reactivating", "winback", "lapsed", "return", "returning", "reengage", "reengagement"),
    ("bonus", "reward", "offer", "incentive", "credit", "perk", "benefit"),
    ("progression", "progress", "checkpoint", "saved", "resume", "state"),
    ("push", "notification", "message", "reminder", "alert"),
    ("email", "mail", "newsletter", "message"),
    ("vip", "premium", "status", "loyalty"),
    ("campaign", "journey", "workflow", "send", "schedule"),
    ("experiment", "test", "holdout", "variant", "ab"),
)
SEMANTIC_SYNONYMS = {
    term: tuple(alias for alias in group if alias != term)
    for group in SEMANTIC_EQUIVALENCE_GROUPS
    for term in group
}


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

    def retrieve(
        self,
        *,
        query: str,
        top_k: int = 5,
        retrieval_mode: str = LEXICAL_RETRIEVAL_MODE,
        tags: list[str] | None = None,
        source_types: list[str] | None = None,
        document_ids: list[str] | None = None,
        include_archived: bool = False,
    ) -> Dict[str, Any]:
        normalized_query = _required_text(query, "query", max_length=MAX_RETRIEVAL_QUERY_CHARS)
        normalized_top_k = max(1, min(int(top_k or 5), MAX_RETRIEVAL_RESULTS))
        normalized_retrieval_mode = _normalize_retrieval_mode(retrieval_mode)
        normalized_tags = _normalize_tags(tags or [])
        normalized_source_types = [_normalize_source_type(item) for item in source_types or []]
        normalized_document_ids = _normalize_id_filters(document_ids or [])
        query_terms = _query_terms(normalized_query)
        if not query_terms:
            raise ValueError("query must include at least one searchable term.")
        feedback_boosts = self._feedback_boosts()
        scored = [
            match
            for chunk in self.repository.list_resources(KNOWLEDGE_CHUNK_RESOURCE_TYPE)
            if (match := _score_chunk(
                self._resource_to_chunk(chunk),
                query=normalized_query,
                query_terms=query_terms,
                retrieval_mode=normalized_retrieval_mode,
                tags=normalized_tags,
                source_types=normalized_source_types,
                document_ids=normalized_document_ids,
                include_archived=include_archived,
                feedback_boosts=feedback_boosts,
            ))
        ]
        ranked = sorted(
            scored,
            key=lambda item: (
                -float(item["score"]),
                str(item["document_title"]).lower(),
                int(item["ordinal"]),
            ),
        )[:normalized_top_k]
        retrieval_id = f"kret_{uuid.uuid4().hex[:20]}"
        citations = [
            {
                **item,
                "rank": index,
                "citation_id": f"C{index}",
                "citation": f"[C{index}] {item['document_title']} chunk {item['ordinal']}",
            }
            for index, item in enumerate(ranked, start=1)
        ]
        context_pack = _build_context_pack(
            retrieval_id=retrieval_id,
            query=normalized_query,
            normalized_query=" ".join(query_terms),
            retrieval_mode=normalized_retrieval_mode,
            citations=citations,
        )
        payload = {
            "retrieval_id": retrieval_id,
            "query": normalized_query,
            "normalized_query": " ".join(query_terms),
            "retrieval_mode": normalized_retrieval_mode,
            "status": "completed",
            "top_k": normalized_top_k,
            "result_count": len(citations),
            "filters": {
                "tags": normalized_tags,
                "source_types": normalized_source_types,
                "document_ids": normalized_document_ids,
                "include_archived": bool(include_archived),
            },
            "citations": citations,
            "context_pack": context_pack,
            "export": {
                "format": "knowledge_evidence_pack.v1",
                "resource_id": retrieval_id,
                "includes": ["query", "filters", "citations", "context_pack"],
            },
        }
        record = self.repository.upsert_resource(
            KNOWLEDGE_RETRIEVAL_RESOURCE_TYPE,
            retrieval_id,
            status="completed",
            name=normalized_query[:120],
            payload=payload,
        )
        self.repository.record_resource_event(
            KNOWLEDGE_RETRIEVAL_RESOURCE_TYPE,
            retrieval_id,
            event_type="knowledge_retrieval_completed",
            payload={
                "retrieval_id": retrieval_id,
                "result_count": len(citations),
                "retrieval_mode": normalized_retrieval_mode,
            },
        )
        return self._resource_to_retrieval(record)

    def list_retrievals(self) -> list[Dict[str, Any]]:
        return [self._resource_to_retrieval(record) for record in self.repository.list_resources(KNOWLEDGE_RETRIEVAL_RESOURCE_TYPE)]

    def get_retrieval(self, retrieval_id: str) -> Dict[str, Any] | None:
        record = self.repository.get_resource(KNOWLEDGE_RETRIEVAL_RESOURCE_TYPE, retrieval_id)
        return self._resource_to_retrieval(record) if record is not None else None

    def export_retrieval(self, retrieval_id: str) -> Dict[str, Any] | None:
        retrieval = self.get_retrieval(retrieval_id)
        if retrieval is None:
            return None
        return {
            "format": "knowledge_evidence_pack.v1",
            "retrieval": {
                key: value
                for key, value in retrieval.items()
                if key
                not in {
                    "audit_id",
                    "masked_fields",
                }
            },
        }

    def _feedback_boosts(self) -> Dict[str, Dict[str, float]]:
        boosts = {"chunks": {}, "documents": {}}
        for record in self.repository.list_resources(AI_FEEDBACK_RESOURCE_TYPE):
            payload = dict(record.get("payload") or {})
            target_type = str(payload.get("target_type") or "")
            target_id = str(payload.get("target_id") or "")
            if not target_id:
                continue
            weight = float(payload.get("weight") or 0.0)
            if target_type == "knowledge_chunk":
                boosts["chunks"][target_id] = _clamp_feedback_boost(boosts["chunks"].get(target_id, 0.0) + weight)
            elif target_type == "knowledge_document":
                boosts["documents"][target_id] = _clamp_feedback_boost(boosts["documents"].get(target_id, 0.0) + weight)
        return boosts

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
                "embedding": _embedding_metadata(
                    " ".join(
                        [
                            title,
                            source_name,
                            source_type,
                            " ".join(tags),
                            chunk_text,
                        ]
                    )
                ),
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

    @staticmethod
    def _resource_to_retrieval(record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        return {
            **payload,
            "retrieval_id": payload.get("retrieval_id") or record.get("resource_id"),
            "status": payload.get("status") or record.get("status") or "completed",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
            "created_by": record.get("created_by") or payload.get("created_by") or "system",
            "updated_by": record.get("updated_by") or payload.get("updated_by") or "system",
            "correlation_id": record.get("correlation_id") or payload.get("correlation_id") or "",
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


def _normalize_retrieval_mode(value: str) -> str:
    normalized = str(value or LEXICAL_RETRIEVAL_MODE).strip().lower().replace("-", "_")
    allowed = {LEXICAL_RETRIEVAL_MODE, HYBRID_RETRIEVAL_MODE}
    if normalized not in allowed:
        raise ValueError(f"retrieval_mode must be one of: {', '.join(sorted(allowed))}.")
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


def _normalize_id_filters(values: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _clean_text(value, max_length=100)
        if not cleaned or cleaned in seen:
            continue
        normalized.append(cleaned)
        seen.add(cleaned)
    return normalized[:50]


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _preview(value: str, *, max_length: int = 260) -> str:
    compact = re.sub(r"\s+", " ", value).strip()
    if len(compact) <= max_length:
        return compact
    return f"{compact[: max_length - 1].rstrip()}..."


def _estimate_tokens(value: str) -> int:
    return max(1, len(value) // 4)


def _clamp_feedback_boost(value: float) -> float:
    return round(max(-3.0, min(3.0, float(value))), 4)


def _query_terms(value: str) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for term in re.findall(r"[a-zA-Z0-9_]+", value.lower()):
        if len(term) <= 1 or term in STOP_WORDS or term in seen:
            continue
        terms.append(term)
        seen.add(term)
    return terms[:32]


def _embedding_metadata(value: str) -> Dict[str, Any]:
    vector = _semantic_vector(value)
    vector_hash = _hash_text(",".join(f"{item:.6f}" for item in vector))[:20]
    return {
        "status": "ready",
        "model": LOCAL_SEMANTIC_VECTOR_MODEL,
        "vector_ref": f"inline:{LOCAL_SEMANTIC_VECTOR_MODEL}:{vector_hash}",
        "dimensions": SEMANTIC_VECTOR_DIMENSIONS,
        "materialized_at": datetime.utcnow().isoformat(),
    }


def _semantic_similarity(query: str, value: str) -> float:
    query_vector = _semantic_vector(query)
    value_vector = _semantic_vector(value)
    score = sum(left * right for left, right in zip(query_vector, value_vector))
    return round(max(0.0, min(1.0, score)), 4)


def _semantic_vector(value: str) -> list[float]:
    vector = [0.0] * SEMANTIC_VECTOR_DIMENSIONS
    for feature in _semantic_features(value):
        digest = hashlib.sha256(f"semantic:{feature}".encode("utf-8")).hexdigest()
        index = int(digest[:8], 16) % SEMANTIC_VECTOR_DIMENSIONS
        vector[index] += 1.0
    norm = math.sqrt(sum(item * item for item in vector))
    if norm <= 0:
        return vector
    return [round(item / norm, 6) for item in vector]


def _semantic_features(value: str) -> list[str]:
    features: list[str] = []
    seen: set[str] = set()
    for raw_term in re.findall(r"[a-zA-Z0-9_]+", str(value or "").lower()):
        if len(raw_term) <= 1 or raw_term in STOP_WORDS:
            continue
        for term in _expanded_semantic_terms(raw_term):
            if term in STOP_WORDS or term in seen:
                continue
            features.append(f"term:{term}")
            seen.add(term)
        for ngram in _character_ngrams(raw_term):
            feature = f"char:{ngram}"
            if feature in seen:
                continue
            features.append(feature)
            seen.add(feature)
    return features[:600]


def _expanded_semantic_terms(term: str) -> list[str]:
    normalized = _semantic_stem(term)
    terms = [term, normalized]
    terms.extend(SEMANTIC_SYNONYMS.get(term, ()))
    terms.extend(SEMANTIC_SYNONYMS.get(normalized, ()))
    expanded: list[str] = []
    seen: set[str] = set()
    for item in terms:
        cleaned = _semantic_stem(item)
        if len(cleaned) <= 1 or cleaned in seen:
            continue
        expanded.append(cleaned)
        seen.add(cleaned)
    return expanded


def _semantic_stem(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "", str(value or "").lower())
    if len(normalized) > 6 and normalized.endswith("ing"):
        return normalized[:-3]
    if len(normalized) > 6 and normalized.endswith("ion"):
        return normalized[:-3]
    if len(normalized) > 5 and normalized.endswith("ed"):
        return normalized[:-2]
    if len(normalized) > 4 and normalized.endswith("s"):
        return normalized[:-1]
    return normalized


def _character_ngrams(value: str) -> list[str]:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "", value.lower())
    if len(normalized) < 4:
        return []
    return [normalized[index : index + 4] for index in range(0, min(len(normalized) - 3, 12))]


def _score_chunk(
    chunk: Dict[str, Any],
    *,
    query: str,
    query_terms: list[str],
    retrieval_mode: str,
    tags: list[str],
    source_types: list[str],
    document_ids: list[str],
    include_archived: bool,
    feedback_boosts: Dict[str, Dict[str, float]],
) -> Dict[str, Any] | None:
    if not include_archived and chunk.get("status") == "archived":
        return None
    if document_ids and str(chunk.get("document_id") or "") not in set(document_ids):
        return None
    if source_types and str(chunk.get("source_type") or "") not in set(source_types):
        return None
    chunk_tags = {str(tag).lower() for tag in chunk.get("tags") or []}
    if tags and not set(tags).issubset(chunk_tags):
        return None
    text = str(chunk.get("text") or "")
    if not text:
        return None
    text_tokens = Counter(re.findall(r"[a-zA-Z0-9_]+", text.lower()))
    metadata_text = " ".join(
        [
            str(chunk.get("source_title") or ""),
            str(chunk.get("source_name") or ""),
            str(chunk.get("source_type") or ""),
            " ".join(chunk.get("tags") or []),
        ]
    ).lower()
    match_terms: list[str] = []
    lexical_score = 0.0
    for term in query_terms:
        frequency = text_tokens.get(term, 0)
        metadata_hit = term in metadata_text
        if frequency or metadata_hit:
            match_terms.append(term)
            lexical_score += min(float(frequency), 5.0)
            if metadata_hit:
                lexical_score += 0.75
    normalized_query = re.sub(r"\s+", " ", query.lower()).strip()
    normalized_text = re.sub(r"\s+", " ", text.lower()).strip()
    if normalized_query and normalized_query in normalized_text:
        lexical_score += 5.0
    lexical_score = round(lexical_score / max(float(len(query_terms)), 1.0), 4) if lexical_score > 0 else 0.0
    semantic_score = 0.0
    if retrieval_mode == HYBRID_RETRIEVAL_MODE:
        semantic_score = _semantic_similarity(query, f"{metadata_text} {text}")
        if lexical_score <= 0 and semantic_score < HYBRID_MIN_SEMANTIC_SCORE:
            return None
    elif lexical_score <= 0:
        return None
    feedback_boost = _clamp_feedback_boost(
        float(feedback_boosts.get("chunks", {}).get(str(chunk.get("chunk_id") or ""), 0.0))
        + float(feedback_boosts.get("documents", {}).get(str(chunk.get("document_id") or ""), 0.0))
    )
    if retrieval_mode == HYBRID_RETRIEVAL_MODE:
        score = round(max(0.0, (lexical_score * 0.65) + (semantic_score * 4.0) + feedback_boost), 4)
    else:
        score = round(max(0.0, lexical_score + feedback_boost), 4)
    return {
        "chunk_id": chunk.get("chunk_id"),
        "document_id": chunk.get("document_id"),
        "document_title": chunk.get("source_title") or chunk.get("document_id") or "",
        "source_id": chunk.get("source_id"),
        "source_name": chunk.get("source_name") or "",
        "source_type": chunk.get("source_type") or "markdown",
        "ordinal": int(chunk.get("ordinal") or 0),
        "score": score,
        "feedback_boost": feedback_boost,
        "ranking_signals": {
            "retrieval_mode": retrieval_mode,
            "lexical_score": lexical_score,
            "semantic_score": semantic_score,
            "feedback_boost": feedback_boost,
            "rerank_score": score,
            "vector_model": LOCAL_SEMANTIC_VECTOR_MODEL if retrieval_mode == HYBRID_RETRIEVAL_MODE else None,
        },
        "match_terms": match_terms,
        "snippet": _snippet(text, match_terms),
        "text": text,
        "summary": chunk.get("summary") or _preview(text, max_length=180),
        "tags": list(chunk.get("tags") or []),
    }


def _snippet(text: str, match_terms: list[str], *, max_length: int = 420) -> str:
    compact = re.sub(r"\s+", " ", text).strip()
    if len(compact) <= max_length:
        return compact
    lowered = compact.lower()
    first_index = min(
        [lowered.find(term) for term in match_terms if lowered.find(term) >= 0],
        default=0,
    )
    start = max(0, first_index - 80)
    end = min(len(compact), start + max_length)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(compact) else ""
    return f"{prefix}{compact[start:end].strip()}{suffix}"


def _build_context_pack(
    *,
    retrieval_id: str,
    query: str,
    normalized_query: str,
    retrieval_mode: str,
    citations: list[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "format": "knowledge_context_pack.v1",
        "retrieval_id": retrieval_id,
        "query": query,
        "normalized_query": normalized_query,
        "retrieval_mode": retrieval_mode,
        "citation_count": len(citations),
        "sections": [
            {
                "citation_id": citation["citation_id"],
                "citation": citation["citation"],
                "heading": citation["document_title"],
                "source": {
                    "document_id": citation["document_id"],
                    "chunk_id": citation["chunk_id"],
                    "source_name": citation["source_name"],
                    "source_type": citation["source_type"],
                    "ordinal": citation["ordinal"],
                },
                "text": citation["text"],
            }
            for citation in citations
        ],
    }


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
