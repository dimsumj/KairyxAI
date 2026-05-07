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
KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE = "knowledge_vector_index"
KNOWLEDGE_VECTOR_RECORD_RESOURCE_TYPE = "knowledge_vector_record"

MAX_CHUNK_CHARS = 1600
MAX_DOCUMENT_CHARS = 250_000
MAX_RETRIEVAL_QUERY_CHARS = 500
MAX_RETRIEVAL_RESULTS = 20
LEXICAL_RETRIEVAL_MODE = "lexical_v1"
HYBRID_RETRIEVAL_MODE = "hybrid_v1"
LOCAL_SEMANTIC_VECTOR_MODEL = "local_semantic_hash_v1"
SEMANTIC_VECTOR_DIMENSIONS = 1024
DEFAULT_VECTOR_INDEX_ID = "kairyx_knowledge_default"
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
    def __init__(self, repository, settings=None):
        self.repository = repository
        self.vector_backend = _vector_backend_config(settings)

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
        affected_vector_indexes: set[str] = set()
        for chunk in self.list_chunks(document_id, include_archived=True):
            chunk_payload = {**chunk, "status": "archived", "archived_at": archived_at}
            self.repository.upsert_resource(
                KNOWLEDGE_CHUNK_RESOURCE_TYPE,
                chunk["chunk_id"],
                status="archived",
                name=document_id,
                payload=chunk_payload,
            )
            affected_vector_indexes.update(self._archive_vector_records(str(chunk.get("chunk_id") or ""), archived_at=archived_at))
        for index_id in sorted(affected_vector_indexes):
            self._refresh_vector_index_counts(index_id)
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
        vector_records = self._vector_records_by_chunk_id() if normalized_retrieval_mode == HYBRID_RETRIEVAL_MODE else {}
        query_vector = _semantic_vector(normalized_query) if normalized_retrieval_mode == HYBRID_RETRIEVAL_MODE else []
        scored = [
            match
            for chunk in self.repository.list_resources(KNOWLEDGE_CHUNK_RESOURCE_TYPE)
            if (match := _score_chunk(
                self._resource_to_chunk(chunk),
                query=normalized_query,
                query_vector=query_vector,
                query_terms=query_terms,
                retrieval_mode=normalized_retrieval_mode,
                tags=normalized_tags,
                source_types=normalized_source_types,
                document_ids=normalized_document_ids,
                include_archived=include_archived,
                feedback_boosts=feedback_boosts,
                vector_records=vector_records,
                active_vector_index_id=self.vector_backend["index_id"],
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
            "vector_index": self._retrieval_vector_index_summary(citations),
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

    def list_vector_indexes(self) -> list[Dict[str, Any]]:
        indexes = [
            self._resource_to_vector_index(record)
            for record in self.repository.list_resources(KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE)
        ]
        indexes.sort(key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""), reverse=True)
        return indexes

    def export_vector_index(self, index_id: str) -> Dict[str, Any] | None:
        normalized_index_id = _normalize_vector_index_id(index_id)
        record = self.repository.get_resource(KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE, normalized_index_id)
        if record is None:
            return None
        records = [
            _redact_vector_record_for_export(item)
            for item in self._list_vector_records(normalized_index_id)
            if item.get("status") != "archived"
        ]
        return {
            "format": "knowledge_vector_index.v1",
            "index": self._resource_to_vector_index(record),
            "records": records,
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

    def _active_vector_index_summary(self) -> Dict[str, Any]:
        record = self.repository.get_resource(KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE, self.vector_backend["index_id"])
        if record is None:
            return _vector_index_payload(self.vector_backend, record_count=0, document_count=0)
        payload = self._resource_to_vector_index(record)
        return _compact_vector_index_summary(payload)

    def _retrieval_vector_index_summary(self, citations: list[Dict[str, Any]]) -> Dict[str, Any]:
        index_ids = sorted(
            {
                str(dict(citation.get("ranking_signals") or {}).get("vector_index_id") or "").strip()
                for citation in citations
                if dict(citation.get("ranking_signals") or {}).get("vector_status") == "ready"
                and str(dict(citation.get("ranking_signals") or {}).get("vector_index_id") or "").strip()
            }
        )
        if not index_ids:
            if any(
                dict(citation.get("ranking_signals") or {}).get("vector_status") == "recomputed_fallback"
                for citation in citations
            ):
                return _recomputed_vector_index_summary()
            return self._active_vector_index_summary()
        if len(index_ids) == 1:
            record = self.repository.get_resource(KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE, index_ids[0])
            if record is None:
                return self._active_vector_index_summary()
            return _compact_vector_index_summary(self._resource_to_vector_index(record))
        summaries = [
            self._resource_to_vector_index(record)
            for index_id in index_ids
            if (record := self.repository.get_resource(KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE, index_id)) is not None
        ]
        if not summaries:
            return self._active_vector_index_summary()
        return {
            "index_id": "mixed",
            "embedding_provider": _collapse_summary_field(summaries, "embedding_provider", "mixed"),
            "embedding_model": _collapse_summary_field(summaries, "embedding_model", "mixed"),
            "vector_store": _collapse_summary_field(summaries, "vector_store", "mixed"),
            "vector_namespace": _collapse_summary_field(summaries, "vector_namespace", "mixed"),
            "dimensions": SEMANTIC_VECTOR_DIMENSIONS,
            "record_count": sum(int(item.get("record_count") or 0) for item in summaries),
            "document_count": sum(int(item.get("document_count") or 0) for item in summaries),
            "storage_mode": _collapse_summary_field(summaries, "storage_mode", "mixed"),
            "status": "active",
        }

    def _vector_records_by_chunk_id(self) -> Dict[str, list[Dict[str, Any]]]:
        records_by_chunk_id: Dict[str, list[Dict[str, Any]]] = {}
        for record in self.repository.list_resources(KNOWLEDGE_VECTOR_RECORD_RESOURCE_TYPE):
            payload = self._resource_to_vector_record(record)
            chunk_id = str(payload.get("chunk_id") or "")
            if not chunk_id or payload.get("status") == "archived":
                continue
            records_by_chunk_id.setdefault(chunk_id, []).append(payload)
        for records in records_by_chunk_id.values():
            records.sort(key=lambda item: str(item.get("updated_at") or item.get("materialized_at") or ""), reverse=True)
        return records_by_chunk_id

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
            embedding_text = " ".join(
                [
                    title,
                    source_name,
                    source_type,
                    " ".join(tags),
                    chunk_text,
                ]
            )
            vector_record = self._persist_vector_record(
                chunk_id=chunk_id,
                document_id=document_id,
                title=title,
                source_id=source_id,
                source_name=source_name,
                source_type=source_type,
                tags=tags,
                text=embedding_text,
            )
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
                "embedding": dict(vector_record.get("embedding") or {}),
            }
            record = self.repository.upsert_resource(
                KNOWLEDGE_CHUNK_RESOURCE_TYPE,
                chunk_id,
                status="active",
                name=document_id,
                payload=payload,
            )
            payloads.append(self._resource_to_chunk(record))
        self._refresh_vector_index_counts()
        return payloads

    def _persist_vector_record(
        self,
        *,
        chunk_id: str,
        document_id: str,
        title: str,
        source_id: str,
        source_name: str,
        source_type: str,
        tags: list[str],
        text: str,
    ) -> Dict[str, Any]:
        vector = _semantic_vector(text)
        vector_hash = _hash_text(",".join(f"{item:.6f}" for item in vector))[:24]
        index_id = self.vector_backend["index_id"]
        record_id = f"{index_id}:{chunk_id}"
        embedding = _embedding_metadata(self.vector_backend, vector_hash=vector_hash)
        embedding["vector_record_id"] = record_id
        payload = {
            "vector_record_id": record_id,
            "index_id": index_id,
            "chunk_id": chunk_id,
            "document_id": document_id,
            "source_id": source_id,
            "source_name": source_name,
            "source_type": source_type,
            "source_title": title,
            "tags": list(tags),
            "status": "active",
            "dimensions": SEMANTIC_VECTOR_DIMENSIONS,
            "vector_hash": vector_hash,
            "vector": vector,
            "embedding": embedding,
            "materialized_at": embedding["materialized_at"],
        }
        self._ensure_vector_index()
        record = self.repository.upsert_resource(
            KNOWLEDGE_VECTOR_RECORD_RESOURCE_TYPE,
            record_id,
            status="active",
            name=index_id,
            payload=payload,
        )
        return self._resource_to_vector_record(record)

    def _ensure_vector_index(self) -> Dict[str, Any]:
        current = self.repository.get_resource(KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE, self.vector_backend["index_id"])
        if current is not None:
            return self._resource_to_vector_index(current)
        payload = {
            **_vector_index_payload(self.vector_backend),
            "record_count": 0,
            "document_count": 0,
        }
        record = self.repository.upsert_resource(
            KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE,
            self.vector_backend["index_id"],
            status="active",
            name=self.vector_backend["index_id"],
            payload=payload,
        )
        return self._resource_to_vector_index(record)

    def _refresh_vector_index_counts(self, index_id: str | None = None) -> Dict[str, Any]:
        index_id = _normalize_vector_index_id(index_id or self.vector_backend["index_id"])
        records = [record for record in self._list_vector_records(index_id) if record.get("status") != "archived"]
        document_count = len({str(record.get("document_id") or "") for record in records if record.get("document_id")})
        current = self.repository.get_resource(KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE, index_id)
        current_payload = dict(current.get("payload") or {}) if current else {}
        if records:
            payload = _vector_index_payload_from_records(
                index_id,
                current_payload,
                records,
                record_count=len(records),
                document_count=document_count,
            )
        else:
            fallback = current_payload or _vector_index_payload(self.vector_backend)
            payload = {
                **fallback,
                "index_id": index_id,
                "status": fallback.get("status") or "active",
                "format": fallback.get("format") or "knowledge_vector_index.v1",
                "record_count": 0,
                "document_count": 0,
                "updated_at": datetime.utcnow().isoformat(),
            }
        record = self.repository.upsert_resource(
            KNOWLEDGE_VECTOR_INDEX_RESOURCE_TYPE,
            index_id,
            status="active",
            name=index_id,
            payload=payload,
        )
        return self._resource_to_vector_index(record)

    def _list_vector_records(self, index_id: str) -> list[Dict[str, Any]]:
        records = [
            self._resource_to_vector_record(record)
            for record in self.repository.list_resources(KNOWLEDGE_VECTOR_RECORD_RESOURCE_TYPE, name=index_id)
        ]
        records.sort(key=lambda record: (str(record.get("document_id") or ""), str(record.get("chunk_id") or "")))
        return records

    def _archive_vector_records(self, chunk_id: str, *, archived_at: str) -> set[str]:
        if not chunk_id:
            return set()
        affected_indexes: set[str] = set()
        for current in self.repository.list_resources(KNOWLEDGE_VECTOR_RECORD_RESOURCE_TYPE):
            record = self._resource_to_vector_record(current)
            if record.get("chunk_id") != chunk_id or record.get("status") == "archived":
                continue
            index_id = _normalize_vector_index_id(str(record.get("index_id") or self.vector_backend["index_id"]))
            payload = {**record, "status": "archived", "archived_at": archived_at}
            self.repository.upsert_resource(
                KNOWLEDGE_VECTOR_RECORD_RESOURCE_TYPE,
                str(record.get("vector_record_id") or current.get("resource_id")),
                status="archived",
                name=index_id,
                payload=payload,
            )
            affected_indexes.add(index_id)
        return affected_indexes

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

    @staticmethod
    def _resource_to_vector_index(record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        return {
            **payload,
            "index_id": payload.get("index_id") or record.get("resource_id"),
            "status": payload.get("status") or record.get("status") or "active",
            "created_at": record.get("created_at"),
            "updated_at": record.get("updated_at"),
            "tenant_id": record.get("tenant_id") or payload.get("tenant_id"),
            "project_id": record.get("project_id") or payload.get("project_id"),
        }

    @staticmethod
    def _resource_to_vector_record(record: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(record.get("payload") or {})
        return {
            **payload,
            "vector_record_id": payload.get("vector_record_id") or record.get("resource_id"),
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


def _vector_backend_config(settings=None) -> Dict[str, Any]:
    provider = _normalize_provider_name(getattr(settings, "knowledge_embedding_provider", "local_hash") if settings else "local_hash")
    model = _clean_text(getattr(settings, "knowledge_embedding_model", LOCAL_SEMANTIC_VECTOR_MODEL) if settings else LOCAL_SEMANTIC_VECTOR_MODEL, max_length=120) or LOCAL_SEMANTIC_VECTOR_MODEL
    vector_store = _normalize_provider_name(getattr(settings, "knowledge_vector_store", "control_plane") if settings else "control_plane")
    index_id = _normalize_vector_index_id(getattr(settings, "knowledge_vector_index", DEFAULT_VECTOR_INDEX_ID) if settings else DEFAULT_VECTOR_INDEX_ID)
    namespace = _normalize_provider_name(getattr(settings, "knowledge_vector_namespace", "default") if settings else "default") or "default"
    secret_ref = _clean_text(getattr(settings, "knowledge_vector_secret_ref", "") if settings else "", max_length=240)
    storage_mode = "control_plane_vector_record" if vector_store == "control_plane" else "external_vector_store_shadow_index"
    return {
        "embedding_provider": provider,
        "embedding_model": model,
        "vector_store": vector_store,
        "index_id": index_id,
        "vector_namespace": namespace,
        "secret_ref_configured": bool(secret_ref),
        "storage_mode": storage_mode,
    }


def _normalize_provider_name(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", str(value or "").strip().lower()).strip("_")
    return normalized[:80]


def _normalize_vector_index_id(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_.:-]+", "_", str(value or "").strip()).strip("_")
    return normalized[:120] or DEFAULT_VECTOR_INDEX_ID


def _vector_index_payload(config: Dict[str, Any], *, record_count: int = 0, document_count: int = 0) -> Dict[str, Any]:
    return {
        "index_id": config["index_id"],
        "status": "active",
        "format": "knowledge_vector_index.v1",
        "embedding_provider": config["embedding_provider"],
        "embedding_model": config["embedding_model"],
        "vector_store": config["vector_store"],
        "vector_namespace": config["vector_namespace"],
        "dimensions": SEMANTIC_VECTOR_DIMENSIONS,
        "distance_metric": "cosine",
        "record_count": int(record_count or 0),
        "document_count": int(document_count or 0),
        "storage_mode": config["storage_mode"],
        "secret_ref_configured": bool(config.get("secret_ref_configured")),
        "updated_at": datetime.utcnow().isoformat(),
    }


def _vector_index_payload_from_records(
    index_id: str,
    current_payload: Dict[str, Any],
    records: list[Dict[str, Any]],
    *,
    record_count: int,
    document_count: int,
) -> Dict[str, Any]:
    return {
        **current_payload,
        "index_id": index_id,
        "status": current_payload.get("status") or "active",
        "format": current_payload.get("format") or "knowledge_vector_index.v1",
        "embedding_provider": _collapse_embedding_field(records, "provider", current_payload.get("embedding_provider") or "local_hash"),
        "embedding_model": _collapse_embedding_field(records, "model", current_payload.get("embedding_model") or LOCAL_SEMANTIC_VECTOR_MODEL),
        "vector_store": _collapse_embedding_field(records, "vector_store", current_payload.get("vector_store") or "control_plane"),
        "vector_namespace": _collapse_embedding_field(records, "vector_namespace", current_payload.get("vector_namespace") or "default"),
        "dimensions": _collapse_embedding_dimensions(records, int(current_payload.get("dimensions") or SEMANTIC_VECTOR_DIMENSIONS)),
        "distance_metric": current_payload.get("distance_metric") or "cosine",
        "record_count": int(record_count or 0),
        "document_count": int(document_count or 0),
        "storage_mode": _collapse_embedding_field(records, "storage_mode", current_payload.get("storage_mode") or "control_plane_vector_record"),
        "secret_ref_configured": any(bool(dict(record.get("embedding") or {}).get("secret_ref_configured")) for record in records),
        "updated_at": datetime.utcnow().isoformat(),
    }


def _compact_vector_index_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key
        in {
            "index_id",
            "embedding_provider",
            "embedding_model",
            "vector_store",
            "vector_namespace",
            "dimensions",
            "record_count",
            "document_count",
            "storage_mode",
            "status",
        }
    }


def _recomputed_vector_index_summary() -> Dict[str, Any]:
    return {
        "index_id": "recomputed_fallback",
        "embedding_provider": "local_hash",
        "embedding_model": LOCAL_SEMANTIC_VECTOR_MODEL,
        "vector_store": "none",
        "vector_namespace": "none",
        "dimensions": SEMANTIC_VECTOR_DIMENSIONS,
        "record_count": 0,
        "document_count": 0,
        "storage_mode": "recomputed_fallback",
        "status": "fallback",
    }


def _collapse_summary_field(summaries: list[Dict[str, Any]], field_name: str, fallback: str) -> str:
    values = sorted(
        {
            str(summary.get(field_name) or "").strip()
            for summary in summaries
            if str(summary.get(field_name) or "").strip()
        }
    )
    if not values:
        return fallback
    if len(values) == 1:
        return values[0]
    return "mixed"


def _collapse_embedding_field(records: list[Dict[str, Any]], field_name: str, fallback: str) -> str:
    values = sorted(
        {
            str(dict(record.get("embedding") or {}).get(field_name) or "").strip()
            for record in records
            if str(dict(record.get("embedding") or {}).get(field_name) or "").strip()
        }
    )
    if not values:
        return fallback
    if len(values) == 1:
        return values[0]
    return "mixed"


def _collapse_embedding_dimensions(records: list[Dict[str, Any]], fallback: int) -> int:
    values: set[int] = set()
    for record in records:
        embedding = dict(record.get("embedding") or {})
        try:
            values.add(int(embedding.get("dimensions") or record.get("dimensions") or 0))
        except (TypeError, ValueError):
            continue
    values.discard(0)
    if len(values) == 1:
        return next(iter(values))
    return fallback or SEMANTIC_VECTOR_DIMENSIONS


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


def _embedding_metadata(config: Dict[str, Any], *, vector_hash: str) -> Dict[str, Any]:
    if config["embedding_provider"] == "local_hash" and config["vector_store"] == "control_plane":
        vector_ref = f"inline:{config['embedding_model']}:{vector_hash}"
    else:
        vector_ref = f"{config['vector_store']}://{config['index_id']}/{vector_hash}"
    return {
        "status": "ready",
        "provider": config["embedding_provider"],
        "model": config["embedding_model"],
        "vector_store": config["vector_store"],
        "vector_index_id": config["index_id"],
        "vector_namespace": config["vector_namespace"],
        "vector_record_id": "",
        "vector_ref": vector_ref,
        "dimensions": SEMANTIC_VECTOR_DIMENSIONS,
        "storage_mode": config["storage_mode"],
        "secret_ref_configured": bool(config.get("secret_ref_configured")),
        "materialized_at": datetime.utcnow().isoformat(),
    }


def _redact_vector_record_for_export(record: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(record)
    payload.pop("vector", None)
    embedding = dict(payload.get("embedding") or {})
    embedding.pop("secret_ref", None)
    payload["embedding"] = embedding
    return payload


def _semantic_similarity(query: str, value: str) -> float:
    query_vector = _semantic_vector(query)
    value_vector = _semantic_vector(value)
    return _semantic_similarity_from_vectors(query_vector, value_vector)


def _semantic_similarity_from_vectors(query_vector: list[float], value_vector: list[float]) -> float:
    if not query_vector or not value_vector or len(query_vector) != len(value_vector):
        return 0.0
    score = sum(left * right for left, right in zip(query_vector, value_vector))
    return round(max(0.0, min(1.0, score)), 4)


def _select_vector_record_for_chunk(
    chunk: Dict[str, Any],
    candidates: list[Dict[str, Any]],
    *,
    active_index_id: str,
) -> Dict[str, Any]:
    active_candidates = [record for record in candidates if record.get("status") != "archived"]
    if not active_candidates:
        return {}
    chunk_embedding = dict(chunk.get("embedding") or {})
    preferred_index_ids = [
        str(chunk_embedding.get("vector_index_id") or "").strip(),
        active_index_id,
    ]
    for index_id in preferred_index_ids:
        if not index_id:
            continue
        for record in active_candidates:
            if str(record.get("index_id") or "") == index_id:
                return record
    return active_candidates[0]


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
    query_vector: list[float],
    query_terms: list[str],
    retrieval_mode: str,
    tags: list[str],
    source_types: list[str],
    document_ids: list[str],
    include_archived: bool,
    feedback_boosts: Dict[str, Dict[str, float]],
    vector_records: Dict[str, list[Dict[str, Any]]],
    active_vector_index_id: str,
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
    vector_record = _select_vector_record_for_chunk(
        chunk,
        vector_records.get(str(chunk.get("chunk_id") or ""), []),
        active_index_id=active_vector_index_id,
    )
    vector_embedding = dict(vector_record.get("embedding") or {}) if vector_record else {}
    vector_status = "not_used"
    if retrieval_mode == HYBRID_RETRIEVAL_MODE:
        if vector_record and vector_record.get("status") != "archived":
            semantic_score = _semantic_similarity_from_vectors(query_vector, list(vector_record.get("vector") or []))
            vector_status = "ready"
        else:
            semantic_score = _semantic_similarity(query, f"{metadata_text} {text}")
            vector_status = "recomputed_fallback"
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
            "vector_status": vector_status,
            "vector_model": vector_embedding.get("model") or (LOCAL_SEMANTIC_VECTOR_MODEL if retrieval_mode == HYBRID_RETRIEVAL_MODE else None),
            "embedding_provider": vector_embedding.get("provider") or ("local_hash" if retrieval_mode == HYBRID_RETRIEVAL_MODE else None),
            "vector_store": vector_embedding.get("vector_store"),
            "vector_index_id": vector_embedding.get("vector_index_id"),
            "vector_record_id": vector_embedding.get("vector_record_id"),
            "storage_mode": vector_embedding.get("storage_mode"),
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
