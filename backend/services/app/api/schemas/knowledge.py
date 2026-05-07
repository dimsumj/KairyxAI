from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field


class KnowledgeDocumentCreateRequest(BaseModel):
    title: str = Field(min_length=1, max_length=180)
    content: str = Field(min_length=1)
    source_type: str = "markdown"
    source_id: str | None = None
    source_name: str | None = None
    source_uri: str | None = None
    tags: list[str] = Field(default_factory=list)
    visibility: str = "workspace"
    provenance: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class KnowledgeRetrievalRequest(BaseModel):
    query: str = Field(min_length=2, max_length=500)
    top_k: int = Field(default=5, ge=1, le=20)
    tags: list[str] = Field(default_factory=list)
    source_types: list[str] = Field(default_factory=list)
    document_ids: list[str] = Field(default_factory=list)
    include_archived: bool = False


class KnowledgeIngestionJobResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    ingestion_job_id: str
    document_id: str
    source_id: str
    status: str
    chunk_count: int = 0
    rejected_section_count: int = 0
    character_count: int = 0
    content_hash: str
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class KnowledgeChunkResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    chunk_id: str
    document_id: str
    source_id: str
    source_name: str = ""
    source_type: str = "markdown"
    source_title: str = ""
    ordinal: int
    text: str
    summary: str = ""
    content_hash: str
    character_count: int = 0
    token_estimate: int = 0
    tags: list[str] = Field(default_factory=list)
    visibility: str = "workspace"
    status: str = "active"
    embedding: Dict[str, Any] = Field(default_factory=dict)
    archived_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class KnowledgeCitationResponse(BaseModel):
    citation_id: str
    citation: str
    rank: int
    score: float
    chunk_id: str
    document_id: str
    document_title: str = ""
    source_id: str
    source_name: str = ""
    source_type: str = "markdown"
    ordinal: int
    match_terms: list[str] = Field(default_factory=list)
    feedback_boost: float = 0.0
    ranking_signals: Dict[str, Any] = Field(default_factory=dict)
    snippet: str = ""
    text: str = ""
    summary: str = ""
    tags: list[str] = Field(default_factory=list)


class KnowledgeDocumentResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    document_id: str
    source_id: str
    source_name: str = ""
    title: str
    source_type: str = "markdown"
    source_uri: str = ""
    visibility: str = "workspace"
    tags: list[str] = Field(default_factory=list)
    status: str = "active"
    content_hash: str
    content_preview: str = ""
    character_count: int = 0
    chunk_count: int = 0
    provenance: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    export: Dict[str, Any] = Field(default_factory=dict)
    ingestion_job: KnowledgeIngestionJobResponse | None = None
    chunks: list[KnowledgeChunkResponse] | None = None
    archived_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: list[str] = Field(default_factory=list)


class KnowledgeDocumentListResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: list[str] = Field(default_factory=list)
    items: list[KnowledgeDocumentResponse] = Field(default_factory=list)


class KnowledgeChunkListResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: list[str] = Field(default_factory=list)
    items: list[KnowledgeChunkResponse] = Field(default_factory=list)


class KnowledgeDocumentExportResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: list[str] = Field(default_factory=list)
    format: str
    document: KnowledgeDocumentResponse
    chunks: list[KnowledgeChunkResponse] = Field(default_factory=list)


class KnowledgeRetrievalResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: list[str] = Field(default_factory=list)
    retrieval_id: str
    query: str
    normalized_query: str = ""
    retrieval_mode: str = "lexical_v1"
    status: str = "completed"
    top_k: int = 5
    result_count: int = 0
    filters: Dict[str, Any] = Field(default_factory=dict)
    citations: list[KnowledgeCitationResponse] = Field(default_factory=list)
    context_pack: Dict[str, Any] = Field(default_factory=dict)
    export: Dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None
    created_by: str = "system"
    updated_by: str = "system"


class KnowledgeRetrievalListResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: list[str] = Field(default_factory=list)
    items: list[KnowledgeRetrievalResponse] = Field(default_factory=list)


class KnowledgeRetrievalExportResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: list[str] = Field(default_factory=list)
    format: str
    retrieval: KnowledgeRetrievalResponse
