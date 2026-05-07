from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.api.schemas.knowledge import (
    KnowledgeChunkListResponse,
    KnowledgeDocumentCreateRequest,
    KnowledgeDocumentExportResponse,
    KnowledgeDocumentListResponse,
    KnowledgeDocumentResponse,
    KnowledgeRetrievalExportResponse,
    KnowledgeRetrievalListResponse,
    KnowledgeRetrievalRequest,
    KnowledgeRetrievalResponse,
)
from app.application.knowledge import KnowledgeService
from app.core.deps import get_knowledge_service
from app.core.governance import GovernanceContext, ensure_permission, get_governance_context, record_audit


router = APIRouter(prefix="/knowledge", tags=["knowledge"])


def _with_audit(
    service: KnowledgeService,
    context: GovernanceContext,
    *,
    action_type: str,
    resource_type: str,
    resource_id: str | None,
    payload: Any,
):
    audit_payload = {
        "resource_id": resource_id,
        "response_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
        "item_count": len(payload.get("items") or []) if isinstance(payload, dict) else None,
        "chunk_count": len(payload.get("chunks") or []) if isinstance(payload, dict) else None,
        "citation_count": len(payload.get("citations") or []) if isinstance(payload, dict) else None,
        "format": payload.get("format") if isinstance(payload, dict) else None,
    }
    audit_id = record_audit(
        service.repository,
        context,
        action_type=action_type,
        resource_type=resource_type,
        resource_id=resource_id,
        payload=audit_payload,
    )
    if isinstance(payload, dict):
        return {
            **payload,
            "audit_id": audit_id,
            "tenant_id": context.tenant_id,
            "project_id": context.project_id,
            "correlation_id": context.correlation_id,
            "masked_fields": [],
        }
    return {
        "data": payload,
        "audit_id": audit_id,
        "tenant_id": context.tenant_id,
        "project_id": context.project_id,
        "correlation_id": context.correlation_id,
        "masked_fields": [],
    }


@router.get("/documents", response_model=KnowledgeDocumentListResponse)
def list_knowledge_documents(
    http_request: Request,
    include_archived: bool = Query(default=False),
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.read")
    payload = {"items": service.list_documents(include_archived=include_archived)}
    return _with_audit(
        service,
        context,
        action_type="knowledge_documents_read",
        resource_type="knowledge_document",
        resource_id=None,
        payload=payload,
    )


@router.get("/retrievals", response_model=KnowledgeRetrievalListResponse)
def list_knowledge_retrievals(
    http_request: Request,
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.read")
    payload = {"items": service.list_retrievals()}
    return _with_audit(
        service,
        context,
        action_type="knowledge_retrievals_read",
        resource_type="knowledge_retrieval",
        resource_id=None,
        payload=payload,
    )


@router.post("/retrievals", response_model=KnowledgeRetrievalResponse, status_code=status.HTTP_201_CREATED)
def create_knowledge_retrieval(
    request: KnowledgeRetrievalRequest,
    http_request: Request,
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.read")
    try:
        payload = service.retrieve(
            query=request.query,
            top_k=request.top_k,
            tags=request.tags,
            source_types=request.source_types,
            document_ids=request.document_ids,
            include_archived=request.include_archived,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _with_audit(
        service,
        context,
        action_type="knowledge_retrieval_completed",
        resource_type="knowledge_retrieval",
        resource_id=payload["retrieval_id"],
        payload=payload,
    )


@router.get("/retrievals/{retrieval_id}", response_model=KnowledgeRetrievalResponse)
def get_knowledge_retrieval(
    retrieval_id: str,
    http_request: Request,
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.read")
    payload = service.get_retrieval(retrieval_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Knowledge retrieval '{retrieval_id}' not found.")
    return _with_audit(
        service,
        context,
        action_type="knowledge_retrieval_read",
        resource_type="knowledge_retrieval",
        resource_id=retrieval_id,
        payload=payload,
    )


@router.get("/retrievals/{retrieval_id}/export", response_model=KnowledgeRetrievalExportResponse)
def export_knowledge_retrieval(
    retrieval_id: str,
    http_request: Request,
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.read")
    payload = service.export_retrieval(retrieval_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Knowledge retrieval '{retrieval_id}' not found.")
    return _with_audit(
        service,
        context,
        action_type="knowledge_retrieval_exported",
        resource_type="knowledge_retrieval",
        resource_id=retrieval_id,
        payload=payload,
    )


@router.post("/documents", response_model=KnowledgeDocumentResponse, status_code=status.HTTP_201_CREATED)
def create_knowledge_document(
    request: KnowledgeDocumentCreateRequest,
    http_request: Request,
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.write")
    try:
        payload = service.create_document(
            title=request.title,
            content=request.content,
            source_type=request.source_type,
            source_id=request.source_id,
            source_name=request.source_name,
            source_uri=request.source_uri,
            tags=request.tags,
            visibility=request.visibility,
            provenance=request.provenance,
            metadata=request.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return _with_audit(
        service,
        context,
        action_type="knowledge_document_ingested",
        resource_type="knowledge_document",
        resource_id=payload["document_id"],
        payload=payload,
    )


@router.get("/documents/{document_id}", response_model=KnowledgeDocumentResponse)
def get_knowledge_document(
    document_id: str,
    http_request: Request,
    include_chunks: bool = Query(default=False),
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.read")
    payload = service.get_document(document_id, include_chunks=include_chunks)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Knowledge document '{document_id}' not found.")
    return _with_audit(
        service,
        context,
        action_type="knowledge_document_read",
        resource_type="knowledge_document",
        resource_id=document_id,
        payload=payload,
    )


@router.get("/documents/{document_id}/chunks", response_model=KnowledgeChunkListResponse)
def list_knowledge_chunks(
    document_id: str,
    http_request: Request,
    include_archived: bool = Query(default=False),
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.read")
    if service.get_document(document_id, include_chunks=False) is None:
        raise HTTPException(status_code=404, detail=f"Knowledge document '{document_id}' not found.")
    payload = {"items": service.list_chunks(document_id, include_archived=include_archived)}
    return _with_audit(
        service,
        context,
        action_type="knowledge_chunks_read",
        resource_type="knowledge_chunk",
        resource_id=document_id,
        payload=payload,
    )


@router.get("/documents/{document_id}/export", response_model=KnowledgeDocumentExportResponse)
def export_knowledge_document(
    document_id: str,
    http_request: Request,
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.read")
    payload = service.export_document(document_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Knowledge document '{document_id}' not found.")
    return _with_audit(
        service,
        context,
        action_type="knowledge_document_exported",
        resource_type="knowledge_document",
        resource_id=document_id,
        payload=payload,
    )


@router.post("/documents/{document_id}/archive", response_model=KnowledgeDocumentResponse)
def archive_knowledge_document(
    document_id: str,
    http_request: Request,
    service: KnowledgeService = Depends(get_knowledge_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "knowledge.write")
    try:
        payload = service.archive_document(document_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Knowledge document '{document_id}' not found.")
    return _with_audit(
        service,
        context,
        action_type="knowledge_document_archived",
        resource_type="knowledge_document",
        resource_id=document_id,
        payload=payload,
    )
