from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.schemas.cohorts import CohortResponse
from app.api.schemas.sql_workspace import (
    SavedQueryCreateRequest,
    SavedQueryResponse,
    SavedQueryToCohortRequest,
    SqlPreviewRequest,
    SqlPreviewResponse,
)
from app.application.cohorts import CohortService
from app.application.sql_workspace import SqlWorkspaceService
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_cohort_service, get_sql_workspace_service


router = APIRouter(prefix="/sql-workspace", tags=["sql-workspace"])


@router.post("/preview", response_model=SqlPreviewResponse)
def preview_sql(request: SqlPreviewRequest, http_request: Request, service: SqlWorkspaceService = Depends(get_sql_workspace_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "sql_workspace.preview")
    try:
        payload = service.preview(
            request.sql,
            limit=request.limit,
            timeout_seconds=request.timeout_seconds,
            scan_limit_rows=request.scan_limit_rows,
        )
        return build_audited_response(
            service.repository,
            context,
            action_type="sql_workspace_preview",
            resource_type="sql_query_audit",
            resource_id=None,
            payload=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/queries", response_model=dict)
def list_saved_queries(http_request: Request, service: SqlWorkspaceService = Depends(get_sql_workspace_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "sql_workspace.queries.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="sql_workspace_queries_read",
        resource_type="saved_query",
        resource_id=None,
        payload={"items": service.list_saved_queries()},
    )


@router.post("/queries", response_model=SavedQueryResponse, status_code=status.HTTP_201_CREATED)
def create_saved_query(request: SavedQueryCreateRequest, http_request: Request, service: SqlWorkspaceService = Depends(get_sql_workspace_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "sql_workspace.queries.create")
    try:
        payload = service.create_saved_query(request.name, request.sql, request.description)
        return build_audited_response(
            service.repository,
            context,
            action_type="sql_workspace_query_created",
            resource_type="saved_query",
            resource_id=payload["query_id"],
            payload=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/queries/{query_id}/cohort", response_model=dict, status_code=status.HTTP_201_CREATED)
def create_cohort_from_saved_query(
    query_id: str,
    request: SavedQueryToCohortRequest,
    http_request: Request,
    sql_service: SqlWorkspaceService = Depends(get_sql_workspace_service),
    cohort_service: CohortService = Depends(get_cohort_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "sql_workspace.query_to_cohort")
    query = sql_service.get_saved_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Saved query '{query_id}' not found.")
    try:
        payload = cohort_service.create_cohort(
            name=request.name,
            cohort_type="sql",
            definition={"sql": query["sql"]},
            refresh_mode=request.refresh_mode,
            owner=request.owner,
            activate=request.activate,
        )
        return build_audited_response(
            sql_service.repository,
            context,
            action_type="sql_workspace_query_to_cohort",
            resource_type="cohort",
            resource_id=payload["cohort_id"],
            payload=payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
