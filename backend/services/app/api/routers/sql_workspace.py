from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

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
from app.core.deps import get_cohort_service, get_sql_workspace_service


router = APIRouter(prefix="/sql-workspace", tags=["sql-workspace"])


@router.post("/preview", response_model=SqlPreviewResponse)
def preview_sql(request: SqlPreviewRequest, service: SqlWorkspaceService = Depends(get_sql_workspace_service)):
    try:
        return service.preview(request.sql, limit=request.limit, timeout_seconds=request.timeout_seconds)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/queries", response_model=dict)
def list_saved_queries(service: SqlWorkspaceService = Depends(get_sql_workspace_service)):
    return {"items": service.list_saved_queries()}


@router.post("/queries", response_model=SavedQueryResponse, status_code=status.HTTP_201_CREATED)
def create_saved_query(request: SavedQueryCreateRequest, service: SqlWorkspaceService = Depends(get_sql_workspace_service)):
    try:
        return service.create_saved_query(request.name, request.sql, request.description)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/queries/{query_id}/cohort", response_model=CohortResponse, status_code=status.HTTP_201_CREATED)
def create_cohort_from_saved_query(
    query_id: str,
    request: SavedQueryToCohortRequest,
    sql_service: SqlWorkspaceService = Depends(get_sql_workspace_service),
    cohort_service: CohortService = Depends(get_cohort_service),
):
    query = sql_service.get_saved_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail=f"Saved query '{query_id}' not found.")
    try:
        return cohort_service.create_cohort(
            name=request.name,
            cohort_type="sql",
            definition={"sql": query["sql"]},
            refresh_mode=request.refresh_mode,
            owner=request.owner,
            activate=request.activate,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
