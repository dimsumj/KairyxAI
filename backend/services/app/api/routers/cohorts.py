from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.api.schemas.cohorts import CohortCreateRequest, CohortMemberPage, CohortResponse, CohortUpdateRequest, CohortVersionListResponse
from app.application.cohorts import CohortService
from app.core.errors import ResourceLockedError
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_cohort_service


router = APIRouter(prefix="/cohorts", tags=["cohorts"])


@router.get("", response_model=dict)
def list_cohorts(service: CohortService = Depends(get_cohort_service)):
    return {"items": service.list_cohorts()}


@router.post("", response_model=CohortResponse, status_code=status.HTTP_201_CREATED)
def create_cohort(request: CohortCreateRequest, http_request: Request, service: CohortService = Depends(get_cohort_service)):
    ensure_permission(get_governance_context(http_request), "cohorts.create")
    try:
        return service.create_cohort(
            name=request.name,
            cohort_type=request.type,
            definition=request.definition,
            refresh_mode=request.refresh_mode,
            owner=request.owner,
            description=request.description,
            tags=request.tags,
            activate=request.activate,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/{cohort_id}", response_model=CohortResponse)
def get_cohort(cohort_id: str, service: CohortService = Depends(get_cohort_service)):
    cohort = service.get_cohort(cohort_id)
    if cohort is None:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
    return cohort


@router.patch("/{cohort_id}", response_model=CohortResponse)
def update_cohort(
    cohort_id: str,
    request: CohortUpdateRequest,
    http_request: Request,
    service: CohortService = Depends(get_cohort_service),
):
    ensure_permission(get_governance_context(http_request), "cohorts.update")
    try:
        return service.update_cohort(cohort_id, request.model_dump(exclude_none=True))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")


@router.get("/{cohort_id}/members", response_model=CohortMemberPage)
def get_cohort_members(
    cohort_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    service: CohortService = Depends(get_cohort_service),
):
    try:
        return service.list_members(cohort_id, page=page, page_size=page_size)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")


@router.post("/{cohort_id}/refresh", response_model=CohortResponse)
def refresh_cohort(cohort_id: str, http_request: Request, service: CohortService = Depends(get_cohort_service)):
    ensure_permission(get_governance_context(http_request), "cohorts.refresh")
    try:
        return service.refresh_cohort(cohort_id, force=True)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/{cohort_id}/activate", response_model=CohortResponse)
def activate_cohort(cohort_id: str, http_request: Request, service: CohortService = Depends(get_cohort_service)):
    ensure_permission(get_governance_context(http_request), "cohorts.activate")
    try:
        return service.activate_cohort(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/{cohort_id}/pause", response_model=CohortResponse)
def pause_cohort(cohort_id: str, http_request: Request, service: CohortService = Depends(get_cohort_service)):
    ensure_permission(get_governance_context(http_request), "cohorts.pause")
    try:
        return service.pause_cohort(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")


@router.get("/{cohort_id}/versions", response_model=CohortVersionListResponse)
def list_cohort_versions(cohort_id: str, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.list_versions(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")


@router.get("/{cohort_id}/refresh-jobs", response_model=dict)
def list_cohort_refresh_jobs(
    cohort_id: str,
    http_request: Request,
    service: CohortService = Depends(get_cohort_service),
):
    ensure_permission(get_governance_context(http_request), "cohorts.refresh_jobs.read")
    try:
        return service.list_refresh_jobs(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")


@router.get("/{cohort_id}/overview", response_model=dict)
def get_cohort_overview(
    cohort_id: str,
    http_request: Request,
    service: CohortService = Depends(get_cohort_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "cohorts.overview.read")
    try:
        payload = service.get_overview(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="cohort_overview_read",
        resource_type="cohort",
        resource_id=cohort_id,
        payload=payload,
    )


@router.get("/{cohort_id}/metrics", response_model=dict)
def get_cohort_metrics(cohort_id: str, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.get_metrics(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")


@router.get("/{cohort_id}/compare", response_model=dict)
def compare_cohort_versions(
    cohort_id: str,
    base_version: int = Query(..., ge=1),
    target_version: int = Query(..., ge=1),
    service: CohortService = Depends(get_cohort_service),
):
    try:
        return service.compare_versions(cohort_id, base_version=base_version, target_version=target_version)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort snapshot comparison is not available for '{cohort_id}'.")


@router.post("/{cohort_id}/rollback", response_model=CohortResponse)
def rollback_cohort(cohort_id: str, version: int, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.rollback_cohort(cohort_id, version)
    except KeyError as exc:
        detail = f"Cohort '{cohort_id}' not found." if str(exc) == cohort_id else f"Cohort version '{version}' not found."
        raise HTTPException(status_code=404, detail=detail)


@router.post("/{cohort_id}/archive", response_model=CohortResponse)
def archive_cohort(cohort_id: str, http_request: Request, service: CohortService = Depends(get_cohort_service)):
    ensure_permission(get_governance_context(http_request), "cohorts.archive")
    try:
        return service.archive_cohort(cohort_id)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")


@router.post("/{cohort_id}/restore", response_model=CohortResponse)
def restore_cohort(cohort_id: str, http_request: Request, service: CohortService = Depends(get_cohort_service)):
    ensure_permission(get_governance_context(http_request), "cohorts.restore")
    try:
        return service.restore_cohort(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.delete("/{cohort_id}/permanent", response_model=dict)
def permanently_delete_cohort(
    cohort_id: str,
    http_request: Request,
    service: CohortService = Depends(get_cohort_service),
):
    ensure_permission(get_governance_context(http_request), "cohorts.permanent_delete")
    try:
        return service.permanent_delete(cohort_id)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
