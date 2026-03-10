from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.schemas.cohorts import CohortCreateRequest, CohortMemberPage, CohortResponse, CohortVersionListResponse
from app.application.cohorts import CohortService
from app.core.deps import get_cohort_service


router = APIRouter(prefix="/cohorts", tags=["cohorts"])


@router.get("", response_model=dict)
def list_cohorts(service: CohortService = Depends(get_cohort_service)):
    return {"items": service.list_cohorts()}


@router.post("", response_model=CohortResponse, status_code=status.HTTP_201_CREATED)
def create_cohort(request: CohortCreateRequest, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.create_cohort(
            name=request.name,
            cohort_type=request.type,
            definition=request.definition,
            refresh_mode=request.refresh_mode,
            owner=request.owner,
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
def refresh_cohort(cohort_id: str, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.refresh_cohort(cohort_id, force=True)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/{cohort_id}/activate", response_model=CohortResponse)
def activate_cohort(cohort_id: str, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.activate_cohort(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.post("/{cohort_id}/pause", response_model=CohortResponse)
def pause_cohort(cohort_id: str, service: CohortService = Depends(get_cohort_service)):
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


@router.post("/{cohort_id}/rollback", response_model=CohortResponse)
def rollback_cohort(cohort_id: str, version: int, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.rollback_cohort(cohort_id, version)
    except KeyError as exc:
        detail = f"Cohort '{cohort_id}' not found." if str(exc) == cohort_id else f"Cohort version '{version}' not found."
        raise HTTPException(status_code=404, detail=detail)


@router.post("/{cohort_id}/archive", response_model=CohortResponse)
def archive_cohort(cohort_id: str, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.archive_cohort(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")


@router.post("/{cohort_id}/restore", response_model=CohortResponse)
def restore_cohort(cohort_id: str, service: CohortService = Depends(get_cohort_service)):
    try:
        return service.restore_cohort(cohort_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{cohort_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
