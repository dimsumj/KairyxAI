from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.schemas.mappings import MappingResponse, MappingUpdateRequest, MappingVersionListResponse
from app.application.mappings import MappingService
from app.core.deps import get_mapping_service


router = APIRouter(prefix="/mappings", tags=["mappings"])


@router.get("/{connector_name}", response_model=MappingResponse)
def get_mapping(
    connector_name: str,
    scope_type: str = Query("source"),
    scope_key: str | None = Query(None),
    service: MappingService = Depends(get_mapping_service),
):
    return service.get_mapping(connector_name, scope_type=scope_type, scope_key=scope_key)


@router.get("/{connector_name}/effective", response_model=MappingResponse)
def get_effective_mapping(
    connector_name: str,
    job_id: str | None = Query(None),
    service: MappingService = Depends(get_mapping_service),
):
    mapping = service.get_effective_mapping(connector_name, job_id=job_id)
    return {
        "connector_name": connector_name,
        "scope_type": "effective",
        "scope_key": job_id,
        "mapping": mapping,
        "required_coverage": 100.0,
        "effective_mapping": mapping,
    }


@router.get("/{connector_name}/versions", response_model=MappingVersionListResponse)
def list_mapping_versions(
    connector_name: str,
    scope_type: str = Query("source"),
    scope_key: str | None = Query(None),
    service: MappingService = Depends(get_mapping_service),
):
    return service.list_versions(connector_name, scope_type=scope_type, scope_key=scope_key)


@router.put("/{connector_name}", response_model=MappingResponse)
def save_mapping(
    connector_name: str,
    request: MappingUpdateRequest,
    service: MappingService = Depends(get_mapping_service),
):
    saved = service.save_mapping(
        connector_name,
        request.mapping,
        scope_type=request.scope_type,
        scope_key=request.scope_key,
        changed_by=request.changed_by,
    )
    return saved


@router.post("/{connector_name}/rollback/{version}", response_model=MappingResponse)
def rollback_mapping(
    connector_name: str,
    version: int,
    scope_type: str = Query("source"),
    scope_key: str | None = Query(None),
    changed_by: str = Query("system"),
    service: MappingService = Depends(get_mapping_service),
):
    try:
        return service.rollback(
            connector_name,
            version,
            scope_type=scope_type,
            scope_key=scope_key,
            changed_by=changed_by,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Mapping version '{version}' not found.")
