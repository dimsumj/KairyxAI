from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.schemas.mappings import MappingResponse, MappingUpdateRequest
from app.application.mappings import MappingService
from app.core.deps import get_mapping_service


router = APIRouter(prefix="/mappings", tags=["mappings"])


@router.get("/{connector_name}", response_model=MappingResponse)
def get_mapping(connector_name: str, service: MappingService = Depends(get_mapping_service)):
    return {"connector_name": connector_name, "mapping": service.get_mapping(connector_name)}


@router.put("/{connector_name}", response_model=MappingResponse)
def save_mapping(
    connector_name: str,
    request: MappingUpdateRequest,
    service: MappingService = Depends(get_mapping_service),
):
    saved = service.save_mapping(connector_name, request.mapping)
    return {"connector_name": saved["connector_name"], "mapping": saved["mapping"]}
