from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.api.schemas.provider_connections import ProviderConnectionCreateRequest, ProviderConnectionResponse, ProviderConnectionUpdateRequest
from app.application.provider_connections import ProviderConnectionService
from app.core.deps import get_repository
from app.core.governance import ensure_permission, get_governance_context


router = APIRouter(prefix="/provider-connections", tags=["provider-connections"])


def get_provider_connection_service(repository=Depends(get_repository)) -> ProviderConnectionService:
    return ProviderConnectionService(repository)


@router.get("", response_model=dict)
def list_provider_connections(request: Request, service: ProviderConnectionService = Depends(get_provider_connection_service)):
    ensure_permission(get_governance_context(request), "provider_connections.read")
    return {"items": service.list_connections()}


@router.post("", response_model=ProviderConnectionResponse, status_code=status.HTTP_201_CREATED)
def create_provider_connection(
    payload: ProviderConnectionCreateRequest,
    request: Request,
    service: ProviderConnectionService = Depends(get_provider_connection_service),
):
    ensure_permission(get_governance_context(request), "provider_connections.write")
    try:
        return service.create_connection(payload.name, payload.provider, payload.config)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/{provider_connection_id}", response_model=ProviderConnectionResponse)
def get_provider_connection(provider_connection_id: str, request: Request, service: ProviderConnectionService = Depends(get_provider_connection_service)):
    ensure_permission(get_governance_context(request), "provider_connections.read")
    record = service.get_connection(provider_connection_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Provider connection '{provider_connection_id}' not found.")
    return record


@router.patch("/{provider_connection_id}", response_model=ProviderConnectionResponse)
def update_provider_connection(
    provider_connection_id: str,
    payload: ProviderConnectionUpdateRequest,
    request: Request,
    service: ProviderConnectionService = Depends(get_provider_connection_service),
):
    ensure_permission(get_governance_context(request), "provider_connections.write")
    try:
        return service.update_connection(provider_connection_id, payload.model_dump(exclude_none=True))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Provider connection '{provider_connection_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.delete("/{provider_connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_provider_connection(provider_connection_id: str, request: Request, service: ProviderConnectionService = Depends(get_provider_connection_service)):
    ensure_permission(get_governance_context(request), "provider_connections.write")
    deleted = service.delete_connection(provider_connection_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Provider connection '{provider_connection_id}' not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
