from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.api.schemas.provider_connections import ProviderConnectionCreateRequest, ProviderConnectionResponse, ProviderConnectionUpdateRequest
from app.application.braze_provider import BrazeApiError, BrazeProviderService
from app.application.provider_connections import ProviderConnectionService
from app.application.sendgrid_provider import SendGridApiError, SendGridProviderService
from app.core.deps import get_braze_provider_service, get_repository, get_sendgrid_provider_service
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


@router.get("/{provider_connection_id}/sendgrid/templates", response_model=dict)
def list_sendgrid_templates(
    provider_connection_id: str,
    request: Request,
    service: SendGridProviderService = Depends(get_sendgrid_provider_service),
):
    ensure_permission(get_governance_context(request), "provider_connections.read")
    try:
        return {"items": service.list_dynamic_templates(provider_connection_id)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Provider connection '{provider_connection_id}' not found.")
    except SendGridApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/{provider_connection_id}/messaging-assets", response_model=dict)
def list_provider_messaging_assets(
    provider_connection_id: str,
    request: Request,
    service: ProviderConnectionService = Depends(get_provider_connection_service),
    sendgrid_service: SendGridProviderService = Depends(get_sendgrid_provider_service),
    braze_service: BrazeProviderService = Depends(get_braze_provider_service),
):
    ensure_permission(get_governance_context(request), "provider_connections.read")
    connection = service.get_connection(provider_connection_id)
    if connection is None:
        raise HTTPException(status_code=404, detail=f"Provider connection '{provider_connection_id}' not found.")
    provider = str(connection.get("provider") or "").strip().lower()
    try:
        if provider == "sendgrid":
            return {
                "provider": provider,
                "items": sendgrid_service.list_dynamic_templates(provider_connection_id),
            }
        if provider == "braze":
            return {
                "provider": provider,
                "items": braze_service.list_api_campaigns(provider_connection_id),
            }
    except SendGridApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except BrazeApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    raise HTTPException(status_code=409, detail=f"Provider '{provider}' does not support campaign messaging assets.")


@router.delete("/{provider_connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_provider_connection(provider_connection_id: str, request: Request, service: ProviderConnectionService = Depends(get_provider_connection_service)):
    ensure_permission(get_governance_context(request), "provider_connections.write")
    try:
        deleted = service.delete_connection(provider_connection_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Provider connection '{provider_connection_id}' not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
