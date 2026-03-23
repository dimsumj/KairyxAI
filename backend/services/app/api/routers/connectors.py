from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.api.schemas.connectors import ConnectorCreateRequest, ConnectorHealthResponse, ConnectorResponse
from app.application.connectors import ConnectorService
from app.core.errors import ResourceLockedError
from app.core.governance import ensure_permission, get_governance_context
from app.core.deps import get_connector_service


router = APIRouter(prefix="/connectors", tags=["connectors"])


@router.get("", response_model=list[ConnectorResponse])
def list_connectors(service: ConnectorService = Depends(get_connector_service)):
    return service.list_connectors()


@router.post("", response_model=ConnectorResponse, status_code=status.HTTP_201_CREATED)
def create_connector(request: ConnectorCreateRequest, http_request: Request, service: ConnectorService = Depends(get_connector_service)):
    ensure_permission(get_governance_context(http_request), "connectors.write")
    try:
        return service.create_connector(request.name, request.type, request.config, connector_id=request.connector_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/{connector_name}/health", response_model=ConnectorHealthResponse)
def connector_health(connector_name: str, service: ConnectorService = Depends(get_connector_service)):
    try:
        return service.health_check(connector_name)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")


@router.delete("/{connector_name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_connector(connector_name: str, http_request: Request, service: ConnectorService = Depends(get_connector_service)):
    ensure_permission(get_governance_context(http_request), "connectors.write")
    try:
        deleted = service.delete_connector(connector_name)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
