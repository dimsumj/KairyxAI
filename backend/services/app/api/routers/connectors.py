from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status

from app.api.schemas.connectors import ConnectorCreateRequest, ConnectorHealthResponse, ConnectorResponse
from app.application.connectors import ConnectorService
from app.core.deps import get_connector_service


router = APIRouter(prefix="/connectors", tags=["connectors"])


@router.get("", response_model=list[ConnectorResponse])
def list_connectors(service: ConnectorService = Depends(get_connector_service)):
    return service.list_connectors()


@router.post("", response_model=ConnectorResponse, status_code=status.HTTP_201_CREATED)
def create_connector(request: ConnectorCreateRequest, service: ConnectorService = Depends(get_connector_service)):
    return service.create_connector(request.name, request.type, request.config)


@router.get("/{connector_name}/health", response_model=ConnectorHealthResponse)
def connector_health(connector_name: str, service: ConnectorService = Depends(get_connector_service)):
    try:
        return service.health_check(connector_name)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")


@router.delete("/{connector_name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_connector(connector_name: str, service: ConnectorService = Depends(get_connector_service)):
    deleted = service.delete_connector(connector_name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
