from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse

from app.api.schemas.mcp_connections import (
    McpConnectionAuthStartResponse,
    McpConnectionCreateRequest,
    McpConnectionResponse,
    McpConnectionUpdateRequest,
    McpConnectionValidateRequest,
    McpConnectionValidateResponse,
    McpResultSnapshotCreateCohortRequest,
    McpResultSnapshotImportRequest,
    McpResultSnapshotResponse,
)
from app.application.mcp_connections import McpConnectionService
from app.core.deps import get_mcp_connection_service
from app.core.governance import build_audited_response, ensure_permission, get_governance_context


router = APIRouter(prefix="/mcp-connections", tags=["mcp-connections"])


@router.get("", response_model=dict)
def list_mcp_connections(
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.read")
    payload = {"items": service.list_connections(actor_id=context.actor_id)}
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_connection_list",
        resource_type="mcp_connection",
        resource_id=None,
        payload=payload,
    )


@router.post("", response_model=McpConnectionResponse, status_code=status.HTTP_201_CREATED)
def create_mcp_connection(
    payload: McpConnectionCreateRequest,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.write")
    try:
        created = service.create_connection(name=payload.name, preset_key=payload.preset_key, endpoint_url=payload.endpoint_url)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_connection_create",
        resource_type="mcp_connection",
        resource_id=created["mcp_connection_id"],
        payload=created,
    )


@router.post("/validate", response_model=McpConnectionValidateResponse)
def validate_mcp_connection(
    payload: McpConnectionValidateRequest,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.write")
    try:
        validated = service.validate_endpoint(payload.endpoint_url, preset_key=payload.preset_key)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_connection_validate",
        resource_type="mcp_connection",
        resource_id=None,
        payload=validated,
    )


@router.get("/snapshots", response_model=dict)
def list_mcp_result_snapshots(
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.read")
    payload = {"items": service.list_snapshots()}
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_snapshot_list",
        resource_type="mcp_result_snapshot",
        resource_id=None,
        payload=payload,
    )


@router.get("/{mcp_connection_id}", response_model=McpConnectionResponse)
def get_mcp_connection(
    mcp_connection_id: str,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.read")
    payload = service.get_connection(mcp_connection_id, actor_id=context.actor_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"MCP connection '{mcp_connection_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_connection_read",
        resource_type="mcp_connection",
        resource_id=mcp_connection_id,
        payload=payload,
    )


@router.patch("/{mcp_connection_id}", response_model=McpConnectionResponse)
def update_mcp_connection(
    mcp_connection_id: str,
    payload: McpConnectionUpdateRequest,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.write")
    try:
        updated = service.update_connection(mcp_connection_id, payload.model_dump(exclude_none=True), actor_id=context.actor_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"MCP connection '{mcp_connection_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_connection_update",
        resource_type="mcp_connection",
        resource_id=mcp_connection_id,
        payload=updated,
    )


@router.delete("/{mcp_connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_mcp_connection(
    mcp_connection_id: str,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.write")
    deleted = service.delete_connection(mcp_connection_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"MCP connection '{mcp_connection_id}' not found.")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{mcp_connection_id}/connect/start", response_model=McpConnectionAuthStartResponse)
def start_mcp_connection_oauth(
    mcp_connection_id: str,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.connect")
    callback_url = str(request.url_for("complete_mcp_connection_oauth"))
    try:
        payload = service.start_oauth_connection(
            mcp_connection_id,
            actor_id=context.actor_id,
            callback_url=callback_url,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"MCP connection '{mcp_connection_id}' not found.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_connection_connect_start",
        resource_type="mcp_connection",
        resource_id=mcp_connection_id,
        payload=payload,
    )


@router.get("/connect/callback", response_class=HTMLResponse, name="complete_mcp_connection_oauth")
def complete_mcp_connection_oauth(
    state: str,
    code: str | None = None,
    error: str | None = None,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    payload = service.complete_oauth_connection(state=state, code=code, error=error)
    return HTMLResponse(service.oauth_callback_html(payload))


@router.post("/{mcp_connection_id}/disconnect", response_model=McpConnectionResponse)
def disconnect_mcp_connection(
    mcp_connection_id: str,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.connect")
    try:
        payload = service.disconnect(mcp_connection_id, actor_id=context.actor_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"MCP connection '{mcp_connection_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_connection_disconnect",
        resource_type="mcp_connection",
        resource_id=mcp_connection_id,
        payload=payload,
    )


@router.post("/{mcp_connection_id}/refresh-tools", response_model=McpConnectionResponse)
def refresh_mcp_connection_tools(
    mcp_connection_id: str,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.read")
    try:
        payload = service.refresh_tools(mcp_connection_id, actor_id=context.actor_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"MCP connection '{mcp_connection_id}' not found.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_connection_refresh_tools",
        resource_type="mcp_connection",
        resource_id=mcp_connection_id,
        payload=payload,
    )


@router.post("/{mcp_connection_id}/snapshots", response_model=McpResultSnapshotResponse, status_code=status.HTTP_201_CREATED)
def import_mcp_result_snapshot(
    mcp_connection_id: str,
    payload: McpResultSnapshotImportRequest,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "mcp_connections.write")
    try:
        snapshot = service.import_snapshot(
            mcp_connection_id,
            name=payload.name,
            query_result=payload.query_result.model_dump(),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"MCP connection '{mcp_connection_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_snapshot_import",
        resource_type="mcp_result_snapshot",
        resource_id=snapshot["snapshot_id"],
        payload=snapshot,
    )


@router.post("/snapshots/{snapshot_id}/cohorts", response_model=dict, status_code=status.HTTP_201_CREATED)
def create_cohort_from_mcp_snapshot(
    snapshot_id: str,
    payload: McpResultSnapshotCreateCohortRequest,
    request: Request,
    service: McpConnectionService = Depends(get_mcp_connection_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "cohorts.create")
    try:
        cohort = service.create_cohort_from_snapshot(
            snapshot_id,
            name=payload.name,
            description=payload.description,
            tags=payload.tags,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"MCP snapshot '{snapshot_id}' not found.")
    except HTTPException:
        raise
    return build_audited_response(
        service.repository,
        context,
        action_type="mcp_snapshot_create_cohort",
        resource_type="cohort",
        resource_id=cohort["cohort_id"],
        payload={"cohort": cohort, "snapshot_id": snapshot_id},
    )
