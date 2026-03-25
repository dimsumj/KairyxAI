from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request

from app.application.audit import AuditService
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_audit_service


router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/actions", response_model=dict)
def list_audit_actions(
    http_request: Request,
    limit: int = Query(100, ge=1, le=500),
    action_type: str | None = Query(None),
    resource_type: str | None = Query(None),
    resource_id: str | None = Query(None),
    actor_role: str | None = Query(None),
    tenant_id: str | None = Query(None),
    project_id: str | None = Query(None),
    high_risk_only: bool = Query(False),
    service: AuditService = Depends(get_audit_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "audit.logs.read")
    payload = service.list_actions(
        limit=limit,
        action_type=action_type,
        resource_type=resource_type,
        resource_id=resource_id,
        actor_role=actor_role,
        tenant_id=tenant_id,
        project_id=project_id,
        high_risk_only=high_risk_only,
        include_all_tenants=context.platform_admin,
    )
    return build_audited_response(
        service.repository,
        context,
        action_type="audit_logs_read",
        resource_type="action_history",
        resource_id=resource_id,
        payload=payload,
    )
