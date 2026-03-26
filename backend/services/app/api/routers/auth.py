from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from app.application.projects import ProjectWorkspaceService
from app.core.deps import get_project_workspace_service, get_repository
from app.core.governance import get_governance_context
from app.core.settings import get_settings


router = APIRouter(prefix="/auth", tags=["auth"])


def _organization_to_public(item: dict | None) -> dict | None:
    if not item:
        return None
    organization_id = str(item.get("tenant_id") or item.get("organization_id") or "").strip()
    if not organization_id:
        return None
    return {
        "organization_id": organization_id,
        "name": item.get("name"),
        "status": item.get("status"),
        "role": item.get("role"),
    }


def _project_to_public(item: dict | None) -> dict | None:
    if not item:
        return None
    project_id = str(item.get("project_id") or "").strip()
    if not project_id:
        return None
    return {
        "organization_id": str(item.get("tenant_id") or item.get("organization_id") or "").strip() or None,
        "project_id": project_id,
        "name": item.get("name"),
        "description": item.get("description"),
        "status": item.get("status"),
        "role": item.get("role"),
    }


@router.get("/me", response_model=dict)
def get_authenticated_actor(
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    accessible_tenants = service.list_accessible_organization_spaces(context.actor_id)
    accessible_projects = service.list_accessible_projects(context.tenant_id, context.actor_id) if context.tenant_id else []
    actor = repository.get_platform_user(context.actor_id) or {}
    current_organization = repository.get_tenant(context.tenant_id) if context.tenant_id else None
    current_project = repository.get_project(context.tenant_id, context.project_id) if context.tenant_id and context.project_id else None
    return {
        "email": actor.get("email"),
        "display_name": actor.get("display_name"),
        "organization_id": context.tenant_id,
        "project_id": context.project_id,
        "organization_role": context.org_role,
        "project_role": context.project_role,
        "correlation_id": context.correlation_id,
        "platform_admin": context.platform_admin,
        "auth_mode": context.auth_mode,
        "organization": _organization_to_public(
            {
                **(current_organization or {}),
                "tenant_id": context.tenant_id,
                "role": context.org_role,
            }
        ),
        "project": _project_to_public(
            {
                **(current_project or {}),
                "tenant_id": context.tenant_id,
                "project_id": context.project_id,
                "role": context.project_role,
            }
        ),
        "accessible_organizations": [_organization_to_public(item) for item in accessible_tenants],
        "accessible_projects": [_project_to_public(item) for item in accessible_projects],
        "needs_onboarding": len(accessible_tenants) == 0,
        "needs_org_selection": len(accessible_tenants) > 1 and not context.tenant_id,
        "needs_project_selection": bool(context.tenant_id) and len(accessible_projects) > 1 and not context.project_id,
    }


@router.get("/organization-space/{organization_id}", response_model=dict)
def inspect_organization_space_access(
    organization_id: str,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
):
    context = get_governance_context(request)
    try:
        payload = service.inspect_organization_space_access(organization_id, context.actor_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return {
        "organization_id": payload["organization_id"],
        "exists": payload["exists"],
        "accessible": payload["accessible"],
        "role": payload["role"],
        "membership_status": payload["membership_status"],
        "organization": _organization_to_public(payload["organization"]),
    }


@router.get("/oidc-config", response_model=dict)
def get_oidc_config():
    settings = get_settings()
    return {
        "provider": settings.oidc_provider or "oidc",
        "client_id": settings.oidc_client_id,
        "issuer": settings.oidc_issuer,
        "authorize_url": settings.oidc_authorize_url,
        "token_url": settings.oidc_token_url,
        "logout_url": settings.oidc_logout_url,
        "audience": settings.oidc_audience,
        "hosted_domain": settings.oidc_google_hosted_domain,
        "include_audience_parameter": settings.oidc_provider != "google",
    }
