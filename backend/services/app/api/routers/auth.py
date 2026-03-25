from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from app.application.projects import ProjectWorkspaceService
from app.core.deps import get_project_workspace_service, get_repository
from app.core.governance import get_governance_context
from app.core.settings import get_settings


router = APIRouter(prefix="/auth", tags=["auth"])


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
    return {
        "actor_id": context.actor_id,
        "actor_role": context.actor_role,
        "tenant_id": context.tenant_id,
        "project_id": context.project_id,
        "org_role": context.org_role,
        "project_role": context.project_role,
        "correlation_id": context.correlation_id,
        "platform_admin": context.platform_admin,
        "auth_mode": context.auth_mode,
        "email": actor.get("email"),
        "display_name": actor.get("display_name"),
        "accessible_tenants": accessible_tenants,
        "accessible_projects": accessible_projects,
        "needs_onboarding": len(accessible_tenants) == 0,
        "needs_org_selection": len(accessible_tenants) > 1 and not context.tenant_id,
        "needs_project_selection": bool(context.tenant_id) and len(accessible_projects) > 1 and not context.project_id,
    }


@router.get("/oidc-config", response_model=dict)
def get_oidc_config():
    settings = get_settings()
    return {
        "client_id": settings.oidc_client_id,
        "issuer": settings.oidc_issuer,
        "authorize_url": settings.oidc_authorize_url,
        "token_url": settings.oidc_token_url,
        "logout_url": settings.oidc_logout_url,
        "audience": settings.oidc_audience,
    }
