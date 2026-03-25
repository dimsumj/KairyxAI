from __future__ import annotations

from fastapi import APIRouter, Request

from app.core.governance import get_governance_context
from app.core.settings import get_settings


router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/me", response_model=dict)
def get_authenticated_actor(request: Request):
    context = get_governance_context(request)
    return {
        "actor_id": context.actor_id,
        "actor_role": context.actor_role,
        "tenant_id": context.tenant_id,
        "correlation_id": context.correlation_id,
        "platform_admin": context.platform_admin,
        "auth_mode": context.auth_mode,
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
