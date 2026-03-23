from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.schemas.tenants import TenantCreateRequest, TenantMembershipRequest
from app.application.tenants import TenantService
from app.core.deps import get_repository
from app.core.governance import ensure_platform_admin, get_governance_context


router = APIRouter(prefix="/tenants", tags=["tenants"])


def get_tenant_service(repository=Depends(get_repository)) -> TenantService:
    return TenantService(repository)


@router.get("", response_model=dict)
def list_tenants(request: Request, service: TenantService = Depends(get_tenant_service)):
    ensure_platform_admin(get_governance_context(request))
    return {"items": service.list_tenants()}


@router.post("", response_model=dict, status_code=status.HTTP_201_CREATED)
def create_tenant(payload: TenantCreateRequest, request: Request, service: TenantService = Depends(get_tenant_service)):
    ensure_platform_admin(get_governance_context(request))
    return {"tenant": service.create_tenant(payload.tenant_id, payload.name, status=payload.status)}


@router.get("/{tenant_id}/memberships", response_model=dict)
def list_tenant_memberships(tenant_id: str, request: Request, service: TenantService = Depends(get_tenant_service)):
    ensure_platform_admin(get_governance_context(request))
    return service.list_memberships(tenant_id)


@router.put("/{tenant_id}/memberships/{user_id}", response_model=dict)
def put_tenant_membership(
    tenant_id: str,
    user_id: str,
    payload: TenantMembershipRequest,
    request: Request,
    service: TenantService = Depends(get_tenant_service),
):
    ensure_platform_admin(get_governance_context(request))
    if payload.user_id != user_id:
        raise HTTPException(status_code=409, detail="Path user_id must match payload user_id.")
    return service.upsert_membership(
        tenant_id,
        user_id,
        role=payload.role,
        status=payload.status,
        email=payload.email,
        display_name=payload.display_name,
    )
