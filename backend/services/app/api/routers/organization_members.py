from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.schemas.organization_members import (
    OrganizationInviteCreateRequest,
    OrganizationMemberCreateRequest,
    OrganizationMemberUpdateRequest,
)
from app.application.projects import ProjectWorkspaceService
from app.core.deps import get_project_workspace_service, get_repository
from app.core.governance import ensure_org_admin, get_governance_context, record_audit


router = APIRouter(tags=["organization-members"])


def _commit_workspace_mutation(repository) -> None:
    repository.session.commit()


@router.get("/organization-members", response_model=dict)
def list_organization_members(
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
):
    context = get_governance_context(request)
    if not context.tenant_id:
        raise HTTPException(status_code=409, detail="Organization space selection is required.")
    return {"items": service.list_organization_members(context.tenant_id, context.actor_id)}


@router.post("/organization-members", response_model=dict, status_code=status.HTTP_201_CREATED)
def add_organization_member(
    payload: OrganizationMemberCreateRequest,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    if not context.tenant_id:
        raise HTTPException(status_code=409, detail="Organization space selection is required.")
    ensure_org_admin(context)
    try:
        result = service.create_organization_member(
            context.tenant_id,
            email=payload.email,
            display_name=payload.display_name,
            role=payload.role,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Organization space '{context.tenant_id}' was not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    record_audit(
        repository,
        context,
        action_type="organization_member_added",
        resource_type="organization_member",
        resource_id=(result.get("member") or {}).get("user_id") or result["invite"]["invite_code"],
        payload=result,
    )
    _commit_workspace_mutation(repository)
    return result


@router.patch("/organization-members/{member_id}", response_model=dict)
def update_organization_member_role(
    member_id: int,
    payload: OrganizationMemberUpdateRequest,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    if not context.tenant_id:
        raise HTTPException(status_code=409, detail="Organization space selection is required.")
    ensure_org_admin(context)
    try:
        member = service.update_organization_member_role(
            context.tenant_id,
            member_id,
            role=payload.role,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Organization member '{member_id}' was not found.")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(
        repository,
        context,
        action_type="organization_member_role_updated",
        resource_type="organization_member",
        resource_id=str(member_id),
        payload=member,
    )
    _commit_workspace_mutation(repository)
    return {"member": member}


@router.post("/organization-invites", response_model=dict, status_code=status.HTTP_201_CREATED)
def create_organization_invite(
    payload: OrganizationInviteCreateRequest,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    if not context.tenant_id:
        raise HTTPException(status_code=409, detail="Organization space selection is required.")
    ensure_org_admin(context)
    try:
        invite = service.create_organization_invite(
            context.tenant_id,
            email=payload.email,
            display_name=payload.display_name,
            role=payload.role,
            expires_in_days=payload.expires_in_days,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Organization space '{context.tenant_id}' was not found.")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(
        repository,
        context,
        action_type="organization_invite_created",
        resource_type="organization_invite",
        resource_id=invite["invite_code"],
        payload=invite,
    )
    _commit_workspace_mutation(repository)
    return {"invite": invite}
