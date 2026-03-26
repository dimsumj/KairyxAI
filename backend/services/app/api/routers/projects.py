from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.schemas.projects import (
    ProjectCreateRequest,
    ProjectInviteCreateRequest,
    ProjectInviteRedeemRequest,
)
from app.application.projects import ProjectWorkspaceService
from app.core.deps import get_project_workspace_service, get_repository
from app.core.governance import ensure_org_admin, get_governance_context, record_audit


router = APIRouter(tags=["projects"])


def _commit_workspace_mutation(repository) -> None:
    repository.session.commit()


@router.get("/projects", response_model=dict)
def list_projects(
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
):
    context = get_governance_context(request)
    if not context.tenant_id:
        raise HTTPException(status_code=409, detail="Organization space selection is required.")
    return {"items": service.list_accessible_projects(context.tenant_id, context.actor_id)}


@router.post("/projects", response_model=dict, status_code=status.HTTP_201_CREATED)
def create_project(
    payload: ProjectCreateRequest,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    if not context.tenant_id:
        raise HTTPException(status_code=409, detail="Organization space selection is required.")
    ensure_org_admin(context)
    try:
        project = service.create_project(
            context.tenant_id,
            project_id=payload.project_id,
            name=payload.name,
            description=payload.description,
            user_id=context.actor_id,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Organization space '{context.tenant_id}' was not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    record_audit(
        repository,
        context,
        action_type="project_created",
        resource_type="project",
        resource_id=project["project_id"],
        payload=project,
    )
    _commit_workspace_mutation(repository)
    return {"project": project}


@router.post("/projects/{project_id}/invites", response_model=dict, status_code=status.HTTP_201_CREATED)
def create_project_invite(
    project_id: str,
    payload: ProjectInviteCreateRequest,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    if not context.tenant_id:
        raise HTTPException(status_code=409, detail="Organization space selection is required.")
    ensure_org_admin(context)
    try:
        invite = service.create_project_invite(
            context.tenant_id,
            project_id,
            email=payload.email,
            display_name=payload.display_name,
            org_role=payload.org_role,
            project_role=payload.project_role,
            expires_in_days=payload.expires_in_days,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Project '{project_id}' was not found in organization space '{context.tenant_id}'.")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(
        repository,
        context,
        action_type="project_invite_created",
        resource_type="project_invite",
        resource_id=invite["invite_code"],
        payload=invite,
    )
    _commit_workspace_mutation(repository)
    return {"invite": invite}


@router.post("/project-invites/redeem", response_model=dict)
def redeem_project_invite(
    payload: ProjectInviteRedeemRequest,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    actor = repository.get_platform_user(context.actor_id) or {}
    try:
        result = service.redeem_project_invite(
            payload.invite_code,
            user_id=context.actor_id,
            email=actor.get("email"),
            display_name=actor.get("display_name"),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Invite was not found.")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    record_audit(
        repository,
        context,
        action_type="project_invite_redeemed",
        resource_type="project_invite",
        resource_id=payload.invite_code,
        payload=result,
    )
    _commit_workspace_mutation(repository)
    return result
