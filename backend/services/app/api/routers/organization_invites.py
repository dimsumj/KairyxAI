from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.schemas.organization_members import OrganizationInviteRedeemRequest
from app.application.projects import ProjectWorkspaceService
from app.core.deps import get_project_workspace_service, get_repository
from app.core.governance import get_governance_context, record_audit


router = APIRouter(tags=["organization-invites"])


def _commit_workspace_mutation(repository) -> None:
    repository.session.commit()


@router.post("/organization-invites/redeem", response_model=dict)
def redeem_organization_invite(
    payload: OrganizationInviteRedeemRequest,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    actor = repository.get_platform_user(context.actor_id) or {}
    try:
        result = service.redeem_organization_invite(
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
        action_type="organization_invite_redeemed",
        resource_type="organization_invite",
        resource_id=payload.invite_code,
        payload=result,
    )
    _commit_workspace_mutation(repository)
    return result
