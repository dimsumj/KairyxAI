from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.schemas.onboarding import OrganizationSpaceOnboardingRequest
from app.application.projects import ProjectWorkspaceService
from app.core.deps import get_project_workspace_service, get_repository
from app.core.governance import get_governance_context, record_audit


router = APIRouter(prefix="/onboarding", tags=["onboarding"])


def _commit_workspace_mutation(repository) -> None:
    repository.session.commit()


@router.post("/organization-space", response_model=dict, status_code=status.HTTP_201_CREATED)
def create_organization_space(
    payload: OrganizationSpaceOnboardingRequest,
    request: Request,
    service: ProjectWorkspaceService = Depends(get_project_workspace_service),
    repository=Depends(get_repository),
):
    context = get_governance_context(request)
    current_orgs = service.list_accessible_organization_spaces(context.actor_id)
    if current_orgs and not context.platform_admin:
        raise HTTPException(status_code=403, detail="Self-serve organization-space onboarding is only available before you join an organization space.")
    actor = repository.get_platform_user(context.actor_id) or {}
    try:
        result = service.create_organization_space_and_first_project(
            organization_id=payload.organization_id,
            organization_name=payload.organization_name,
            project_id=payload.project_id,
            project_name=payload.project_name,
            project_description=payload.project_description,
            user_id=context.actor_id,
            email=actor.get("email"),
            display_name=actor.get("display_name"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    record_audit(
        repository,
        context,
        action_type="organization_space_onboarding_completed",
        resource_type="organization_space",
        resource_id=result["organization_space"]["tenant_id"],
        payload={
            "organization_space": result["organization_space"],
            "project": result["project"],
        },
    )
    _commit_workspace_mutation(repository)
    return result
