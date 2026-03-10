from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from app.application.templates import ScenarioTemplateService
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_template_service


class TemplateInstantiateRequest(BaseModel):
    name_prefix: str | None = None
    owner: str = "system"
    activate_cohort: bool = True
    publish_workflow: bool = False


router = APIRouter(prefix="/templates", tags=["templates"])


@router.get("", response_model=dict)
def list_templates(http_request: Request, service: ScenarioTemplateService = Depends(get_template_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "templates.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="templates_read",
        resource_type="scenario_template",
        resource_id=None,
        payload=service.list_templates(),
    )


@router.get("/{template_id}", response_model=dict)
def get_template(template_id: str, http_request: Request, service: ScenarioTemplateService = Depends(get_template_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "templates.read")
    payload = service.get_template(template_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Template '{template_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="template_read",
        resource_type="scenario_template",
        resource_id=template_id,
        payload=payload,
    )


@router.post("/{template_id}/instantiate", response_model=dict, status_code=status.HTTP_201_CREATED)
def instantiate_template(
    template_id: str,
    request: TemplateInstantiateRequest,
    http_request: Request,
    service: ScenarioTemplateService = Depends(get_template_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "templates.instantiate")
    try:
        payload = service.instantiate(
            template_id,
            name_prefix=request.name_prefix,
            owner=request.owner,
            activate_cohort=request.activate_cohort,
            publish_workflow=request.publish_workflow,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Template '{template_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="template_instantiated",
        resource_type="scenario_template_instance",
        resource_id=payload.get("instance_id"),
        payload=payload,
    )
