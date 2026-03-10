from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.schemas.activation import ActivationCallbackIngestRequest
from app.application.workflows import WorkflowService
from app.core.deps import get_workflow_service
from app.core.governance import build_audited_response, ensure_permission, get_governance_context


router = APIRouter(prefix="/activation", tags=["activation"])


@router.post("/callbacks/{provider}")
def ingest_provider_callbacks(
    provider: str,
    request: Request,
    payload: ActivationCallbackIngestRequest,
    service: WorkflowService = Depends(get_workflow_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "activation.callbacks.ingest")
    try:
        result = service.ingest_delivery_callback(provider, [item.model_dump() for item in payload.callbacks])
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="activation_callbacks_ingest",
        resource_type="provider_callback",
        resource_id=provider,
        payload=result,
    )
