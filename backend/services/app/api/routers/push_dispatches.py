from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.schemas.push_dispatches import PushDispatchResponse, PushDispatchSendRequest
from app.application.push_dispatches import PushDispatchService
from app.core.deps import get_push_dispatch_service
from app.core.governance import ensure_permission, get_governance_context


router = APIRouter(prefix="/push-dispatches", tags=["push-dispatches"])


@router.get("", response_model=dict)
def list_push_dispatches(
    request: Request,
    service: PushDispatchService = Depends(get_push_dispatch_service),
):
    ensure_permission(get_governance_context(request), "push_dispatches.read")
    return {"items": service.list_dispatches()}


@router.get("/{push_dispatch_id}", response_model=PushDispatchResponse)
def get_push_dispatch(
    push_dispatch_id: str,
    request: Request,
    service: PushDispatchService = Depends(get_push_dispatch_service),
):
    ensure_permission(get_governance_context(request), "push_dispatches.read")
    dispatch = service.get_dispatch(push_dispatch_id)
    if dispatch is None:
        raise HTTPException(status_code=404, detail=f"Push dispatch '{push_dispatch_id}' not found.")
    return dispatch


@router.post("/send-now", response_model=PushDispatchResponse, status_code=status.HTTP_201_CREATED)
def send_push_dispatch_now(
    payload: PushDispatchSendRequest,
    request: Request,
    service: PushDispatchService = Depends(get_push_dispatch_service),
):
    ensure_permission(get_governance_context(request), "push_dispatches.execute")
    try:
        return service.send_now(payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
