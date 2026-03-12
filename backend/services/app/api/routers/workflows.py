from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.schemas.workflows import (
    OrchestratorRunRequest,
    WorkflowConfirmationRequest,
    WorkflowCreateRequest,
    WorkflowEventIngestRequest,
    WorkflowExecutionPage,
    WorkflowResponse,
    WorkflowRunRequest,
    WorkflowThresholdEvaluateRequest,
    WorkflowUpdateRequest,
)
from app.application.workflows import WorkflowService
from app.core.errors import MissingDependencyError, ResourceLockedError
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_workflow_service


router = APIRouter(tags=["workflows"])
workflow_router = APIRouter(prefix="/workflows", tags=["workflows"])
orchestrator_router = APIRouter(prefix="/orchestrator", tags=["orchestrator"])


@workflow_router.get("", response_model=dict)
def list_workflows(service: WorkflowService = Depends(get_workflow_service)):
    return {"items": service.list_workflows()}


@workflow_router.post("", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
def create_workflow(request: WorkflowCreateRequest, http_request: Request, service: WorkflowService = Depends(get_workflow_service)):
    ensure_permission(get_governance_context(http_request), "workflows.create")
    try:
        return service.create_workflow(
            name=request.name,
            cohort_id=request.cohort_id,
            schedule=request.schedule,
            action=request.action,
            policy=request.policy,
            budget_policy=request.budget_policy,
            trigger=request.trigger,
            channel_config=request.channel_config,
            experiment_id=request.experiment_id,
            requires_confirmation=request.requires_confirmation,
            steps=request.steps,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Cohort '{request.cohort_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@workflow_router.get("/{workflow_id}", response_model=WorkflowResponse)
def get_workflow(workflow_id: str, service: WorkflowService = Depends(get_workflow_service)):
    workflow = service.get_workflow(workflow_id)
    if workflow is None:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")
    return workflow


@workflow_router.put("/{workflow_id}", response_model=WorkflowResponse)
def update_workflow(workflow_id: str, request: WorkflowUpdateRequest, http_request: Request, service: WorkflowService = Depends(get_workflow_service)):
    ensure_permission(get_governance_context(http_request), "workflows.update")
    try:
        return service.update_workflow(workflow_id, request.model_dump(exclude_none=True))
    except KeyError as exc:
        detail = f"Workflow '{workflow_id}' not found." if str(exc) == workflow_id else f"Cohort '{exc.args[0]}' not found."
        raise HTTPException(status_code=404, detail=detail)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@workflow_router.post("/{workflow_id}/publish", response_model=WorkflowResponse)
def publish_workflow(workflow_id: str, http_request: Request, service: WorkflowService = Depends(get_workflow_service)):
    ensure_permission(get_governance_context(http_request), "workflows.publish")
    try:
        return service.publish_workflow(workflow_id)
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@workflow_router.post("/{workflow_id}/pause", response_model=WorkflowResponse)
def pause_workflow(workflow_id: str, http_request: Request, service: WorkflowService = Depends(get_workflow_service)):
    ensure_permission(get_governance_context(http_request), "workflows.pause")
    try:
        return service.pause_workflow(workflow_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")


@workflow_router.post("/{workflow_id}/resume", response_model=WorkflowResponse)
def resume_workflow(workflow_id: str, http_request: Request, service: WorkflowService = Depends(get_workflow_service)):
    ensure_permission(get_governance_context(http_request), "workflows.resume")
    try:
        return service.resume_workflow(workflow_id)
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@workflow_router.post("/{workflow_id}/test-run", response_model=dict)
def test_run_workflow(
    workflow_id: str,
    request: WorkflowRunRequest,
    http_request: Request,
    service: WorkflowService = Depends(get_workflow_service),
):
    ensure_permission(get_governance_context(http_request), "workflows.execute")
    try:
        return service.test_run(
            workflow_id,
            limit=request.limit,
            confirm=request.confirm,
            sandbox=request.sandbox,
            reference_time=request.reference_time,
            confirmation_token=request.confirmation_token,
        )
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@workflow_router.get("/{workflow_id}/executions", response_model=WorkflowExecutionPage)
def list_workflow_executions(workflow_id: str, service: WorkflowService = Depends(get_workflow_service)):
    try:
        return {"items": service.list_executions(workflow_id)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")


@workflow_router.get("/{workflow_id}/policy-counters", response_model=dict)
def get_workflow_policy_counters(workflow_id: str, service: WorkflowService = Depends(get_workflow_service)):
    try:
        return service.get_policy_counters(workflow_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")


@workflow_router.get("/{workflow_id}/versions", response_model=dict)
def get_workflow_versions(workflow_id: str, service: WorkflowService = Depends(get_workflow_service)):
    try:
        return service.list_versions(workflow_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")


@workflow_router.post("/{workflow_id}/confirm", response_model=dict)
def confirm_workflow(
    workflow_id: str,
    request: WorkflowConfirmationRequest,
    http_request: Request,
    service: WorkflowService = Depends(get_workflow_service),
):
    ensure_permission(get_governance_context(http_request), "workflows.confirm")
    try:
        return service.confirm_workflow(workflow_id, note=request.note, valid_for_hours=request.valid_for_hours)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")


@workflow_router.get("/{workflow_id}/deliveries", response_model=dict)
def list_workflow_deliveries(
    workflow_id: str,
    request: Request,
    service: WorkflowService = Depends(get_workflow_service),
):
    context = get_governance_context(request)
    ensure_permission(context, "workflows.deliveries.read")
    try:
        payload = {"items": service.list_deliveries(workflow_id)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="workflow_deliveries_read",
        resource_type="workflow",
        resource_id=workflow_id,
        payload=payload,
    )


@orchestrator_router.post("/kill-switch/on", response_model=dict)
def enable_kill_switch(http_request: Request, service: WorkflowService = Depends(get_workflow_service)):
    ensure_permission(get_governance_context(http_request), "orchestrator.kill_switch")
    return service.set_kill_switch(True)


@orchestrator_router.post("/kill-switch/off", response_model=dict)
def disable_kill_switch(http_request: Request, service: WorkflowService = Depends(get_workflow_service)):
    ensure_permission(get_governance_context(http_request), "orchestrator.kill_switch")
    return service.set_kill_switch(False)


@orchestrator_router.post("/run-due", response_model=dict)
def run_due_workflows(
    request: OrchestratorRunRequest,
    http_request: Request,
    service: WorkflowService = Depends(get_workflow_service),
):
    ensure_permission(get_governance_context(http_request), "workflows.execute")
    try:
        return service.run_due_workflows(
            reference_time=request.reference_time,
            limit_per_workflow=request.limit_per_workflow,
            confirmation_tokens=request.confirmation_tokens,
        )
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@orchestrator_router.post("/events:ingest", response_model=dict)
def ingest_orchestrator_event(
    request: WorkflowEventIngestRequest,
    http_request: Request,
    service: WorkflowService = Depends(get_workflow_service),
):
    ensure_permission(get_governance_context(http_request), "orchestrator.events.ingest")
    try:
        return service.ingest_event(
            event_type=request.event_type,
            user_ids=request.user_ids,
            payload=request.payload,
            reference_time=request.reference_time,
            confirmation_tokens=request.confirmation_tokens,
        )
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@orchestrator_router.post("/thresholds:evaluate", response_model=dict)
def evaluate_orchestrator_threshold(
    request: WorkflowThresholdEvaluateRequest,
    http_request: Request,
    service: WorkflowService = Depends(get_workflow_service),
):
    ensure_permission(get_governance_context(http_request), "orchestrator.thresholds.evaluate")
    try:
        return service.evaluate_thresholds(
            metric_id=request.metric_id,
            value=request.value,
            reference_time=request.reference_time,
            confirmation_tokens=request.confirmation_tokens,
        )
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
