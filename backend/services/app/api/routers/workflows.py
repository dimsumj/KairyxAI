from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.schemas.workflows import OrchestratorRunRequest, WorkflowCreateRequest, WorkflowExecutionPage, WorkflowResponse, WorkflowRunRequest
from app.application.workflows import WorkflowService
from app.core.deps import get_workflow_service


router = APIRouter(tags=["workflows"])
workflow_router = APIRouter(prefix="/workflows", tags=["workflows"])
orchestrator_router = APIRouter(prefix="/orchestrator", tags=["orchestrator"])


@workflow_router.get("", response_model=dict)
def list_workflows(service: WorkflowService = Depends(get_workflow_service)):
    return {"items": service.list_workflows()}


@workflow_router.post("", response_model=WorkflowResponse, status_code=status.HTTP_201_CREATED)
def create_workflow(request: WorkflowCreateRequest, service: WorkflowService = Depends(get_workflow_service)):
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


@workflow_router.post("/{workflow_id}/publish", response_model=WorkflowResponse)
def publish_workflow(workflow_id: str, service: WorkflowService = Depends(get_workflow_service)):
    try:
        return service.publish_workflow(workflow_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@workflow_router.post("/{workflow_id}/pause", response_model=WorkflowResponse)
def pause_workflow(workflow_id: str, service: WorkflowService = Depends(get_workflow_service)):
    try:
        return service.pause_workflow(workflow_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")


@workflow_router.post("/{workflow_id}/resume", response_model=WorkflowResponse)
def resume_workflow(workflow_id: str, service: WorkflowService = Depends(get_workflow_service)):
    try:
        return service.resume_workflow(workflow_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Workflow '{workflow_id}' not found.")


@workflow_router.post("/{workflow_id}/test-run", response_model=dict)
def test_run_workflow(
    workflow_id: str,
    request: WorkflowRunRequest,
    service: WorkflowService = Depends(get_workflow_service),
):
    try:
        return service.test_run(
            workflow_id,
            limit=request.limit,
            confirm=request.confirm,
            sandbox=request.sandbox,
            reference_time=request.reference_time,
        )
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


@orchestrator_router.post("/kill-switch/on", response_model=dict)
def enable_kill_switch(service: WorkflowService = Depends(get_workflow_service)):
    return service.set_kill_switch(True)


@orchestrator_router.post("/kill-switch/off", response_model=dict)
def disable_kill_switch(service: WorkflowService = Depends(get_workflow_service)):
    return service.set_kill_switch(False)


@orchestrator_router.post("/run-due", response_model=dict)
def run_due_workflows(
    request: OrchestratorRunRequest,
    service: WorkflowService = Depends(get_workflow_service),
):
    try:
        return service.run_due_workflows(reference_time=request.reference_time, limit_per_workflow=request.limit_per_workflow)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
