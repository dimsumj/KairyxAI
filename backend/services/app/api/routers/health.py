from __future__ import annotations

from pydantic import BaseModel
from fastapi import APIRouter, Depends
from fastapi import Request

from app.application.control_loop import ControlLoopService
from app.application.health_monitor import HealthMonitorService
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.settings import get_settings
from app.core.deps import get_control_loop_service, get_health_monitor_service
from bigquery_service import get_shared_bigquery_service


router = APIRouter(tags=["health"])

class SchedulerTickRequest(BaseModel):
    reference_time: str | None = None


@router.get("/health")
def health(service: HealthMonitorService = Depends(get_health_monitor_service)):
    settings = get_settings()
    bigquery_service = get_shared_bigquery_service()
    mock_state_backend = bigquery_service.get_mock_state_backend()
    snapshot = service.snapshot(persist=True)
    payload = {
        "status": "ok",
        "service": settings.app_name,
        "mode": settings.data_backend_mode,
        "mock_state_backend": mock_state_backend,
        "mock_state_persistent": bigquery_service.is_mock_state_persistent(),
        "data_aliases": bigquery_service.get_v1_table_aliases(),
        **snapshot,
    }
    if settings.data_backend_mode == "mock":
        payload["local_cache"] = bigquery_service.get_local_cache_stats()
    return payload


@router.get("/health/modules")
def health_modules(service: HealthMonitorService = Depends(get_health_monitor_service)):
    return {"items": service.list_modules()}


@router.get("/health/alerts")
def health_alerts(include_resolved: bool = False, module: str | None = None, service: HealthMonitorService = Depends(get_health_monitor_service)):
    return {"items": service.list_alerts(include_resolved=include_resolved, module=module)}


@router.get("/health/scheduler", response_model=dict)
def health_scheduler(http_request: Request, service: ControlLoopService = Depends(get_control_loop_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "health.scheduler.read")
    return build_audited_response(
        service.repository,
        context,
        action_type="scheduler_jobs_read",
        resource_type="scheduler_job",
        resource_id=None,
        payload={"items": service.list_jobs()},
    )


@router.post("/health/scheduler/tick", response_model=dict)
def run_scheduler_tick(
    request: SchedulerTickRequest,
    http_request: Request,
    service: ControlLoopService = Depends(get_control_loop_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "health.scheduler.tick")
    return build_audited_response(
        service.repository,
        context,
        action_type="scheduler_tick",
        resource_type="scheduler_job",
        resource_id=None,
        payload=service.tick(reference_time=request.reference_time),
    )
