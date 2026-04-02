from __future__ import annotations

import os

from fastapi import FastAPI, Request

from app.application.control_loop import ControlLoopService
from app.core.db import init_db, session_scope
from app.core.request_context import RequestContext, request_context
from app.core.settings import get_settings, validate_runtime_settings
from app.core.worker_auth import require_worker_auth
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository
from workers.queue_runtime import start_queue_poller, stop_queue_poller


app = FastAPI(title="KairyxAI Scheduler Worker")


@app.get("/health/live")
def health_live() -> dict:
    return {"status": "ok", "service": "scheduler-worker"}


@app.on_event("startup")
def _startup() -> None:
    validate_runtime_settings(get_settings())
    start_queue_poller(app, service_role="scheduler-worker", handler=_handle_queue_payload)


@app.on_event("shutdown")
def _shutdown() -> None:
    stop_queue_poller(app)


def _run_scheduler(reference_time: str | None = None) -> dict:
    init_db()
    tenant_id = str(os.getenv("BOOTSTRAP_TENANT_ID", "default"))
    project_id = str(os.getenv("BOOTSTRAP_PROJECT_ID", "default"))
    with request_context(
        RequestContext(
            actor_id="worker:scheduler",
            actor_role="admin",
            tenant_id=tenant_id,
            project_id=project_id,
            correlation_id="worker-scheduler",
            platform_admin=True,
            org_role="owner",
            project_role="admin",
            auth_mode="worker",
        )
    ):
        with session_scope() as session:
            repository = SqlAlchemyControlPlaneRepository(session)
            repository.ensure_tenant(tenant_id, os.getenv("BOOTSTRAP_TENANT_NAME", "Default Tenant"))
            repository.ensure_project(tenant_id, project_id, os.getenv("BOOTSTRAP_PROJECT_NAME", "Default Project"))
            result = ControlLoopService(repository, get_settings()).tick(reference_time=reference_time)
    return {"status": "ok", "result": result}


def _handle_queue_payload(payload: dict, _attributes: dict | None = None) -> None:
    reference_time = payload.get("reference_time")
    _run_scheduler(str(reference_time) if reference_time else None)


@app.post("/run")
async def run_scheduler(request: Request) -> dict:
    require_worker_auth(request)
    body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    reference_time = body.get("reference_time") if isinstance(body, dict) else None
    return _run_scheduler(str(reference_time) if reference_time else None)
