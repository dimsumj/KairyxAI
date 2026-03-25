from __future__ import annotations

import os

from fastapi import FastAPI, Request

from app.application.control_loop import ControlLoopService
from app.core.db import init_db, session_scope
from app.core.request_context import RequestContext, request_context
from app.core.settings import get_settings
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository


app = FastAPI(title="KairyxAI Scheduler Worker")


@app.get("/health/live")
def health_live() -> dict:
    return {"status": "ok", "service": "scheduler-worker"}


@app.post("/run")
async def run_scheduler(request: Request) -> dict:
    body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    reference_time = body.get("reference_time") if isinstance(body, dict) else None

    init_db()
    tenant_id = str(os.getenv("BOOTSTRAP_TENANT_ID", "default"))
    with request_context(
        RequestContext(
            actor_id="worker:scheduler",
            actor_role="admin",
            tenant_id=tenant_id,
            correlation_id="worker-scheduler",
            platform_admin=True,
            auth_mode="worker",
        )
    ):
        with session_scope() as session:
            repository = SqlAlchemyControlPlaneRepository(session)
            repository.ensure_tenant(tenant_id, os.getenv("BOOTSTRAP_TENANT_NAME", "Default Tenant"))
            result = ControlLoopService(repository, get_settings()).tick(reference_time=reference_time)
    return {"status": "ok", "result": result}
