from __future__ import annotations

import base64
import json
import os

from fastapi import FastAPI, HTTPException, Request

from app.application.predictions import PredictionService
from app.core.db import init_db, session_scope
from app.core.request_context import RequestContext, request_context
from app.core.settings import get_settings, validate_runtime_settings
from app.core.worker_auth import require_worker_auth
from app.infrastructure.repositories.sqlalchemy_control_plane import SqlAlchemyControlPlaneRepository


app = FastAPI(title="KairyxAI Prediction Worker")


def _decode_pubsub_envelope(envelope: dict) -> dict:
    message = dict(envelope.get("message") or {})
    raw_data = str(message.get("data") or "").strip()
    if not raw_data:
        return {}
    decoded = base64.b64decode(raw_data)
    return json.loads(decoded.decode("utf-8"))


@app.get("/health/live")
def health_live() -> dict:
    return {"status": "ok", "service": "prediction-worker"}


@app.on_event("startup")
def _startup() -> None:
    validate_runtime_settings(get_settings())


@app.post("/pubsub/push")
async def handle_pubsub_push(request: Request) -> dict:
    require_worker_auth(request)
    envelope = await request.json()
    payload = _decode_pubsub_envelope(envelope)
    job_id = str(payload.get("job_id") or "").strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="Pub/Sub payload is missing job_id.")

    init_db()
    with session_scope() as session:
        repository = SqlAlchemyControlPlaneRepository(session)
        job = repository.get_prediction_job(job_id, include_all_tenants=True)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Prediction job '{job_id}' not found.")

    tenant_id = str(job.get("tenant_id") or os.getenv("BOOTSTRAP_TENANT_ID", "default"))
    project_id = str(job.get("project_id") or os.getenv("BOOTSTRAP_PROJECT_ID", "default"))
    with request_context(
        RequestContext(
            actor_id="worker:prediction",
            actor_role="admin",
            tenant_id=tenant_id,
            project_id=project_id,
            correlation_id=f"worker-prediction-{job_id}",
            platform_admin=True,
            org_role="owner",
            project_role="admin",
            auth_mode="worker",
        )
    ):
        with session_scope() as session:
            repository = SqlAlchemyControlPlaneRepository(session)
            result = PredictionService(repository, get_settings()).run_job(job_id)
    return {"status": "ok", "job_id": job_id, "result": result}
