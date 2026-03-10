from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.api.schemas.exports import ExportJobCreateRequest
from app.api.schemas.jobs import build_job_response
from app.application.exports import ExportService
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_export_service


router = APIRouter(prefix="/exports", tags=["exports"])


@router.get("")
def list_export_jobs(service: ExportService = Depends(get_export_service)):
    return {"items": [build_job_response(job, base_path="/api/v1/exports") for job in service.list_jobs()]}


@router.post("", status_code=status.HTTP_201_CREATED)
def create_export_job(request: ExportJobCreateRequest, http_request: Request, service: ExportService = Depends(get_export_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "exports.create")
    try:
        job = service.create_job(request.model_dump())
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Prediction job '{request.prediction_job_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="exports_create",
        resource_type="export_job",
        resource_id=job["id"],
        payload=build_job_response(job, base_path="/api/v1/exports").model_dump(mode="json"),
    )


@router.get("/{job_id}")
def get_export_job(job_id: str, service: ExportService = Depends(get_export_service)):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Export job '{job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/exports")


@router.post("/{job_id}/run")
def run_export_job(job_id: str, http_request: Request, service: ExportService = Depends(get_export_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "exports.run")
    try:
        job = service.run_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Export job '{job_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="exports_run",
        resource_type="export_job",
        resource_id=job_id,
        payload=build_job_response(job, base_path="/api/v1/exports").model_dump(mode="json"),
    )


@router.post("/{job_id}/retry")
def retry_export_job(job_id: str, http_request: Request, service: ExportService = Depends(get_export_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "exports.retry")
    try:
        job = service.retry_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Export job '{job_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="exports_retry",
        resource_type="export_job",
        resource_id=job_id,
        payload=build_job_response(job, base_path="/api/v1/exports").model_dump(mode="json"),
    )


@router.get("/{job_id}/diagnostics")
def get_export_diagnostics(job_id: str, http_request: Request, service: ExportService = Depends(get_export_service)):
    context = get_governance_context(http_request)
    ensure_permission(context, "exports.diagnostics.read")
    try:
        payload = {"items": service.list_diagnostics(job_id)}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Export job '{job_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="exports_diagnostics_read",
        resource_type="export_job",
        resource_id=job_id,
        payload=payload,
    )
