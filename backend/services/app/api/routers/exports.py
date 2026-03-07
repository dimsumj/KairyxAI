from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.schemas.exports import ExportJobCreateRequest
from app.api.schemas.jobs import build_job_response
from app.application.exports import ExportService
from app.core.deps import get_export_service


router = APIRouter(prefix="/exports", tags=["exports"])


@router.get("")
def list_export_jobs(service: ExportService = Depends(get_export_service)):
    return {"items": [build_job_response(job, base_path="/api/v1/exports") for job in service.list_jobs()]}


@router.post("", status_code=status.HTTP_201_CREATED)
def create_export_job(request: ExportJobCreateRequest, service: ExportService = Depends(get_export_service)):
    try:
        job = service.create_job(request.model_dump())
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Prediction job '{request.prediction_job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/exports")


@router.get("/{job_id}")
def get_export_job(job_id: str, service: ExportService = Depends(get_export_service)):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Export job '{job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/exports")


@router.post("/{job_id}/run")
def run_export_job(job_id: str, service: ExportService = Depends(get_export_service)):
    try:
        job = service.run_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Export job '{job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/exports")
