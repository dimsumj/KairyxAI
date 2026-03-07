from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from app.api.schemas.jobs import build_job_response
from app.application.imports import ImportService
from app.core.deps import get_import_service


class ImportJobCreateRequest(BaseModel):
    source_name: str
    start_date: str
    end_date: str
    page_size: int | None = None


router = APIRouter(prefix="/imports", tags=["imports"])


@router.get("")
def list_imports(service: ImportService = Depends(get_import_service)):
    jobs = [
        build_job_response(
            job,
            base_path="/api/v1/imports",
            extra_links={"checkpoints": f"/api/v1/imports/{job['id']}/checkpoints"},
        )
        for job in service.list_jobs()
    ]
    return {"items": jobs}


@router.post("", status_code=status.HTTP_201_CREATED)
def create_import(request: ImportJobCreateRequest, service: ImportService = Depends(get_import_service)):
    try:
        job = service.create_job(request.source_name, request.start_date, request.end_date, request.page_size)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Connector '{request.source_name}' not found.")
    return build_job_response(job, base_path="/api/v1/imports", extra_links={"checkpoints": f"/api/v1/imports/{job['id']}/checkpoints"})


@router.get("/{job_id}")
def get_import(job_id: str, service: ImportService = Depends(get_import_service)):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/imports", extra_links={"checkpoints": f"/api/v1/imports/{job['id']}/checkpoints"})


@router.post("/{job_id}/run")
def run_import(job_id: str, service: ImportService = Depends(get_import_service)):
    try:
        job = service.run_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/imports", extra_links={"checkpoints": f"/api/v1/imports/{job['id']}/checkpoints"})


@router.get("/{job_id}/checkpoints")
def get_import_checkpoints(job_id: str, service: ImportService = Depends(get_import_service)):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Import job '{job_id}' not found.")
    return {"items": service.repository.list_checkpoints(job_id)}
