from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.schemas.jobs import build_job_response
from app.api.schemas.predictions import PredictionJobCreateRequest, PredictionResultsPage
from app.application.predictions import PredictionService
from app.core.deps import get_prediction_service, get_settings_dependency


router = APIRouter(prefix="/predictions", tags=["predictions"])


@router.get("")
def list_prediction_jobs(service: PredictionService = Depends(get_prediction_service)):
    return {
        "items": [
            build_job_response(
                job,
                base_path="/api/v1/predictions",
                extra_links={"results": f"/api/v1/predictions/{job['id']}/results"},
            )
            for job in service.list_jobs()
        ]
    }


@router.post("", status_code=status.HTTP_201_CREATED)
def create_prediction_job(request: PredictionJobCreateRequest, service: PredictionService = Depends(get_prediction_service)):
    try:
        job = service.create_job(request.import_job_id, request.prediction_mode)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Import job '{request.import_job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/predictions", extra_links={"results": f"/api/v1/predictions/{job['id']}/results"})


@router.get("/{job_id}")
def get_prediction_job(job_id: str, service: PredictionService = Depends(get_prediction_service)):
    job = service.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Prediction job '{job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/predictions", extra_links={"results": f"/api/v1/predictions/{job['id']}/results"})


@router.post("/{job_id}/run")
def run_prediction_job(job_id: str, service: PredictionService = Depends(get_prediction_service)):
    try:
        job = service.run_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Prediction job '{job_id}' not found.")
    return build_job_response(job, base_path="/api/v1/predictions", extra_links={"results": f"/api/v1/predictions/{job['id']}/results"})


@router.get("/{job_id}/results", response_model=PredictionResultsPage)
def list_prediction_results(
    job_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    service: PredictionService = Depends(get_prediction_service),
):
    try:
        payload = service.list_results(job_id, page, page_size)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Prediction job '{job_id}' not found.")
    return payload
