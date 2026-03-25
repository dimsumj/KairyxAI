from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse

from app.api.schemas.jobs import build_job_response
from app.api.schemas.predictions import PredictionJobCreateRequest, PredictionModelTrainRequest, PredictionResultsPage
from app.application.predictions import PredictionService
from app.core.errors import MissingDependencyError, ResourceLockedError
from app.core.deps import get_prediction_service, get_settings_dependency
from app.core.governance import ensure_permission, get_governance_context


router = APIRouter(prefix="/predictions", tags=["predictions"])


@router.get("/models/latest", response_model=dict)
def get_latest_prediction_model(http_request: Request, service: PredictionService = Depends(get_prediction_service)):
    ensure_permission(get_governance_context(http_request), "predictions.models.read")
    payload = service.get_latest_model()
    if payload is None:
        raise HTTPException(status_code=404, detail="No churn model version has been trained yet.")
    return {"model": payload}


@router.get("/models/runs", response_model=dict)
def list_prediction_model_runs(http_request: Request, service: PredictionService = Depends(get_prediction_service)):
    ensure_permission(get_governance_context(http_request), "predictions.models.read")
    return {
        "items": service.list_model_versions(),
        "training_status": service.get_model_training_status(),
        "readiness": service.get_model_readiness(),
    }


@router.post("/models/train", response_model=dict)
def train_prediction_model(
    request: PredictionModelTrainRequest,
    http_request: Request,
    service: PredictionService = Depends(get_prediction_service),
):
    ensure_permission(get_governance_context(http_request), "predictions.models.train")
    return {"model": service.train_local_model(reference_time=request.reference_time, min_rows=request.min_rows)}


@router.post("/models/train/start", response_model=dict)
def start_prediction_model_training(
    request: PredictionModelTrainRequest,
    http_request: Request,
    service: PredictionService = Depends(get_prediction_service),
):
    ensure_permission(get_governance_context(http_request), "predictions.models.train")
    return service.start_local_model_training(reference_time=request.reference_time, min_rows=request.min_rows)


@router.post("/models/train/stop", response_model=dict)
def stop_prediction_model_training(http_request: Request, service: PredictionService = Depends(get_prediction_service)):
    ensure_permission(get_governance_context(http_request), "predictions.models.train")
    try:
        return service.stop_local_model_training()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


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
        job = service.create_job(
            import_job_id=request.import_job_id,
            source_name=request.source_name,
            audience_scope=request.audience_scope,
            prediction_mode=request.prediction_mode,
        )
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
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
    except MissingDependencyError as exc:
        raise HTTPException(status_code=404, detail=exc.detail)
    except ResourceLockedError as exc:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail=str(exc))
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Prediction job '{job_id}' not found.")
    except Exception as exc:
        service.rollback_session()
        failed_job = None
        try:
            failed_job = service.get_job(job_id)
        except Exception:
            service.rollback_session()
        payload = {"detail": str(exc)}
        if failed_job is not None:
            payload["job"] = build_job_response(
                failed_job,
                base_path="/api/v1/predictions",
                extra_links={"results": f"/api/v1/predictions/{failed_job['id']}/results"},
            ).model_dump(mode="json")
        return JSONResponse(status_code=500, content=payload)
    return build_job_response(job, base_path="/api/v1/predictions", extra_links={"results": f"/api/v1/predictions/{job['id']}/results"})


@router.post("/{job_id}/stop")
def stop_prediction_job(job_id: str, service: PredictionService = Depends(get_prediction_service)):
    try:
        job = service.stop_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Prediction job '{job_id}' not found.")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
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
