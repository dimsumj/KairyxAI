from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.api.schemas.experiments import (
    ExperimentConfigRequest,
    ExperimentDecisionRequest,
    ExperimentDecisionResponse,
    ExperimentEventPage,
    ExperimentLifecycleRequest,
    ExperimentOutcomeIngestRequest,
)
from app.application.experiments import ExperimentConfigService
from app.core.deps import get_experiment_service


router = APIRouter(prefix="/experiments", tags=["experiments"])


@router.get("/config")
def get_experiment_config(experiment_id: str = "churn_engagement_v1", service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"experiment": service.get_config(experiment_id)}


@router.put("/config")
def put_experiment_config(request: ExperimentConfigRequest, service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"experiment": service.save_config(request.model_dump(), experiment_id=request.experiment_id)}


@router.post("/config")
def post_experiment_config(request: ExperimentLifecycleRequest, experiment_id: str = "churn_engagement_v1", service: ExperimentConfigService = Depends(get_experiment_service)):
    payload = request.model_dump()
    payload["experiment_id"] = experiment_id
    return {"experiment": service.save_config(payload, experiment_id=experiment_id)}


@router.get("/summary")
def get_experiment_summary(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return service.get_summary(experiment_id)


@router.post("/{experiment_id}/start")
def start_experiment(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"experiment": service.start(experiment_id)}


@router.post("/{experiment_id}/stop")
def stop_experiment(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"experiment": service.stop(experiment_id)}


@router.get("/{experiment_id}/summary")
def get_named_experiment_summary(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return service.get_summary(experiment_id)


@router.get("/{experiment_id}/exposures", response_model=ExperimentEventPage)
def get_experiment_exposures(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"items": service.list_exposures(experiment_id)}


@router.get("/{experiment_id}/outcomes", response_model=ExperimentEventPage)
def get_experiment_outcomes(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"items": service.list_outcomes(experiment_id)}


@router.post("/{experiment_id}/outcomes:ingest")
def ingest_experiment_outcomes(
    experiment_id: str,
    request: ExperimentOutcomeIngestRequest,
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    return service.ingest_outcomes(experiment_id, [item.model_dump() for item in request.outcomes])


@router.post("/{experiment_id}/decision", response_model=ExperimentDecisionResponse)
def post_experiment_decision(
    experiment_id: str,
    request: ExperimentDecisionRequest,
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    try:
        return service.decide(experiment_id, decided_by=request.decided_by)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Experiment '{experiment_id}' not found.")
