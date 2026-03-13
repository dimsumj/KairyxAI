from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.schemas.experiments import (
    ExperimentConfigRequest,
    ExperimentDecisionRequest,
    ExperimentDecisionResponse,
    ExperimentEventPage,
    ExperimentLifecycleRequest,
    ExperimentOptimizerRunRequest,
    ExperimentOutcomeIngestRequest,
)
from app.application.experiments import ExperimentConfigService
from app.core.governance import ensure_permission, get_governance_context
from app.core.deps import get_experiment_service


router = APIRouter(prefix="/experiments", tags=["experiments"])


@router.get("/config")
def get_experiment_config(experiment_id: str = "churn_engagement_v1", service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"experiment": service.get_config(experiment_id)}


@router.put("/config")
def put_experiment_config(request: ExperimentConfigRequest, http_request: Request, service: ExperimentConfigService = Depends(get_experiment_service)):
    ensure_permission(get_governance_context(http_request), "experiments.config.write")
    return {"experiment": service.save_config(request.model_dump(), experiment_id=request.experiment_id)}


@router.post("/config")
def post_experiment_config(request: ExperimentLifecycleRequest, http_request: Request, experiment_id: str = "churn_engagement_v1", service: ExperimentConfigService = Depends(get_experiment_service)):
    ensure_permission(get_governance_context(http_request), "experiments.config.write")
    payload = request.model_dump()
    payload["experiment_id"] = experiment_id
    return {"experiment": service.save_config(payload, experiment_id=experiment_id)}


@router.get("/summary")
def get_experiment_summary(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return service.get_summary(experiment_id)


@router.post("/{experiment_id}/start")
def start_experiment(experiment_id: str, http_request: Request, service: ExperimentConfigService = Depends(get_experiment_service)):
    ensure_permission(get_governance_context(http_request), "experiments.start")
    return {"experiment": service.start(experiment_id)}


@router.post("/{experiment_id}/stop")
def stop_experiment(experiment_id: str, http_request: Request, service: ExperimentConfigService = Depends(get_experiment_service)):
    ensure_permission(get_governance_context(http_request), "experiments.stop")
    return {"experiment": service.stop(experiment_id)}


@router.get("/{experiment_id}/summary")
def get_named_experiment_summary(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return service.get_summary(experiment_id)


@router.get("/{experiment_id}/versions", response_model=dict)
def get_experiment_versions(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return service.list_versions(experiment_id)


@router.get("/{experiment_id}/assignments", response_model=ExperimentEventPage)
def get_experiment_assignments(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"items": service.list_assignments(experiment_id)}


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
    http_request: Request,
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    ensure_permission(get_governance_context(http_request), "experiments.outcomes.ingest")
    return service.ingest_outcomes(experiment_id, [item.model_dump() for item in request.outcomes])


@router.post("/{experiment_id}/decision", response_model=ExperimentDecisionResponse)
def post_experiment_decision(
    experiment_id: str,
    request: ExperimentDecisionRequest,
    http_request: Request,
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    ensure_permission(get_governance_context(http_request), "experiments.decision")
    try:
        return service.decide(experiment_id, decided_by=request.decided_by)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Experiment '{experiment_id}' not found.")


@router.get("/{experiment_id}/rollout-suggestion", response_model=dict)
def get_experiment_rollout_suggestion(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return service.get_rollout_suggestion(experiment_id)


@router.get("/{experiment_id}/optimizer", response_model=dict)
def get_experiment_optimizer_state(
    experiment_id: str,
    http_request: Request,
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    ensure_permission(get_governance_context(http_request), "experiments.rollout.read")
    return service.get_optimizer_state(experiment_id)


@router.post("/{experiment_id}/optimizer/run", response_model=dict)
def run_experiment_optimizer(
    experiment_id: str,
    request: ExperimentOptimizerRunRequest,
    http_request: Request,
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    ensure_permission(get_governance_context(http_request), "experiments.optimizer.run")
    return service.run_optimizer(
        experiment_id,
        reference_time=request.reference_time,
        apply_changes=request.apply_changes,
    )
