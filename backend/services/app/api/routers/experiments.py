from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.schemas.experiments import (
    AIEvaluationExportResponse,
    AIEvaluationListResponse,
    AIEvaluationRequest,
    AIEvaluationResponse,
    AIEvaluationSummaryResponse,
    ExperimentConfigRequest,
    ExperimentDecisionRequest,
    ExperimentDecisionResponse,
    ExperimentEventPage,
    ExperimentLifecycleRequest,
    ExperimentOptimizerRunRequest,
    ExperimentOutcomeIngestRequest,
)
from app.application.ai_evaluations import AIEvaluationService
from app.application.experiments import ExperimentConfigService
from app.core.governance import build_audited_response, ensure_permission, get_governance_context
from app.core.deps import get_ai_evaluation_service, get_experiment_service


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


@router.get("/ai-evaluations", response_model=AIEvaluationListResponse)
def list_ai_evaluations(
    http_request: Request,
    evaluation_type: str | None = None,
    target_type: str | None = None,
    target_id: str | None = None,
    limit: int = 100,
    service: AIEvaluationService = Depends(get_ai_evaluation_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "experiments.evaluations.read")
    payload = {
        "items": service.list_evaluations(
            evaluation_type=evaluation_type,
            target_type=target_type,
            target_id=target_id,
            limit=limit,
        )
    }
    return build_audited_response(
        service.repository,
        context,
        action_type="ai_evaluations_read",
        resource_type="ai_evaluation_record",
        resource_id=None,
        payload=payload,
    )


@router.post("/ai-evaluations", response_model=AIEvaluationResponse, status_code=201)
def record_ai_evaluation(
    request: AIEvaluationRequest,
    http_request: Request,
    service: AIEvaluationService = Depends(get_ai_evaluation_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "experiments.evaluations.write")
    try:
        payload = service.record_evaluation(**request.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return build_audited_response(
        service.repository,
        context,
        action_type="ai_evaluation_recorded",
        resource_type="ai_evaluation_record",
        resource_id=payload["evaluation_id"],
        payload=payload,
    )


@router.get("/ai-evaluations/summary", response_model=AIEvaluationSummaryResponse)
def summarize_ai_evaluations(
    http_request: Request,
    evaluation_type: str | None = None,
    target_type: str | None = None,
    service: AIEvaluationService = Depends(get_ai_evaluation_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "experiments.evaluations.read")
    payload = service.summarize(evaluation_type=evaluation_type, target_type=target_type)
    return build_audited_response(
        service.repository,
        context,
        action_type="ai_evaluations_summarized",
        resource_type="ai_evaluation_record",
        resource_id=None,
        payload=payload,
    )


@router.get("/ai-evaluations/{evaluation_id}", response_model=AIEvaluationResponse)
def get_ai_evaluation(
    evaluation_id: str,
    http_request: Request,
    service: AIEvaluationService = Depends(get_ai_evaluation_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "experiments.evaluations.read")
    payload = service.get_evaluation(evaluation_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"AI evaluation '{evaluation_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="ai_evaluation_read",
        resource_type="ai_evaluation_record",
        resource_id=evaluation_id,
        payload=payload,
    )


@router.get("/ai-evaluations/{evaluation_id}/export", response_model=AIEvaluationExportResponse)
def export_ai_evaluation(
    evaluation_id: str,
    http_request: Request,
    service: AIEvaluationService = Depends(get_ai_evaluation_service),
):
    context = get_governance_context(http_request)
    ensure_permission(context, "experiments.evaluations.read")
    payload = service.export_evaluation(evaluation_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"AI evaluation '{evaluation_id}' not found.")
    return build_audited_response(
        service.repository,
        context,
        action_type="ai_evaluation_exported",
        resource_type="ai_evaluation_record",
        resource_id=evaluation_id,
        payload=payload,
    )


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


@router.get("/{experiment_id}/integrity", response_model=dict)
def get_experiment_integrity(
    experiment_id: str,
    http_request: Request,
    service: ExperimentConfigService = Depends(get_experiment_service),
):
    ensure_permission(get_governance_context(http_request), "experiments.integrity.read")
    return service.get_measurement_integrity(experiment_id)


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
