from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.schemas.experiments import ExperimentConfigRequest
from app.application.experiments import ExperimentConfigService
from app.core.deps import get_experiment_service


router = APIRouter(prefix="/experiments", tags=["experiments"])


@router.get("/config")
def get_experiment_config(service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"experiment": service.get_config()}


@router.put("/config")
def put_experiment_config(request: ExperimentConfigRequest, service: ExperimentConfigService = Depends(get_experiment_service)):
    return {"experiment": service.save_config(request.model_dump())}


@router.get("/summary")
def get_experiment_summary(experiment_id: str, service: ExperimentConfigService = Depends(get_experiment_service)):
    return service.get_summary(experiment_id)
