from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class ExperimentConfigRequest(BaseModel):
    experiment_id: str = "churn_engagement_v1"
    enabled: bool = True
    holdout_pct: float = 0.10
    b_variant_pct: float = 0.50


class ExperimentConfigResponse(BaseModel):
    experiment: Dict


class ExperimentLifecycleRequest(BaseModel):
    enabled: bool | None = None
    primary_metric: str = "return_rate"
    guardrail_metrics: List[str] = Field(default_factory=lambda: ["engagement_rate", "policy_block_rate"])
    min_sample_size: int = 20
    min_runtime_hours: int = 24
    cohort_id: str | None = None
    holdout_pct: float = 0.10
    b_variant_pct: float = 0.50


class ExperimentEventPage(BaseModel):
    items: List[Dict[str, Any]]


class ExperimentDecisionRequest(BaseModel):
    decided_by: str = "system"


class ExperimentDecisionResponse(BaseModel):
    experiment_id: str
    summary: Dict[str, Any]
    next_step: str
