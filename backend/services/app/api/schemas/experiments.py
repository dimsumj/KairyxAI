from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class ExperimentConfigRequest(BaseModel):
    experiment_id: str = "churn_engagement_v1"
    enabled: bool = True
    holdout_pct: float = 0.10
    b_variant_pct: float = 0.0
    blacklist_user_ids: List[str] = Field(default_factory=list)
    rollout_policy: str = "conservative"
    multiple_comparisons_method: str = "none"
    scenario_type: str = "churn_rescue"
    optimization_mode: str = "fixed_ab"
    holdout_floor_pct: float = 0.10
    max_daily_shift_pct: float = 0.10
    approved_variants: List[Dict[str, Any]] = Field(default_factory=list)
    eligibility_threshold_steps: List[float] = Field(default_factory=lambda: [0.85, 0.75, 0.65, 0.55])


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
    b_variant_pct: float = 0.0
    blacklist_user_ids: List[str] = Field(default_factory=list)
    rollout_policy: str = "conservative"
    multiple_comparisons_method: str = "none"
    scenario_type: str = "churn_rescue"
    optimization_mode: str = "fixed_ab"
    holdout_floor_pct: float = 0.10
    max_daily_shift_pct: float = 0.10
    approved_variants: List[Dict[str, Any]] = Field(default_factory=list)
    eligibility_threshold_steps: List[float] = Field(default_factory=lambda: [0.85, 0.75, 0.65, 0.55])


class ExperimentEventPage(BaseModel):
    items: List[Dict[str, Any]]


class ExperimentOutcomeEvent(BaseModel):
    workflow_id: str
    cohort_id: str
    experiment_id: str
    user_id: str
    occurred_at: str
    action_execution_id: str | None = None
    delivery_id: str | None = None
    provider_callback_id: str | None = None
    group: str = "treatment"
    outcome_name: str = "returned"
    product_outcome_type: str | None = None
    attribution_window_days: int = 7
    exposure_id: str | None = None
    variant_id: str | None = None
    template_id: str | None = None
    source: str = "internal_writeback"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ExperimentOutcomeIngestRequest(BaseModel):
    outcomes: List[ExperimentOutcomeEvent] = Field(default_factory=list)


class ExperimentDecisionRequest(BaseModel):
    decided_by: str = "system"


class ExperimentDecisionResponse(BaseModel):
    experiment_id: str
    summary: Dict[str, Any]
    next_step: str
    decision_reason: str | None = None


class ExperimentOptimizerRunRequest(BaseModel):
    reference_time: str | None = None
    apply_changes: bool = True
