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


class AIEvaluationRequest(BaseModel):
    evaluation_type: str
    target_type: str
    target_id: str | None = None
    outcome: str = "neutral"
    score: float | None = Field(default=None, ge=0, le=1)
    dimensions: Dict[str, Any] = Field(default_factory=dict)
    citation_ids: List[str] = Field(default_factory=list)
    artifact_ids: List[str] = Field(default_factory=list)
    prompt_summary: str | None = None
    response_summary: str | None = None
    comments: str | None = None
    source: str = "operator"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    evaluated_by: str | None = None


class AIEvaluationAutoGradeRequest(BaseModel):
    target_type: str
    target_id: str | None = None
    prompt: str | None = None
    response: str | None = None
    citations: List[Dict[str, Any]] = Field(default_factory=list)
    artifacts: List[Dict[str, Any]] = Field(default_factory=list)
    expected_artifact_type: str | None = None
    generated_title: str | None = None
    generated_body: str | None = None
    source: str = "auto_grader"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AIEvaluationJudgeRunItem(BaseModel):
    evaluation_type: str
    target_type: str
    target_id: str | None = None
    prompt: str | None = None
    response: str | None = None
    prompt_summary: str | None = None
    response_summary: str | None = None
    citations: List[Dict[str, Any]] = Field(default_factory=list)
    artifacts: List[Dict[str, Any]] = Field(default_factory=list)
    citation_ids: List[str] = Field(default_factory=list)
    artifact_ids: List[str] = Field(default_factory=list)
    expected_artifact_type: str | None = None
    generated_title: str | None = None
    generated_body: str | None = None
    outcome: str | None = None
    score: float | None = Field(default=None, ge=0, le=1)
    dimensions: Dict[str, Any] = Field(default_factory=dict)
    comments: str | None = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AIEvaluationJudgeRunRequest(BaseModel):
    run_type: str = "model_judge"
    run_label: str | None = None
    model_profile_id: str | None = None
    rubric: Dict[str, Any] = Field(default_factory=dict)
    items: List[AIEvaluationJudgeRunItem] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AIEvaluationResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    evaluation_id: str
    evaluation_type: str
    target_type: str
    target_id: str = ""
    outcome: str = "neutral"
    score: float | None = None
    score_source: str = "none"
    dimensions: Dict[str, float] = Field(default_factory=dict)
    citation_ids: List[str] = Field(default_factory=list)
    artifact_ids: List[str] = Field(default_factory=list)
    prompt_summary: str = ""
    response_summary: str = ""
    comments: str = ""
    source: str = "operator"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    evaluated_by: str = "system"
    status: str = "recorded"
    recorded_at: str = ""
    export: Dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None
    created_by: str = "system"
    updated_by: str = "system"


class AIEvaluationListResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    items: List[AIEvaluationResponse] = Field(default_factory=list)


class AIEvaluationSummaryResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    total_records: int = 0
    average_score: float | None = None
    positive_rate: float = 0.0
    negative_rate: float = 0.0
    edited_rate: float = 0.0
    outcome_counts: Dict[str, int] = Field(default_factory=dict)
    evaluation_type_counts: Dict[str, int] = Field(default_factory=dict)
    target_type_counts: Dict[str, int] = Field(default_factory=dict)
    dimension_averages: Dict[str, float] = Field(default_factory=dict)
    latest_recorded_at: str = ""


class AIQualityMonitorAlert(BaseModel):
    severity: str
    code: str
    title: str
    detail: str = ""


class AIQualityDimensionCard(BaseModel):
    dimension: str
    label: str
    average: float | None = None
    status: str = "missing"


class AIQualityMonitorRecord(BaseModel):
    evaluation_id: str
    evaluation_type: str
    target_type: str
    target_id: str = ""
    outcome: str = "neutral"
    score: float | None = None
    source: str = ""
    evaluated_by: str = ""
    recorded_at: str = ""
    export: Dict[str, Any] = Field(default_factory=dict)


class AIQualityMonitorResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    format: str = "ai_quality_monitor.v1"
    status: str = "healthy"
    generated_at: str = ""
    scope: Dict[str, Any] = Field(default_factory=dict)
    summary: AIEvaluationSummaryResponse | Dict[str, Any] = Field(default_factory=dict)
    feedback_summary: Dict[str, Any] = Field(default_factory=dict)
    feedback_learning: Dict[str, Any] = Field(default_factory=dict)
    alerts: List[AIQualityMonitorAlert] = Field(default_factory=list)
    alert_count: int = 0
    dimension_cards: List[AIQualityDimensionCard] = Field(default_factory=list)
    coverage_gaps: List[str] = Field(default_factory=list)
    judge_readiness: Dict[str, Any] = Field(default_factory=dict)
    latest_alert_check: Dict[str, Any] = Field(default_factory=dict)
    recent_records: List[AIQualityMonitorRecord] = Field(default_factory=list)
    export: Dict[str, Any] = Field(default_factory=dict)


class AIQualityMonitorExportResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    format: str = "ai_quality_monitor.v1"
    monitor: AIQualityMonitorResponse | Dict[str, Any]


class AIQualityAlertCheckExportResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    format: str = "ai_quality_alert_check.v1"
    check: Dict[str, Any] = Field(default_factory=dict)


class AIEvaluationExportResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    format: str
    evaluation: AIEvaluationResponse


class AIEvaluationAutoGradeResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    grading_id: str
    target_type: str
    target_id: str = ""
    evaluations: List[AIEvaluationResponse] = Field(default_factory=list)
    summary: Dict[str, Any] = Field(default_factory=dict)
    export: Dict[str, Any] = Field(default_factory=dict)


class AIEvaluationJudgeRunResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    run_id: str
    run_type: str
    run_label: str = ""
    status: str = "recorded"
    model_selection: Dict[str, Any] = Field(default_factory=dict)
    evaluations: List[AIEvaluationResponse] = Field(default_factory=list)
    summary: Dict[str, Any] = Field(default_factory=dict)
    export: Dict[str, Any] = Field(default_factory=dict)


class AIFeedbackRequest(BaseModel):
    feedback_type: str
    target_type: str
    target_id: str | None = None
    sentiment: str | None = None
    weight: float | None = Field(default=None, ge=-1, le=1)
    rating: float | None = Field(default=None, ge=0, le=1)
    citation_ids: List[str] = Field(default_factory=list)
    artifact_ids: List[str] = Field(default_factory=list)
    related_evaluation_id: str | None = None
    change_summary: str | None = None
    outcome_metrics: Dict[str, Any] = Field(default_factory=dict)
    comments: str | None = None
    source: str = "operator"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    recorded_by: str | None = None


class AIFeedbackResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    feedback_id: str
    feedback_type: str
    target_type: str
    target_id: str = ""
    sentiment: str = "neutral"
    weight: float = 0.0
    rating: float | None = None
    citation_ids: List[str] = Field(default_factory=list)
    artifact_ids: List[str] = Field(default_factory=list)
    related_evaluation_id: str = ""
    change_summary: str = ""
    outcome_metrics: Dict[str, float] = Field(default_factory=dict)
    comments: str = ""
    source: str = "operator"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    recorded_by: str = "system"
    status: str = "recorded"
    recorded_at: str = ""
    export: Dict[str, Any] = Field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None
    created_by: str = "system"
    updated_by: str = "system"


class AIFeedbackListResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    items: List[AIFeedbackResponse] = Field(default_factory=list)


class AIFeedbackSummaryResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    total_records: int = 0
    positive_rate: float = 0.0
    negative_rate: float = 0.0
    sentiment_counts: Dict[str, int] = Field(default_factory=dict)
    feedback_type_counts: Dict[str, int] = Field(default_factory=dict)
    target_counts: Dict[str, int] = Field(default_factory=dict)
    target_weight_scores: Dict[str, float] = Field(default_factory=dict)
    metric_averages: Dict[str, float] = Field(default_factory=dict)
    latest_recorded_at: str = ""


class AIFeedbackExportResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    format: str
    feedback: AIFeedbackResponse


class AIFeedbackLearningResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    correlation_id: str = ""
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
    profile_id: str
    target_type: str = ""
    summary: Dict[str, Any] = Field(default_factory=dict)
    top_positive_targets: List[Dict[str, Any]] = Field(default_factory=list)
    top_negative_targets: List[Dict[str, Any]] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)
    prompt_context: str = ""
    export: Dict[str, Any] = Field(default_factory=dict)
