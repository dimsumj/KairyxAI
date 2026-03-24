from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class PredictionJobCreateRequest(BaseModel):
    import_job_id: str
    prediction_mode: str = "local"


class PredictionModelTrainRequest(BaseModel):
    reference_time: str | None = None
    min_rows: int = 12


class PredictionResultItem(BaseModel):
    user_id: str
    churn_state: str
    predicted_churn_risk: str
    churn_reason: str
    prediction_source: str
    suggested_action: str
    days_since_last_seen: int | str | None = None
    ltv: float | str | None = None
    session_count: int | str | None = None
    event_count: int | str | None = None
    baseline_churn_score: float | str | None = None
    model_version: str | None = None
    score_timestamp: str | None = None
    effective_local_model_version: str | None = None
    effective_local_model_state: str | None = None
    eligibility_reason: str | None = None
    recommended_template_id: str | None = None
    recommended_variant: str | None = None
    policy_snapshot_id: str | None = None
    top_signals: List[Dict[str, Any]] = Field(default_factory=list)


class PredictionResultsPage(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[PredictionResultItem]
