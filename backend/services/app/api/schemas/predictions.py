from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field, model_validator


class PredictionJobCreateRequest(BaseModel):
    import_job_id: str | None = None
    source_name: str | None = None
    audience_scope: str | None = None
    prediction_mode: str = "local"

    @model_validator(mode="after")
    def validate_target(self) -> "PredictionJobCreateRequest":
        resolved_scope = str(self.audience_scope or ("source" if self.source_name else "import")).strip().lower()
        if resolved_scope not in {"import", "source"}:
            raise ValueError("audience_scope must be 'import' or 'source'.")
        if self.import_job_id and self.source_name:
            raise ValueError("Provide either import_job_id or source_name, not both.")
        if resolved_scope == "import" and not str(self.import_job_id or "").strip():
            raise ValueError("import_job_id is required when audience_scope is 'import'.")
        if resolved_scope == "source" and not str(self.source_name or "").strip():
            raise ValueError("source_name is required when audience_scope is 'source'.")
        return self


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
