from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class CopilotQueryRequest(BaseModel):
    question: str
    time_window: str | None = None
    filters: Dict[str, Any] = Field(default_factory=dict)


class CopilotExplainRequest(BaseModel):
    metric_id: str
    time_window: str = "7d"
    dimensions: List[str] = Field(default_factory=lambda: ["platform", "country", "campaign"])


class CopilotRecommendRequest(BaseModel):
    insight: Dict[str, Any] = Field(default_factory=dict)
    metric_context: Dict[str, Any] = Field(default_factory=dict)


class CopilotReportRequest(BaseModel):
    report_type: str = "daily"
    time_window: str = "7d"


class CopilotReportReviewRequest(BaseModel):
    disposition: str = "acknowledged"
    notes: str = ""


class CopilotResponse(BaseModel):
    conclusion: str
    evidence: List[Dict[str, Any]] = Field(default_factory=list)
    key_evidence: List[Dict[str, Any]] = Field(default_factory=list)
    impact_scope: Dict[str, Any] = Field(default_factory=dict)
    recommended_action: Dict[str, Any] = Field(default_factory=dict)
    suggested_action: Dict[str, Any] = Field(default_factory=dict)
    confidence: str = "medium"
    metric_window: str = "7d"
    risk_notes: List[str] = Field(default_factory=list)
    methodology: Dict[str, Any] = Field(default_factory=dict)
    query_id: str | None = None
    anomaly_id: str | None = None
    report_id: str | None = None
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
