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


class CopilotResponse(BaseModel):
    conclusion: str
    key_evidence: List[Dict[str, Any]] = Field(default_factory=list)
    impact_scope: Dict[str, Any] = Field(default_factory=dict)
    suggested_action: Dict[str, Any] = Field(default_factory=dict)
    confidence: str = "medium"
    methodology: Dict[str, Any] = Field(default_factory=dict)
