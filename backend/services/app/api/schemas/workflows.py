from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class WorkflowCreateRequest(BaseModel):
    name: str
    cohort_id: str
    schedule: Dict[str, Any] = Field(default_factory=lambda: {"type": "daily"})
    action: Dict[str, Any] = Field(default_factory=dict)
    policy: Dict[str, Any] = Field(default_factory=dict)
    budget_policy: Dict[str, Any] = Field(default_factory=dict)
    trigger: Dict[str, Any] | None = None
    channel_config: Dict[str, Any] | None = None
    experiment_id: str | None = None
    requires_confirmation: bool = False
    steps: List[Dict[str, Any]] = Field(default_factory=list)


class WorkflowUpdateRequest(BaseModel):
    name: str | None = None
    cohort_id: str | None = None
    schedule: Dict[str, Any] | None = None
    action: Dict[str, Any] | None = None
    policy: Dict[str, Any] | None = None
    budget_policy: Dict[str, Any] | None = None
    trigger: Dict[str, Any] | None = None
    channel_config: Dict[str, Any] | None = None
    experiment_id: str | None = None
    requires_confirmation: bool | None = None
    steps: List[Dict[str, Any]] | None = None


class WorkflowRunRequest(BaseModel):
    limit: int = 20
    confirm: bool = False
    sandbox: bool = True
    reference_time: str | None = None
    confirmation_token: str | None = None


class OrchestratorRunRequest(BaseModel):
    reference_time: str | None = None
    limit_per_workflow: int = 100
    confirmation_tokens: Dict[str, str] = Field(default_factory=dict)


class WorkflowEventIngestRequest(BaseModel):
    event_type: str
    user_ids: List[str] = Field(default_factory=list)
    payload: Dict[str, Any] = Field(default_factory=dict)
    reference_time: str | None = None
    confirmation_tokens: Dict[str, str] = Field(default_factory=dict)


class WorkflowThresholdEvaluateRequest(BaseModel):
    metric_id: str
    value: float
    reference_time: str | None = None
    confirmation_tokens: Dict[str, str] = Field(default_factory=dict)


class WorkflowConfirmationRequest(BaseModel):
    note: str = ""
    valid_for_hours: int = 24


class WorkflowResponse(BaseModel):
    workflow_id: str
    name: str
    status: str
    tenant_id: str | None = None
    project_id: str | None = None
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    current_version: int
    published_version: int | None = None
    definition: Dict[str, Any] = Field(default_factory=dict)
    trigger: Dict[str, Any] = Field(default_factory=dict)
    policy: Dict[str, Any] = Field(default_factory=dict)
    budget_policy: Dict[str, Any] = Field(default_factory=dict)
    experiment_id: str | None = None
    channel_config: Dict[str, Any] = Field(default_factory=dict)
    publish_preflight: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class WorkflowExecutionPage(BaseModel):
    items: List[Dict[str, Any]] = Field(default_factory=list)
