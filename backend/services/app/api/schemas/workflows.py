from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class WorkflowCreateRequest(BaseModel):
    name: str
    cohort_id: str
    schedule: Dict[str, Any] = Field(default_factory=lambda: {"type": "daily"})
    action: Dict[str, Any] = Field(default_factory=dict)
    policy: Dict[str, Any] = Field(default_factory=dict)
    experiment_id: str | None = None
    requires_confirmation: bool = False


class WorkflowRunRequest(BaseModel):
    limit: int = 20
    confirm: bool = False
    sandbox: bool = True


class WorkflowResponse(BaseModel):
    workflow_id: str
    name: str
    status: str
    current_version: int
    published_version: int | None = None
    definition: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class WorkflowExecutionPage(BaseModel):
    items: List[Dict[str, Any]] = Field(default_factory=list)
