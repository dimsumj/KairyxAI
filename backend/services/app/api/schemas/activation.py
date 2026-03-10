from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class ActivationCallbackEvent(BaseModel):
    delivery_id: str | None = None
    action_execution_id: str | None = None
    workflow_id: str | None = None
    user_id: str | None = None
    event_id: str | None = None
    event_type: str = "delivered"
    status: str | None = None
    occurred_at: str | None = None
    outcome_name: str | None = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ActivationCallbackIngestRequest(BaseModel):
    callbacks: List[ActivationCallbackEvent] = Field(default_factory=list)
