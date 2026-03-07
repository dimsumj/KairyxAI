from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class ExportJobCreateRequest(BaseModel):
    prediction_job_id: str
    provider: str = "webhook"
    channel: str = "push_notification"
    include_churned: bool = False
    include_risks: List[str] = Field(default_factory=lambda: ["high", "medium"])
    audience_name: Optional[str] = None
    webhook_url: Optional[str] = None
    webhook_token: Optional[str] = None
