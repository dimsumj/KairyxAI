from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field


class PushDispatchSendRequest(BaseModel):
    name: str | None = None
    user_id: str
    provider_connection_id: str | None = None
    campaign_name: str | None = None
    title: str | None = None
    body: str
    deep_link: str | None = None
    deep_link_token: str | None = None
    data: Dict[str, Any] = Field(default_factory=dict)
    provider_options: Dict[str, Any] = Field(default_factory=dict)


class PushDispatchResponse(BaseModel):
    push_dispatch_id: str
    name: str
    status: str
    channel: str = "push_notification"
    user_id: str
    provider: str
    provider_mode: str
    provider_backend: str
    provider_connection_id: str | None = None
    campaign_name: str | None = None
    title: str | None = None
    body: str | None = None
    deep_link: str | None = None
    deep_link_token: str | None = None
    data: Dict[str, Any] = Field(default_factory=dict)
    provider_options: Dict[str, Any] = Field(default_factory=dict)
    provider_request_id: str | None = None
    provider_campaign_id: str | None = None
    provider_accepted: bool | None = None
    simulated: bool = False
    send_attempts: int = 0
    last_send_started_at: str | None = None
    last_send_completed_at: str | None = None
    last_error: str | None = None
    result_summary: Dict[str, Any] = Field(default_factory=dict)
    tenant_id: str | None = None
    project_id: str | None = None
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    created_at: str
    updated_at: str
