from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field


class EmailCampaignCreateRequest(BaseModel):
    name: str
    provider_connection_id: str
    template_id: str
    audience: Dict[str, Any] = Field(default_factory=dict)
    merge_fields: Dict[str, Any] = Field(default_factory=dict)
    from_email: str | None = None
    from_name: str | None = None
    subject: str | None = None
    provider: str | None = None
    recipient_email_field: str | None = "email"
    recipient_external_id_field: str | None = None
    deeplink_template: str | None = None
    deeplink_override_field: str | None = None
    deeplink_template_field: str | None = None
    schedule_at: str | None = None


class EmailCampaignUpdateRequest(BaseModel):
    name: str | None = None
    provider_connection_id: str | None = None
    template_id: str | None = None
    audience: Dict[str, Any] | None = None
    merge_fields: Dict[str, Any] | None = None
    from_email: str | None = None
    from_name: str | None = None
    subject: str | None = None
    provider: str | None = None
    recipient_email_field: str | None = None
    recipient_external_id_field: str | None = None
    deeplink_template: str | None = None
    deeplink_override_field: str | None = None
    deeplink_template_field: str | None = None
    schedule_at: str | None = None


class EmailCampaignResponse(BaseModel):
    email_campaign_id: str
    name: str
    status: str
    provider: str
    provider_connection_id: str
    template_id: str
    template_summary: Dict[str, Any] = Field(default_factory=dict)
    from_email: str | None = None
    from_name: str | None = None
    subject: str | None = None
    audience: Dict[str, Any] = Field(default_factory=dict)
    recipient_email_field: str | None = "email"
    recipient_external_id_field: str | None = None
    merge_fields: Dict[str, Any] = Field(default_factory=dict)
    deeplink_template: str | None = None
    deeplink_override_field: str | None = None
    deeplink_template_field: str = "deeplink_url"
    schedule_at: str | None = None
    send_attempts: int = 0
    last_send_started_at: str | None = None
    last_send_completed_at: str | None = None
    last_error: str | None = None
    cancelled_at: str | None = None
    result_summary: Dict[str, Any] = Field(default_factory=dict)
    tenant_id: str | None = None
    project_id: str | None = None
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    created_at: str
    updated_at: str
