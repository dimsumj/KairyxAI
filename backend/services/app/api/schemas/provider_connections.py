from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field


class ProviderConnectionCreateRequest(BaseModel):
    name: str
    provider: str
    config: Dict[str, Any] = Field(default_factory=dict)


class ProviderConnectionUpdateRequest(BaseModel):
    name: str | None = None
    config: Dict[str, Any] | None = None


class ProviderConnectionResponse(BaseModel):
    provider_connection_id: str
    name: str
    provider: str
    status: str
    config: Dict[str, Any] = Field(default_factory=dict)
    tenant_id: str
    project_id: str
    created_by: str
    updated_by: str
    correlation_id: str
    created_at: str
    updated_at: str
