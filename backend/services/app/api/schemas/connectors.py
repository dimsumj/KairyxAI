from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class ConnectorCreateRequest(BaseModel):
    name: str
    type: str
    connector_id: str | None = None
    config: Dict[str, Any] = Field(default_factory=dict)


class ConnectorResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    connector_id: str | None = None
    name: str
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    created_at: str
    updated_at: str


class ConnectorHealthResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    connector_id: str | None = None
    name: str
    type: str
    ok: bool
    message: Optional[str] = None


class ConnectorTableListResponse(BaseModel):
    tenant_id: str | None = None
    project_id: str | None = None
    connector_id: str | None = None
    name: str
    type: str
    items: list[Dict[str, Any]] = Field(default_factory=list)
