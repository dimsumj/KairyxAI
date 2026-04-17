from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class McpConnectionValidateRequest(BaseModel):
    endpoint_url: str
    preset_key: str | None = None


class McpConnectionValidateResponse(BaseModel):
    endpoint_url: str
    preset_key: str
    transport_type: str
    auth_mode: str
    is_valid: bool = True
    notes: List[str] = Field(default_factory=list)


class McpConnectionCreateRequest(BaseModel):
    name: str
    preset_key: str = "amplitude_us"
    endpoint_url: str | None = None


class McpConnectionUpdateRequest(BaseModel):
    name: str | None = None
    preset_key: str | None = None
    endpoint_url: str | None = None
    status: str | None = None


class McpConnectionAuthorizationState(BaseModel):
    actor_id: str | None = None
    status: str = "not_authorized"
    authorized_at: str | None = None
    expires_at: str | None = None
    has_refresh_token: bool = False
    last_error: str = ""


class McpToolDefinition(BaseModel):
    name: str
    description: str = ""
    allowed: bool = False
    classification: str = "blocked"
    input_schema: Dict[str, Any] = Field(default_factory=dict)


class McpConnectionResponse(BaseModel):
    mcp_connection_id: str
    name: str
    preset_key: str
    endpoint_url: str
    transport_type: str
    auth_mode: str
    status: str
    allowed_tools: List[str] = Field(default_factory=list)
    discovered_tools: List[McpToolDefinition] = Field(default_factory=list)
    discovered_tool_count: int = 0
    authorization: McpConnectionAuthorizationState = Field(default_factory=McpConnectionAuthorizationState)
    last_discovered_at: str | None = None
    last_validated_at: str | None = None
    tenant_id: str
    project_id: str
    created_by: str
    updated_by: str
    correlation_id: str
    created_at: str
    updated_at: str


class McpConnectionAuthStartResponse(BaseModel):
    authorization_url: str
    state_id: str
    popup_title: str = "Authorize MCP Connection"


class McpConnectionQueryResultRequest(BaseModel):
    question: str
    answer: str = ""
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    tool_calls: List[Dict[str, Any]] = Field(default_factory=list)
    result: Dict[str, Any] = Field(default_factory=dict)


class McpResultSnapshotImportRequest(BaseModel):
    name: str | None = None
    query_result: McpConnectionQueryResultRequest


class McpResultSnapshotCreateCohortRequest(BaseModel):
    name: str
    description: str = ""
    tags: List[str] = Field(default_factory=lambda: ["mcp", "snapshot"])


class McpResultSnapshotResponse(BaseModel):
    snapshot_id: str
    mcp_connection_id: str
    name: str
    question: str
    answer: str = ""
    row_count: int = 0
    identifier_fields: List[str] = Field(default_factory=list)
    rows_preview: List[Dict[str, Any]] = Field(default_factory=list)
    status: str
    tenant_id: str
    project_id: str
    created_by: str
    updated_by: str
    correlation_id: str
    created_at: str
    updated_at: str
