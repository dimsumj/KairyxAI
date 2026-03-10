from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class SqlPreviewRequest(BaseModel):
    sql: str
    limit: int = 50
    timeout_seconds: int = 30
    scan_limit_rows: int = 50000


class SavedQueryCreateRequest(BaseModel):
    name: str
    sql: str
    description: str = ""


class SavedQueryToCohortRequest(BaseModel):
    name: str
    refresh_mode: str = "manual"
    owner: str = "system"
    activate: bool = False


class SqlPreviewResponse(BaseModel):
    sql: str
    resolved_sql: str
    aliases: Dict[str, str] = Field(default_factory=dict)
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    estimated_scan_rows: int = 0
    timeout_seconds: int = 30
    scan_limit_rows: int = 50000
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)


class SavedQueryResponse(BaseModel):
    query_id: str
    name: str
    description: str = ""
    sql: str
    created_at: str
    updated_at: str
    audit_id: int | None = None
    masked_fields: List[str] = Field(default_factory=list)
