from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class JobProgress(BaseModel):
    current: int = 0
    total: int = 0
    pct: float = 0.0
    details: Dict[str, Any] = Field(default_factory=dict)


class JobResponse(BaseModel):
    id: str
    type: str
    status: str
    tenant_id: str | None = None
    project_id: str | None = None
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    created_at: str
    updated_at: str
    progress: JobProgress = Field(default_factory=JobProgress)
    error: Optional[str] = None
    links: Dict[str, str] = Field(default_factory=dict)
    spec: Dict[str, Any] = Field(default_factory=dict)
    quality_report: Dict[str, Any] = Field(default_factory=dict)
    checkpoint_state: Dict[str, Any] = Field(default_factory=dict)
    mapping_coverage: float = 0.0


class PaginatedResponse(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[Dict[str, Any]]
