from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class CohortCreateRequest(BaseModel):
    name: str
    type: str
    definition: Dict[str, Any] = Field(default_factory=dict)
    refresh_mode: str = "manual"
    owner: str = "system"
    description: str = ""
    tags: List[str] = Field(default_factory=list)
    activate: bool = False


class CohortActionRequest(BaseModel):
    force: bool = False


class CohortUpdateRequest(BaseModel):
    name: str | None = None
    type: str | None = None
    definition: Dict[str, Any] | None = None
    refresh_mode: str | None = None
    owner: str | None = None
    description: str | None = None
    tags: List[str] | None = None


class CohortResponse(BaseModel):
    cohort_id: str
    name: str
    type: str
    status: str
    tenant_id: str | None = None
    project_id: str | None = None
    created_by: str = "system"
    updated_by: str = "system"
    correlation_id: str = ""
    refresh_mode: str
    owner: str
    description: str = ""
    version: int
    version_id: int
    member_count: int = 0
    last_refreshed_at: str | None = None
    deleted_at: str | None = None
    definition: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    preview_members: List[Dict[str, Any]] = Field(default_factory=list)
    delta: Dict[str, int] = Field(default_factory=dict)
    refresh_policy: Dict[str, Any] = Field(default_factory=dict)
    activation_preflight: Dict[str, Any] = Field(default_factory=dict)
    metrics_summary: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class CohortMemberPage(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[Dict[str, Any]] = Field(default_factory=list)


class CohortVersionListResponse(BaseModel):
    items: List[Dict[str, Any]] = Field(default_factory=list)
