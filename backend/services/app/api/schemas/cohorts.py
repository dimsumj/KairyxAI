from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class CohortCreateRequest(BaseModel):
    name: str
    type: str
    definition: Dict[str, Any] = Field(default_factory=dict)
    refresh_mode: str = "manual"
    owner: str = "system"
    tags: List[str] = Field(default_factory=list)
    activate: bool = False


class CohortActionRequest(BaseModel):
    force: bool = False


class CohortResponse(BaseModel):
    cohort_id: str
    name: str
    type: str
    status: str
    refresh_mode: str
    owner: str
    version: int
    member_count: int = 0
    last_refreshed_at: str | None = None
    definition: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    preview_members: List[Dict[str, Any]] = Field(default_factory=list)
    delta: Dict[str, int] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class CohortMemberPage(BaseModel):
    page: int
    page_size: int
    total: int
    items: List[Dict[str, Any]] = Field(default_factory=list)
