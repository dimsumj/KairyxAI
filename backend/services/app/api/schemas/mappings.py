from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class MappingUpdateRequest(BaseModel):
    mapping: Dict[str, Any] = Field(default_factory=dict)
    scope_type: str = "source"
    scope_key: str | None = None
    changed_by: str = "system"


class MappingResponse(BaseModel):
    connector_name: str
    scope_type: str = "source"
    scope_key: str | None = None
    mapping: Dict[str, Any] = Field(default_factory=dict)
    required_coverage: float = 100.0
    effective_mapping: Dict[str, Any] = Field(default_factory=dict)


class MappingVersionItem(BaseModel):
    version: int
    created_at: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class MappingVersionListResponse(BaseModel):
    connector_name: str
    scope_type: str = "source"
    scope_key: str | None = None
    items: List[MappingVersionItem] = Field(default_factory=list)
