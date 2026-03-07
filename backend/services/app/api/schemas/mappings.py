from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, Field


class MappingUpdateRequest(BaseModel):
    mapping: Dict[str, Any] = Field(default_factory=dict)


class MappingResponse(BaseModel):
    connector_name: str
    mapping: Dict[str, Any] = Field(default_factory=dict)
