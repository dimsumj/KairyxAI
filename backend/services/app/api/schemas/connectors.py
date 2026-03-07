from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class ConnectorCreateRequest(BaseModel):
    name: str
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)


class ConnectorResponse(BaseModel):
    name: str
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class ConnectorHealthResponse(BaseModel):
    name: str
    type: str
    ok: bool
    message: Optional[str] = None
