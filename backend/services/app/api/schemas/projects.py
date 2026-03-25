from __future__ import annotations

from pydantic import BaseModel, Field


class ProjectCreateRequest(BaseModel):
    project_id: str
    name: str
    description: str = ""


class ProjectResponse(BaseModel):
    tenant_id: str
    project_id: str
    name: str
    description: str = ""
    status: str
    created_by: str
    updated_by: str
    correlation_id: str
    created_at: str
    updated_at: str


class ProjectInviteCreateRequest(BaseModel):
    email: str | None = None
    display_name: str | None = None
    org_role: str = "member"
    project_role: str = "operator"
    expires_in_days: int = 7


class ProjectInviteRedeemRequest(BaseModel):
    invite_code: str


class ProjectInviteResponse(BaseModel):
    id: int
    tenant_id: str
    project_id: str
    invite_code: str
    invite_url: str
    email: str | None = None
    display_name: str | None = None
    org_role: str
    project_role: str
    status: str
    created_by: str
    redeemed_by: str | None = None
    correlation_id: str
    expires_at: str | None = None
    redeemed_at: str | None = None
    created_at: str
    updated_at: str


class ProjectListResponse(BaseModel):
    items: list[dict] = Field(default_factory=list)
