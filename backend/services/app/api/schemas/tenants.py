from __future__ import annotations

from pydantic import BaseModel


class TenantCreateRequest(BaseModel):
    tenant_id: str
    name: str
    status: str = "active"


class TenantResponse(BaseModel):
    tenant_id: str
    name: str
    status: str
    created_at: str
    updated_at: str


class TenantMembershipRequest(BaseModel):
    user_id: str
    role: str
    status: str = "active"
    email: str | None = None
    display_name: str | None = None


class PlatformUserResponse(BaseModel):
    user_id: str
    email: str | None = None
    display_name: str | None = None
    created_at: str
    updated_at: str


class TenantMembershipResponse(BaseModel):
    id: int
    tenant_id: str
    user_id: str
    role: str
    status: str
    created_at: str
    updated_at: str
