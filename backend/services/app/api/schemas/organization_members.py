from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


MANAGEABLE_ORG_ROLES = {"admin", "member"}
UPDATABLE_ORG_ROLES = {"owner", "admin", "member"}


def _normalize_email(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        raise ValueError("email is required.")
    return normalized


def _normalize_manageable_role(value: str | None) -> str:
    normalized = str(value or "member").strip().lower()
    if normalized not in MANAGEABLE_ORG_ROLES:
        raise ValueError("role must be one of admin or member.")
    return normalized


def _normalize_updatable_role(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in UPDATABLE_ORG_ROLES:
        raise ValueError("role must be one of owner, admin, or member.")
    return normalized


class OrganizationMemberCreateRequest(BaseModel):
    email: str
    display_name: str | None = None
    role: str = "member"

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        return _normalize_email(value)

    @field_validator("role")
    @classmethod
    def validate_role(cls, value: str) -> str:
        return _normalize_manageable_role(value)


class OrganizationMemberUpdateRequest(BaseModel):
    role: str
    confirm_owner_transfer: bool = False

    @field_validator("role")
    @classmethod
    def validate_role(cls, value: str) -> str:
        return _normalize_updatable_role(value)


class OrganizationInviteCreateRequest(BaseModel):
    email: str
    display_name: str | None = None
    role: str = "member"
    expires_in_days: int = Field(default=7, ge=1, le=30)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        return _normalize_email(value)

    @field_validator("role")
    @classmethod
    def validate_role(cls, value: str) -> str:
        return _normalize_manageable_role(value)


class OrganizationInviteRedeemRequest(BaseModel):
    invite_code: str

class ProjectDeleteRequest(BaseModel):
    confirmation: str

    @field_validator("confirmation")
    @classmethod
    def validate_confirmation(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if normalized != "delete":
            raise ValueError("confirmation must equal 'delete'.")
        return normalized
