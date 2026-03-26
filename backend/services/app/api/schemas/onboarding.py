from __future__ import annotations

import re

from pydantic import BaseModel, field_validator


NEW_ORGANIZATION_ID_PATTERN = re.compile(r"^[a-z0-9]{1,16}$")


class OrganizationSpaceOnboardingRequest(BaseModel):
    organization_id: str
    organization_name: str
    project_id: str
    project_name: str
    project_description: str = ""

    @field_validator("organization_id")
    @classmethod
    def validate_organization_id(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if NEW_ORGANIZATION_ID_PATTERN.fullmatch(normalized) is None:
            raise ValueError("organization_id must use only lowercase letters and numbers and be 16 characters or fewer.")
        return normalized
