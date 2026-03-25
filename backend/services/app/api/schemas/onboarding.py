from __future__ import annotations

from pydantic import BaseModel


class OrganizationSpaceOnboardingRequest(BaseModel):
    organization_id: str
    organization_name: str
    project_id: str
    project_name: str
    project_description: str = ""
