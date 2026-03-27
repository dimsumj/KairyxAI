from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List


IDENTIFIER_RE = re.compile(r"^[a-z0-9](?:[a-z0-9_-]{0,63})$")
PROJECT_ROLES = {"admin", "analyst", "operator"}
ORG_ROLES = {"owner", "admin", "member"}


def normalize_org_role(raw_role: str | None) -> str:
    normalized = str(raw_role or "").strip().lower()
    if normalized in {"owner", "admin"}:
        return normalized
    return "member"


def project_role_for_org_role(raw_role: str | None) -> str:
    normalized = normalize_org_role(raw_role)
    if normalized in {"owner", "admin"}:
        return "admin"
    return "operator"


class ProjectWorkspaceService:
    def __init__(self, repository):
        self.repository = repository

    def _validate_identifier(self, value: str, *, label: str) -> str:
        normalized = str(value or "").strip().lower()
        if not IDENTIFIER_RE.match(normalized):
            raise ValueError(f"{label} must match [a-z0-9][a-z0-9_-]{{0,63}}.")
        return normalized

    def _validate_project_role(self, value: str | None) -> str:
        normalized = str(value or "operator").strip().lower()
        if normalized not in PROJECT_ROLES:
            raise ValueError("project_role must be one of admin, analyst, or operator.")
        return normalized

    def _validate_org_role(self, value: str | None) -> str:
        normalized = normalize_org_role(value)
        if normalized not in ORG_ROLES:
            raise ValueError("org_role must be one of owner, admin, or member.")
        return normalized

    def list_accessible_organization_spaces(self, user_id: str) -> List[Dict[str, Any]]:
        memberships = self.repository.list_user_tenant_memberships(user_id)
        items: List[Dict[str, Any]] = []
        for membership in memberships:
            if str(membership.get("status") or "").lower() != "active":
                continue
            tenant = self.repository.get_tenant(str(membership["tenant_id"]))
            if tenant is None:
                continue
            items.append(
                {
                    **tenant,
                    "role": normalize_org_role(membership.get("role")),
                }
            )
        return items

    def list_accessible_projects(self, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        membership = self.repository.get_tenant_membership(tenant_key, user_id)
        if str((membership or {}).get("status") or "").lower() != "active":
            return []

        project_memberships = {
            str(item["project_id"]): item
            for item in self.repository.list_project_memberships(tenant_id=tenant_key, user_id=user_id)
            if str(item.get("status") or "").lower() == "active"
        }
        default_role = project_role_for_org_role(membership.get("role"))

        items: List[Dict[str, Any]] = []
        for item in self.repository.list_projects(tenant_key):
            explicit_membership = project_memberships.get(str(item["project_id"]))
            items.append(
                {
                    **item,
                    "role": str((explicit_membership or {}).get("role") or default_role),
                }
            )
        return items

    def inspect_organization_space_access(self, organization_id: str, user_id: str) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(organization_id, label="organization_id")
        tenant = self.repository.get_tenant(tenant_key)
        membership = self.repository.get_tenant_membership(tenant_key, user_id) if tenant else None
        membership_status = str(membership.get("status") or "").strip().lower() if membership else ""
        membership_active = membership_status == "active"
        return {
            "organization_id": tenant_key,
            "exists": tenant is not None,
            "accessible": bool(tenant and membership_active),
            "role": normalize_org_role(membership.get("role")) if membership_active else None,
            "membership_status": membership_status or None,
            "organization": ({
                **tenant,
                "role": normalize_org_role(membership.get("role")) if membership_active else None,
            } if tenant else None),
        }

    def create_organization_space_and_first_project(
        self,
        *,
        organization_id: str,
        organization_name: str,
        project_id: str,
        project_name: str,
        project_description: str | None,
        user_id: str,
        email: str | None,
        display_name: str | None,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(organization_id, label="organization_id")
        project_key = self._validate_identifier(project_id, label="project_id")
        tenant = self.repository.get_tenant(tenant_key)
        if tenant is not None:
            raise ValueError(f"Organization space '{tenant_key}' already exists.")
        self.repository.ensure_tenant(tenant_key, organization_name, status="active")
        project = self.repository.get_project(tenant_key, project_key)
        if project is not None:
            raise ValueError(f"Project '{project_key}' already exists in organization space '{tenant_key}'.")
        user = self.repository.upsert_platform_user(user_id, email=email, display_name=display_name)
        org_membership = self.repository.upsert_tenant_membership(tenant_key, user_id, role="owner", status="active")
        project = self.repository.ensure_project(
            tenant_key,
            project_key,
            project_name,
            description=project_description or "",
            status="active",
        )
        project_membership = self.repository.upsert_project_membership(tenant_key, project_key, user_id, role="admin", status="active")
        return {
            "organization_space": self.repository.get_tenant(tenant_key),
            "project": project,
            "user": user,
            "organization_membership": org_membership,
            "project_membership": project_membership,
        }

    def create_project(
        self,
        tenant_id: str,
        *,
        project_id: str,
        name: str,
        description: str | None,
        user_id: str | None = None,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        project_key = self._validate_identifier(project_id, label="project_id")
        tenant = self.repository.get_tenant(tenant_key)
        if tenant is None:
            raise KeyError(tenant_key)
        existing = self.repository.get_project(tenant_key, project_key)
        if existing is not None:
            raise ValueError(f"Project '{project_key}' already exists in organization space '{tenant_key}'.")
        project = self.repository.ensure_project(
            tenant_key,
            project_key,
            name,
            description=description or "",
            status="active",
        )
        if user_id:
            self.repository.upsert_project_membership(
                tenant_key,
                project_key,
                user_id,
                role="admin",
                status="active",
            )
        return project

    def create_project_invite(
        self,
        tenant_id: str,
        project_id: str,
        *,
        email: str | None,
        display_name: str | None,
        org_role: str,
        project_role: str,
        expires_in_days: int = 7,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        project_key = self._validate_identifier(project_id, label="project_id")
        if self.repository.get_project(tenant_key, project_key) is None:
            raise KeyError(project_key)
        invite = self.repository.create_project_invite(
            tenant_key,
            project_key,
            invite_code=f"pinv_{uuid.uuid4().hex[:24]}",
            email=(str(email or "").strip().lower() or None),
            display_name=(str(display_name or "").strip() or None),
            org_role=self._validate_org_role(org_role),
            project_role=self._validate_project_role(project_role),
            expires_at=datetime.utcnow() + timedelta(days=max(1, int(expires_in_days))),
        )
        return invite

    def redeem_project_invite(
        self,
        invite_code: str,
        *,
        user_id: str,
        email: str | None,
        display_name: str | None,
    ) -> Dict[str, Any]:
        invite = self.repository.get_project_invite(str(invite_code))
        if invite is None:
            raise KeyError(invite_code)
        if str(invite.get("status") or "").lower() != "pending":
            raise ValueError("Invite is no longer redeemable.")
        expires_at = invite.get("expires_at")
        if expires_at and datetime.fromisoformat(expires_at) < datetime.utcnow():
            raise ValueError("Invite has expired.")
        invite_email = str(invite.get("email") or "").strip().lower()
        current_email = str(email or "").strip().lower()
        if invite_email and invite_email != current_email:
            raise ValueError("Invite email does not match the authenticated user.")

        tenant_id = str(invite["tenant_id"])
        project_id = str(invite["project_id"])
        self.repository.upsert_platform_user(user_id, email=email, display_name=display_name)

        current_org_membership = self.repository.get_tenant_membership(tenant_id, user_id)
        if current_org_membership is None:
            org_membership = self.repository.upsert_tenant_membership(
                tenant_id,
                user_id,
                role=self._validate_org_role(invite.get("org_role")),
                status="active",
            )
        else:
            org_membership = current_org_membership

        current_project_membership = self.repository.get_project_membership(tenant_id, project_id, user_id)
        if current_project_membership is None:
            project_membership = self.repository.upsert_project_membership(
                tenant_id,
                project_id,
                user_id,
                role=self._validate_project_role(invite.get("project_role")),
                status="active",
            )
        else:
            project_membership = current_project_membership

        redeemed_invite = self.repository.mark_project_invite_redeemed(str(invite_code), redeemed_by=user_id)
        project = self.repository.get_project(tenant_id, project_id)
        tenant = self.repository.get_tenant(tenant_id)
        return {
            "invite": redeemed_invite,
            "organization_space": tenant,
            "project": project,
            "organization_membership": org_membership,
            "project_membership": project_membership,
        }
