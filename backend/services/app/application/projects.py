from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List

from app.core.request_context import RequestContext, request_context
from bigquery_service import BigQueryService, clear_shared_bigquery_service_cache
from gcs_service import GcsService


IDENTIFIER_RE = re.compile(r"^[a-z0-9](?:[a-z0-9_-]{0,63})$")
PROJECT_ROLES = {"admin", "analyst", "operator"}
ORG_ROLES = {"owner", "admin", "member"}
ORG_ADMIN_ROLES = {"owner", "admin"}
MANAGEABLE_ORG_ROLES = {"admin", "member"}


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

    @staticmethod
    def _normalize_email(value: str | None) -> str | None:
        normalized = str(value or "").strip().lower()
        return normalized or None

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

    def _validate_manageable_org_role(self, value: str | None) -> str:
        normalized = str(value or "member").strip().lower()
        if normalized not in MANAGEABLE_ORG_ROLES:
            raise ValueError("role must be one of admin or member.")
        return normalized

    @staticmethod
    def _apply_default_project_marker(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        default_project_id = str(items[0]["project_id"]) if items else ""
        return [
            {
                **item,
                "is_default": bool(default_project_id and str(item["project_id"]) == default_project_id),
            }
            for item in items
        ]

    @staticmethod
    def _delete_project_storage_scope(*, tenant_id: str, project_id: str, user_id: str) -> None:
        scoped_context = RequestContext(
            actor_id=user_id,
            actor_role="admin",
            tenant_id=tenant_id,
            project_id=project_id,
            correlation_id=f"project-delete-{tenant_id}-{project_id}",
            org_role="owner",
            project_role="admin",
        )
        with request_context(scoped_context):
            GcsService().delete_project_scope()
            BigQueryService().delete_project_scope()
        clear_shared_bigquery_service_cache()

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

        default_role = project_role_for_org_role(membership.get("role"))
        items = [
            {
                **item,
                "role": default_role,
            }
            for item in self.repository.list_projects(tenant_key)
        ]
        return self._apply_default_project_marker(items)

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

    def list_organization_members(self, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        membership = self.repository.get_tenant_membership(tenant_key, user_id)
        if str((membership or {}).get("status") or "").lower() != "active":
            return []
        return self.repository.list_organization_members(tenant_key)

    def create_organization_member(
        self,
        tenant_id: str,
        *,
        email: str,
        display_name: str | None,
        role: str,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        if self.repository.get_tenant(tenant_key) is None:
            raise KeyError(tenant_key)
        normalized_email = self._normalize_email(email)
        if normalized_email is None:
            raise ValueError("email is required.")
        validated_role = self._validate_manageable_org_role(role)
        existing_user = self.repository.get_platform_user_by_email(normalized_email)
        existing_members = [
            item
            for item in self.repository.list_organization_members(tenant_key)
            if self._normalize_email(item.get("email")) == normalized_email and str(item.get("status") or "").lower() == "active"
        ]
        if existing_members:
            raise ValueError(f"'{normalized_email}' is already a member of organization space '{tenant_key}'.")
        invite = self.repository.create_organization_invite(
            tenant_key,
            invite_code=f"oinv_{uuid.uuid4().hex[:24]}",
            email=normalized_email,
            display_name=(str(display_name or "").strip() or None),
            role=validated_role,
            expires_at=datetime.utcnow() + timedelta(days=7),
        )
        membership = None
        if existing_user is not None:
            membership = self.repository.upsert_tenant_membership(tenant_key, str(existing_user["user_id"]), role=validated_role, status="active")
        return {
            "member": membership,
            "invite": invite,
        }

    def update_organization_member_role(
        self,
        tenant_id: str,
        membership_id: int,
        *,
        role: str,
        actor_user_id: str,
        actor_org_role: str | None,
        confirm_owner_transfer: bool = False,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        actor_membership = self.repository.get_tenant_membership(tenant_key, actor_user_id)
        if str((actor_membership or {}).get("status") or "").lower() != "active":
            raise PermissionError("Organization admin access is required.")
        actor_role = normalize_org_role((actor_membership or {}).get("role") or actor_org_role)
        if actor_role not in ORG_ADMIN_ROLES:
            raise PermissionError("Organization admin access is required.")
        membership = self.repository.get_tenant_membership_by_id(membership_id)
        if membership is None or str(membership.get("tenant_id")) != tenant_key:
            raise KeyError(membership_id)
        if str(membership.get("status") or "").lower() != "active":
            raise ValueError("Only active organization members can be updated.")
        current_role = normalize_org_role(membership.get("role"))
        if current_role == "owner":
            if actor_role != "owner":
                raise PermissionError("Only the organization owner can affect the current owner.")
            raise ValueError("Transfer organization ownership instead of changing the owner role directly.")

        requested_role = normalize_org_role(role)
        if requested_role == "owner":
            if actor_role != "owner":
                raise PermissionError("Only the organization owner can transfer ownership.")
            if not confirm_owner_transfer:
                raise ValueError("confirm_owner_transfer must be true to transfer organization ownership.")
            return self.transfer_organization_owner(
                tenant_key,
                membership_id,
                actor_user_id=actor_user_id,
                confirmation=True,
            )

        updated = self.repository.update_tenant_membership_role(
            membership_id,
            role=self._validate_manageable_org_role(requested_role),
        )
        return {"member": updated}

    def remove_organization_member(
        self,
        tenant_id: str,
        membership_id: int,
        *,
        actor_user_id: str,
        actor_org_role: str | None,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        actor_membership = self.repository.get_tenant_membership(tenant_key, actor_user_id)
        if str((actor_membership or {}).get("status") or "").lower() != "active":
            raise PermissionError("Organization admin access is required.")
        actor_role = normalize_org_role((actor_membership or {}).get("role") or actor_org_role)
        if actor_role not in ORG_ADMIN_ROLES:
            raise PermissionError("Organization admin access is required.")
        membership = self.repository.get_tenant_membership_by_id(membership_id)
        if membership is None or str(membership.get("tenant_id")) != tenant_key:
            raise KeyError(membership_id)
        current_role = normalize_org_role(membership.get("role"))
        if current_role == "owner":
            raise ValueError("Transfer organization ownership before removing the owner.")
        removed = self.repository.delete_tenant_membership(membership_id)
        return {"removed_member": removed}

    def transfer_organization_owner(
        self,
        tenant_id: str,
        membership_id: int,
        *,
        actor_user_id: str,
        confirmation: bool,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        if not confirmation:
            raise ValueError("confirmation must be true.")
        actor_membership = self.repository.get_tenant_membership(tenant_key, actor_user_id)
        if str((actor_membership or {}).get("status") or "").lower() != "active":
            raise PermissionError("Organization owner access is required.")
        if normalize_org_role(actor_membership.get("role")) != "owner":
            raise PermissionError("Organization owner access is required.")
        membership = self.repository.get_tenant_membership_by_id(membership_id)
        if membership is None or str(membership.get("tenant_id")) != tenant_key:
            raise KeyError(membership_id)
        if str(membership.get("status") or "").lower() != "active":
            raise ValueError("The selected organization member is not active.")
        target_user_id = str(membership.get("user_id") or "").strip()
        if not target_user_id:
            raise ValueError("The selected organization member does not have an active user.")
        target_role = normalize_org_role(membership.get("role"))
        if target_role == "owner":
            raise ValueError("The selected organization member already owns this organization.")
        if target_user_id == str(actor_user_id):
            raise ValueError("Choose a different organization member to transfer ownership.")
        new_owner_membership = self.repository.update_tenant_membership_role(membership_id, role="owner")
        previous_owner_membership = self.repository.update_tenant_membership_role(int(actor_membership["id"]), role="admin")
        return {
            "organization_id": tenant_key,
            "member": new_owner_membership,
            "new_owner": new_owner_membership,
            "previous_owner": previous_owner_membership,
        }

    def create_organization_invite(
        self,
        tenant_id: str,
        *,
        email: str,
        display_name: str | None,
        role: str,
        expires_in_days: int = 7,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        if self.repository.get_tenant(tenant_key) is None:
            raise KeyError(tenant_key)
        return self.repository.create_organization_invite(
            tenant_key,
            invite_code=f"oinv_{uuid.uuid4().hex[:24]}",
            email=self._normalize_email(email) or "",
            display_name=(str(display_name or "").strip() or None),
            role=self._validate_manageable_org_role(role),
            expires_at=datetime.utcnow() + timedelta(days=max(1, int(expires_in_days))),
        )

    def redeem_organization_invite(
        self,
        invite_code: str,
        *,
        user_id: str,
        email: str | None,
        display_name: str | None,
    ) -> Dict[str, Any]:
        invite = self.repository.get_organization_invite(str(invite_code))
        if invite is None:
            raise KeyError(invite_code)
        expires_at = invite.get("expires_at")
        if str(invite.get("status") or "").lower() == "pending" and expires_at and datetime.fromisoformat(expires_at) < datetime.utcnow():
            raise ValueError("Invite has expired.")
        invite_email = self._normalize_email(invite.get("email"))
        current_email = self._normalize_email(email)
        if invite_email and invite_email != current_email:
            raise ValueError("Invite email does not match the authenticated user.")
        existing_membership = self.repository.get_tenant_membership(str(invite["tenant_id"]), user_id)
        if str(invite.get("status") or "").lower() == "expired":
            raise ValueError("Invite has expired.")
        if str(invite.get("status") or "").lower() != "pending":
            if str(invite.get("redeemed_by") or "").strip() == str(user_id) or str((existing_membership or {}).get("status") or "").lower() == "active":
                return {
                    "invite": invite,
                    "organization_space": self.repository.get_tenant(str(invite["tenant_id"])),
                    "organization_membership": existing_membership,
                }
            raise ValueError("Invite is no longer redeemable.")
        self.repository.upsert_platform_user(user_id, email=current_email, display_name=display_name)
        membership = self.repository.upsert_tenant_membership(
            str(invite["tenant_id"]),
            user_id,
            role=self._validate_manageable_org_role(invite.get("role")),
            status="active",
        )
        redeemed_invite = self.repository.mark_organization_invite_redeemed(str(invite_code), redeemed_by=user_id)
        return {
            "invite": redeemed_invite,
            "organization_space": self.repository.get_tenant(str(invite["tenant_id"])),
            "organization_membership": membership,
        }

    def activate_pending_organization_invites(
        self,
        *,
        user_id: str,
        email: str | None,
        display_name: str | None,
    ) -> List[Dict[str, Any]]:
        normalized_email = self._normalize_email(email)
        if normalized_email is None:
            return []
        self.repository.upsert_platform_user(user_id, email=normalized_email, display_name=display_name)
        return self.repository.activate_organization_invites_for_email(
            email=normalized_email,
            user_id=user_id,
            display_name=display_name,
        )

    def delete_project_permanently(
        self,
        tenant_id: str,
        project_id: str,
        *,
        user_id: str,
    ) -> Dict[str, Any]:
        tenant_key = self._validate_identifier(tenant_id, label="organization_id")
        project_key = self._validate_identifier(project_id, label="project_id")
        project = self.repository.get_project(tenant_key, project_key)
        if project is None:
            raise KeyError(project_key)
        self._delete_project_storage_scope(tenant_id=tenant_key, project_id=project_key, user_id=user_id)
        deleted = self.repository.delete_project_permanently(tenant_key, project_key)
        if not deleted:
            raise KeyError(project_key)
        remaining_projects = self.list_accessible_projects(tenant_key, user_id=user_id)
        next_default_project = str(remaining_projects[0]["project_id"]) if remaining_projects else None
        return {
            "deleted_project_id": project_key,
            "organization_id": tenant_key,
            "remaining_projects": remaining_projects,
            "next_default_project_id": next_default_project,
        }

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
