from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import delete, desc, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.request_context import get_request_context
from app.infrastructure.db_models import (
    ActionHistoryModel,
    ConnectorConfigModel,
    ControlPlaneResourceEventModel,
    ControlPlaneResourceModel,
    ControlPlaneResourceVersionModel,
    ExperimentConfigModel,
    ExportJobModel,
    FieldMappingModel,
    ImportJobModel,
    IngestionCheckpointModel,
    MockWarehouseRowModel,
    OrganizationInviteModel,
    PlatformUserModel,
    PredictionJobModel,
    ProjectInviteModel,
    ProjectMembershipModel,
    ProjectModel,
    TenantMembershipModel,
    TenantModel,
)


def _to_json_text(value: Dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _from_json_text(value: str) -> Dict[str, Any]:
    if not value:
        return {}
    return json.loads(value)


class SqlAlchemyControlPlaneRepository:
    def __init__(self, session: Session):
        self.session = session

    def _bootstrap_tenant_id(self) -> str:
        return str(os.getenv("BOOTSTRAP_TENANT_ID", "default")).strip() or "default"

    def _bootstrap_project_id(self) -> str:
        return str(os.getenv("BOOTSTRAP_PROJECT_ID", "default")).strip() or "default"

    def _bootstrap_project_name(self) -> str:
        return str(os.getenv("BOOTSTRAP_PROJECT_NAME", "Default Project")).strip() or "Default Project"

    def _current_tenant_id(self) -> str | None:
        context = get_request_context()
        if context is None:
            return None
        return str(context.tenant_id or "").strip() or None

    def _current_project_id(self) -> str | None:
        context = get_request_context()
        if context is None:
            return None
        return str(context.project_id or "").strip() or None

    def _resolve_tenant_id(self, tenant_id: str | None = None, *, fallback_to_bootstrap: bool = False) -> str | None:
        resolved = str(tenant_id or "").strip() or self._current_tenant_id()
        if resolved:
            return resolved
        if fallback_to_bootstrap:
            return self._bootstrap_tenant_id()
        return None

    def _resolve_project_id(
        self,
        project_id: str | None = None,
        *,
        fallback_to_bootstrap: bool = False,
    ) -> str | None:
        resolved = str(project_id or "").strip() or self._current_project_id()
        if resolved:
            return resolved
        if fallback_to_bootstrap:
            return self._bootstrap_project_id()
        return None

    def _metadata(self, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, str]:
        context = get_request_context()
        resolved_tenant_id = self._resolve_tenant_id(tenant_id, fallback_to_bootstrap=True) or self._bootstrap_tenant_id()
        resolved_project_id = self._resolve_project_id(project_id, fallback_to_bootstrap=True) or self._bootstrap_project_id()
        actor_id = context.actor_id if context is not None else "system"
        correlation_id = context.correlation_id if context is not None else ""
        return {
            "tenant_id": resolved_tenant_id,
            "project_id": resolved_project_id,
            "actor_id": actor_id,
            "correlation_id": correlation_id,
        }

    def _augment_payload(
        self,
        payload: Dict[str, Any],
        *,
        tenant_id: str,
        project_id: str,
        created_by: str | None = None,
        updated_by: str | None = None,
        correlation_id: str = "",
    ) -> Dict[str, Any]:
        return {
            **payload,
            "tenant_id": tenant_id,
            "project_id": project_id,
            "created_by": created_by or payload.get("created_by") or "system",
            "updated_by": updated_by or payload.get("updated_by") or created_by or payload.get("updated_by") or "system",
            "correlation_id": correlation_id or payload.get("correlation_id") or "",
        }

    def _get_resource_row(
        self,
        resource_type: str,
        resource_id: str,
        *,
        tenant_id: str,
        project_id: str,
    ) -> ControlPlaneResourceModel | None:
        return self.session.execute(
            select(ControlPlaneResourceModel).where(
                ControlPlaneResourceModel.tenant_id == tenant_id,
                ControlPlaneResourceModel.project_id == project_id,
                ControlPlaneResourceModel.resource_type == resource_type,
                ControlPlaneResourceModel.resource_id == resource_id,
            )
        ).scalar_one_or_none()

    def ensure_tenant(self, tenant_id: str, name: str | None = None, *, status: str = "active") -> Dict[str, Any]:
        resolved_id = str(tenant_id).strip() or self._bootstrap_tenant_id()
        row = self.session.get(TenantModel, resolved_id)
        if row is None:
            row = TenantModel(tenant_id=resolved_id, name=name or resolved_id, status=status)
            self.session.add(row)
        else:
            row.name = name or row.name
            row.status = status or row.status
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._tenant_to_dict(row)

    def list_tenants(self) -> List[Dict[str, Any]]:
        rows = self.session.execute(select(TenantModel).order_by(TenantModel.tenant_id.asc())).scalars().all()
        return [self._tenant_to_dict(row) for row in rows]

    def get_tenant(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.get(TenantModel, tenant_id)
        return self._tenant_to_dict(row) if row else None

    def ensure_project(
        self,
        tenant_id: str,
        project_id: str,
        name: str | None = None,
        *,
        description: str | None = None,
        status: str = "active",
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        row = self.session.execute(
            select(ProjectModel).where(
                ProjectModel.tenant_id == metadata["tenant_id"],
                ProjectModel.project_id == metadata["project_id"],
            )
        ).scalar_one_or_none()
        if row is None:
            row = ProjectModel(
                tenant_id=metadata["tenant_id"],
                project_id=metadata["project_id"],
                name=name or metadata["project_id"],
                description=description,
                status=status,
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            row.name = name or row.name
            if description is not None:
                row.description = description
            row.status = status or row.status
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._project_to_dict(row)

    def get_project(self, tenant_id: str, project_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(
            select(ProjectModel).where(
                ProjectModel.tenant_id == str(tenant_id),
                ProjectModel.project_id == str(project_id),
            )
        ).scalar_one_or_none()
        return self._project_to_dict(row) if row else None

    def list_projects(
        self,
        tenant_id: str | None = None,
        *,
        user_id: str | None = None,
        include_inactive: bool = False,
    ) -> List[Dict[str, Any]]:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id)
        query = select(ProjectModel)
        if resolved_tenant_id:
            query = query.where(ProjectModel.tenant_id == resolved_tenant_id)
        if not include_inactive:
            query = query.where(ProjectModel.status == "active")
        rows = self.session.execute(query.order_by(ProjectModel.created_at.asc(), ProjectModel.project_id.asc())).scalars().all()
        items = [self._project_to_dict(row) for row in rows]
        if user_id:
            membership = self.get_tenant_membership(resolved_tenant_id or "", user_id) if resolved_tenant_id else None
            if str((membership or {}).get("status") or "").lower() != "active":
                return []
            items = [{**item, "role": str((membership or {}).get("role") or "member")} for item in items]
        return items

    def upsert_platform_user(self, user_id: str, *, email: str | None = None, display_name: str | None = None) -> Dict[str, Any]:
        resolved_user_id = str(user_id).strip()
        row = self.session.get(PlatformUserModel, resolved_user_id)
        if row is None:
            row = PlatformUserModel(user_id=resolved_user_id, email=email, display_name=display_name)
            self.session.add(row)
        else:
            row.email = email or row.email
            row.display_name = display_name or row.display_name
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._platform_user_to_dict(row)

    def get_platform_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.get(PlatformUserModel, user_id)
        return self._platform_user_to_dict(row) if row else None

    def get_platform_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        normalized_email = str(email or "").strip().lower()
        if not normalized_email:
            return None
        row = self.session.execute(
            select(PlatformUserModel).where(PlatformUserModel.email == normalized_email)
        ).scalar_one_or_none()
        return self._platform_user_to_dict(row) if row else None

    def upsert_tenant_membership(self, tenant_id: str, user_id: str, *, role: str, status: str = "active") -> Dict[str, Any]:
        row = self.session.execute(
            select(TenantMembershipModel).where(
                TenantMembershipModel.tenant_id == str(tenant_id),
                TenantMembershipModel.user_id == str(user_id),
            )
        ).scalar_one_or_none()
        if row is None:
            row = TenantMembershipModel(
                tenant_id=str(tenant_id),
                user_id=str(user_id),
                role=str(role),
                status=str(status or "active"),
            )
            self.session.add(row)
        else:
            row.role = str(role)
            row.status = str(status or row.status)
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._tenant_membership_to_dict(row)

    def get_tenant_membership(self, tenant_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(
            select(TenantMembershipModel).where(
                TenantMembershipModel.tenant_id == str(tenant_id),
                TenantMembershipModel.user_id == str(user_id),
            )
        ).scalar_one_or_none()
        return self._tenant_membership_to_dict(row) if row else None

    def list_tenant_memberships(self, tenant_id: str | None = None) -> List[Dict[str, Any]]:
        query = select(TenantMembershipModel)
        if tenant_id:
            query = query.where(TenantMembershipModel.tenant_id == str(tenant_id))
        rows = self.session.execute(query.order_by(TenantMembershipModel.tenant_id.asc(), TenantMembershipModel.user_id.asc())).scalars().all()
        return [self._tenant_membership_to_dict(row) for row in rows]

    def list_user_tenant_memberships(self, user_id: str) -> List[Dict[str, Any]]:
        rows = self.session.execute(
            select(TenantMembershipModel).where(TenantMembershipModel.user_id == str(user_id)).order_by(TenantMembershipModel.tenant_id.asc())
        ).scalars().all()
        return [self._tenant_membership_to_dict(row) for row in rows]

    def get_tenant_membership_by_id(self, membership_id: int) -> Optional[Dict[str, Any]]:
        row = self.session.get(TenantMembershipModel, int(membership_id))
        return self._tenant_membership_to_dict(row) if row else None

    def update_tenant_membership_role(self, membership_id: int, *, role: str) -> Dict[str, Any]:
        row = self.session.get(TenantMembershipModel, int(membership_id))
        if row is None:
            raise KeyError(membership_id)
        row.role = str(role)
        row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._tenant_membership_to_dict(row)

    def delete_tenant_membership(self, membership_id: int) -> Dict[str, Any]:
        row = self.session.get(TenantMembershipModel, int(membership_id))
        if row is None:
            raise KeyError(membership_id)
        payload = self._tenant_membership_to_dict(row)
        self.session.execute(
            delete(ProjectMembershipModel).where(
                ProjectMembershipModel.tenant_id == str(row.tenant_id),
                ProjectMembershipModel.user_id == str(row.user_id),
            )
        )
        self.session.delete(row)
        self.session.flush()
        return payload

    def list_organization_members(self, tenant_id: str) -> List[Dict[str, Any]]:
        rows = self.session.execute(
            select(TenantMembershipModel)
            .where(TenantMembershipModel.tenant_id == str(tenant_id))
            .order_by(TenantMembershipModel.created_at.asc(), TenantMembershipModel.id.asc())
        ).scalars().all()
        members: List[Dict[str, Any]] = []
        active_member_emails: set[str] = set()
        for row in rows:
            user = self.session.get(PlatformUserModel, row.user_id)
            normalized_email = str(user.email or "").strip().lower() if user and user.email else ""
            if normalized_email and str(row.status or "").lower() == "active":
                active_member_emails.add(normalized_email)
            members.append(
                {
                    **self._tenant_membership_to_dict(row),
                    "email": user.email if user else None,
                    "display_name": (user.display_name if user else None) or (user.email if user else None) or row.user_id,
                    "pending": str(row.status or "").lower() != "active",
                    "joined_at": row.created_at.isoformat() if row.created_at else None,
                }
            )
        pending_invites = self.session.execute(
            select(OrganizationInviteModel)
            .where(
                OrganizationInviteModel.tenant_id == str(tenant_id),
                OrganizationInviteModel.status == "pending",
            )
            .order_by(OrganizationInviteModel.created_at.asc(), OrganizationInviteModel.id.asc())
        ).scalars().all()
        for invite in pending_invites:
            normalized_email = str(invite.email or "").strip().lower()
            if normalized_email and normalized_email in active_member_emails:
                continue
            members.append(
                {
                    "id": f"invite:{invite.invite_code}",
                    "member_id": f"invite:{invite.invite_code}",
                    "organization_id": invite.tenant_id,
                    "tenant_id": invite.tenant_id,
                    "user_id": None,
                    "email": invite.email,
                    "display_name": invite.display_name or invite.email,
                    "role": invite.role,
                    "status": invite.status,
                    "pending": True,
                    "invite_code": invite.invite_code,
                    "created_at": invite.created_at.isoformat(),
                    "updated_at": invite.updated_at.isoformat(),
                    "joined_at": None,
                }
            )
        return members

    def upsert_project_membership(
        self,
        tenant_id: str,
        project_id: str,
        user_id: str,
        *,
        role: str,
        status: str = "active",
    ) -> Dict[str, Any]:
        row = self.session.execute(
            select(ProjectMembershipModel).where(
                ProjectMembershipModel.tenant_id == str(tenant_id),
                ProjectMembershipModel.project_id == str(project_id),
                ProjectMembershipModel.user_id == str(user_id),
            )
        ).scalar_one_or_none()
        if row is None:
            row = ProjectMembershipModel(
                tenant_id=str(tenant_id),
                project_id=str(project_id),
                user_id=str(user_id),
                role=str(role),
                status=str(status or "active"),
            )
            self.session.add(row)
        else:
            row.role = str(role)
            row.status = str(status or row.status)
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._project_membership_to_dict(row)

    def get_project_membership(self, tenant_id: str, project_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(
            select(ProjectMembershipModel).where(
                ProjectMembershipModel.tenant_id == str(tenant_id),
                ProjectMembershipModel.project_id == str(project_id),
                ProjectMembershipModel.user_id == str(user_id),
            )
        ).scalar_one_or_none()
        return self._project_membership_to_dict(row) if row else None

    def list_project_memberships(
        self,
        *,
        tenant_id: str | None = None,
        project_id: str | None = None,
        user_id: str | None = None,
    ) -> List[Dict[str, Any]]:
        query = select(ProjectMembershipModel)
        if tenant_id:
            query = query.where(ProjectMembershipModel.tenant_id == str(tenant_id))
        if project_id:
            query = query.where(ProjectMembershipModel.project_id == str(project_id))
        if user_id:
            query = query.where(ProjectMembershipModel.user_id == str(user_id))
        rows = self.session.execute(
            query.order_by(ProjectMembershipModel.tenant_id.asc(), ProjectMembershipModel.project_id.asc(), ProjectMembershipModel.user_id.asc())
        ).scalars().all()
        return [self._project_membership_to_dict(row) for row in rows]

    def create_project_invite(
        self,
        tenant_id: str,
        project_id: str,
        *,
        invite_code: str,
        email: str | None,
        display_name: str | None,
        org_role: str,
        project_role: str,
        status: str = "pending",
        expires_at: datetime | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        row = ProjectInviteModel(
            tenant_id=metadata["tenant_id"],
            project_id=metadata["project_id"],
            invite_code=str(invite_code),
            email=email,
            display_name=display_name,
            org_role=str(org_role),
            project_role=str(project_role),
            status=str(status),
            created_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
            expires_at=expires_at,
        )
        self.session.add(row)
        self.session.flush()
        return self._project_invite_to_dict(row)

    def get_project_invite(self, invite_code: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(
            select(ProjectInviteModel).where(ProjectInviteModel.invite_code == str(invite_code))
        ).scalar_one_or_none()
        return self._project_invite_to_dict(row) if row else None

    def mark_project_invite_redeemed(self, invite_code: str, *, redeemed_by: str, status: str = "redeemed") -> Dict[str, Any]:
        row = self.session.execute(
            select(ProjectInviteModel).where(ProjectInviteModel.invite_code == str(invite_code))
        ).scalar_one_or_none()
        if row is None:
            raise KeyError(invite_code)
        row.status = status
        row.redeemed_by = redeemed_by
        row.redeemed_at = datetime.utcnow()
        row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._project_invite_to_dict(row)

    def create_organization_invite(
        self,
        tenant_id: str,
        *,
        invite_code: str,
        email: str,
        display_name: str | None,
        role: str,
        status: str = "pending",
        expires_at: datetime | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, None)
        normalized_email = str(email or "").strip().lower()
        row = self.session.execute(
            select(OrganizationInviteModel).where(
                OrganizationInviteModel.tenant_id == metadata["tenant_id"],
                OrganizationInviteModel.email == normalized_email,
            )
        ).scalar_one_or_none()
        if row is None:
            row = OrganizationInviteModel(
                tenant_id=metadata["tenant_id"],
                invite_code=str(invite_code),
                email=normalized_email,
                display_name=display_name,
                role=str(role),
                status=str(status),
                created_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
                expires_at=expires_at,
            )
            self.session.add(row)
        else:
            row.invite_code = str(invite_code)
            row.display_name = display_name
            row.role = str(role)
            row.status = str(status)
            row.redeemed_by = None
            row.redeemed_at = None
            row.correlation_id = metadata["correlation_id"]
            row.expires_at = expires_at
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._organization_invite_to_dict(row)

    def get_organization_invite(self, invite_code: str) -> Optional[Dict[str, Any]]:
        row = self.session.execute(
            select(OrganizationInviteModel).where(OrganizationInviteModel.invite_code == str(invite_code))
        ).scalar_one_or_none()
        return self._organization_invite_to_dict(row) if row else None

    def list_organization_invites(self, tenant_id: str) -> List[Dict[str, Any]]:
        rows = self.session.execute(
            select(OrganizationInviteModel)
            .where(OrganizationInviteModel.tenant_id == str(tenant_id))
            .order_by(OrganizationInviteModel.created_at.asc(), OrganizationInviteModel.id.asc())
        ).scalars().all()
        return [self._organization_invite_to_dict(row) for row in rows]

    def mark_organization_invite_redeemed(self, invite_code: str, *, redeemed_by: str, status: str = "redeemed") -> Dict[str, Any]:
        row = self.session.execute(
            select(OrganizationInviteModel).where(OrganizationInviteModel.invite_code == str(invite_code))
        ).scalar_one_or_none()
        if row is None:
            raise KeyError(invite_code)
        row.status = status
        row.redeemed_by = redeemed_by
        row.redeemed_at = datetime.utcnow()
        row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._organization_invite_to_dict(row)

    def activate_organization_invites_for_email(
        self,
        *,
        email: str,
        user_id: str,
        display_name: str | None = None,
    ) -> List[Dict[str, Any]]:
        normalized_email = str(email or "").strip().lower()
        if not normalized_email:
            return []
        rows = self.session.execute(
            select(OrganizationInviteModel).where(
                OrganizationInviteModel.email == normalized_email,
                OrganizationInviteModel.status == "pending",
            )
        ).scalars().all()
        activated: List[Dict[str, Any]] = []
        now = datetime.utcnow()
        for row in rows:
            if row.expires_at and row.expires_at < now:
                row.status = "expired"
                row.updated_at = now
                continue
            self.upsert_platform_user(user_id, email=normalized_email, display_name=display_name)
            self.upsert_tenant_membership(
                row.tenant_id,
                user_id,
                role=row.role,
                status="active",
            )
            row.status = "redeemed"
            row.redeemed_by = str(user_id)
            row.redeemed_at = now
            row.updated_at = now
            activated.append(self._organization_invite_to_dict(row))
        self.session.flush()
        return activated

    def delete_project_permanently(self, tenant_id: str, project_id: str) -> bool:
        project_row = self.session.execute(
            select(ProjectModel).where(
                ProjectModel.tenant_id == str(tenant_id),
                ProjectModel.project_id == str(project_id),
            )
        ).scalar_one_or_none()
        if project_row is None:
            return False

        scoped_deletes = [
            ProjectInviteModel,
            ProjectMembershipModel,
            ConnectorConfigModel,
            FieldMappingModel,
            IngestionCheckpointModel,
            ExportJobModel,
            PredictionJobModel,
            ImportJobModel,
            ExperimentConfigModel,
            ActionHistoryModel,
            ControlPlaneResourceEventModel,
            ControlPlaneResourceVersionModel,
            ControlPlaneResourceModel,
        ]
        for model in scoped_deletes:
            self.session.execute(
                delete(model).where(
                    model.tenant_id == str(tenant_id),
                    model.project_id == str(project_id),
                )
            )

        mock_rows = self.session.execute(
            select(MockWarehouseRowModel)
        ).scalars().all()
        for row in mock_rows:
            try:
                payload = _from_json_text(row.payload_json)
            except json.JSONDecodeError:
                continue
            if (
                str(payload.get("tenant_id") or payload.get("organization_id") or "") == str(tenant_id)
                and str(payload.get("project_id") or "") == str(project_id)
            ):
                self.session.delete(row)

        self.session.delete(project_row)
        self.session.flush()
        return True

    def list_connectors(
        self,
        *,
        include_all_tenants: bool = False,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> List[Dict[str, Any]]:
        query = select(ConnectorConfigModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ConnectorConfigModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ConnectorConfigModel.project_id == resolved_project_id)
        rows = self.session.execute(query.order_by(ConnectorConfigModel.name.asc())).scalars().all()
        return [self._connector_to_dict(row) for row in rows]

    def get_connector(
        self,
        ref: str,
        *,
        tenant_id: str | None = None,
        project_id: str | None = None,
        include_all_tenants: bool = False,
    ) -> Optional[Dict[str, Any]]:
        query = select(ConnectorConfigModel).where(
            (ConnectorConfigModel.name == ref) | (ConnectorConfigModel.connector_id == ref)
        )
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ConnectorConfigModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ConnectorConfigModel.project_id == resolved_project_id)
        row = self.session.execute(query.order_by(ConnectorConfigModel.updated_at.desc())).scalars().first()
        return self._connector_to_dict(row) if row else None

    def upsert_connector(
        self,
        name: str,
        connector_type: str,
        config: Dict[str, Any],
        *,
        connector_id: str | None = None,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        row = self.session.execute(
            select(ConnectorConfigModel).where(
                ConnectorConfigModel.tenant_id == metadata["tenant_id"],
                ConnectorConfigModel.project_id == metadata["project_id"],
                (ConnectorConfigModel.name == name) | (ConnectorConfigModel.connector_id == str(connector_id or "")),
            )
        ).scalar_one_or_none()
        resolved_connector_id = str(connector_id or (row.connector_id if row is not None else f"conn_{uuid.uuid4().hex[:20]}")).strip()
        if row is None:
            row = ConnectorConfigModel(
                tenant_id=metadata["tenant_id"],
                project_id=metadata["project_id"],
                connector_id=resolved_connector_id,
                name=name,
                connector_type=connector_type,
                config_json=_to_json_text(dict(config or {})),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            row.name = name
            row.connector_type = connector_type
            row.connector_id = resolved_connector_id
            row.project_id = metadata["project_id"]
            row.config_json = _to_json_text(dict(config or {}))
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._connector_to_dict(row)

    def delete_connector(self, ref: str, *, tenant_id: str | None = None, project_id: str | None = None) -> bool:
        query = select(ConnectorConfigModel).where(
            (ConnectorConfigModel.name == ref) | (ConnectorConfigModel.connector_id == ref)
        )
        resolved_tenant_id = self._resolve_tenant_id(tenant_id)
        resolved_project_id = self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ConnectorConfigModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ConnectorConfigModel.project_id == resolved_project_id)
        row = self.session.execute(query.order_by(ConnectorConfigModel.updated_at.desc())).scalars().first()
        if row is None:
            return False
        self.session.delete(row)
        self.session.flush()
        return True

    def get_field_mapping(self, connector_name: str, *, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, Any]:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id, fallback_to_bootstrap=True)
        resolved_project_id = self._resolve_project_id(project_id, fallback_to_bootstrap=True)
        row = self.session.execute(
            select(FieldMappingModel).where(
                FieldMappingModel.tenant_id == resolved_tenant_id,
                FieldMappingModel.project_id == resolved_project_id,
                FieldMappingModel.connector_name == connector_name,
            )
        ).scalar_one_or_none()
        return _from_json_text(row.mapping_json) if row else {}

    def save_field_mapping(
        self,
        connector_name: str,
        mapping: Dict[str, Any],
        *,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        row = self.session.execute(
            select(FieldMappingModel).where(
                FieldMappingModel.tenant_id == metadata["tenant_id"],
                FieldMappingModel.project_id == metadata["project_id"],
                FieldMappingModel.connector_name == connector_name,
            )
        ).scalar_one_or_none()
        payload = self._augment_payload(mapping, tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = FieldMappingModel(
                tenant_id=metadata["tenant_id"],
                project_id=metadata["project_id"],
                connector_name=connector_name,
                mapping_json=_to_json_text(payload),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            payload["created_by"] = row.created_by
            payload["updated_by"] = metadata["actor_id"]
            row.project_id = metadata["project_id"]
            row.mapping_json = _to_json_text(payload)
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return {"tenant_id": metadata["tenant_id"], "project_id": metadata["project_id"], "connector_name": connector_name, "mapping": payload}

    def create_import_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._metadata(payload.get("tenant_id"), payload.get("project_id"))
        spec = self._augment_payload(payload.get("spec", {}), tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        row = ImportJobModel(
            id=payload["id"],
            tenant_id=metadata["tenant_id"],
            project_id=metadata["project_id"],
            source_name=payload["source_name"],
            status=payload["status"],
            spec_json=_to_json_text(spec),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
            created_by=metadata["actor_id"],
            updated_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "import")

    def list_import_jobs(self, *, tenant_id: str | None = None, project_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(ImportJobModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ImportJobModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ImportJobModel.project_id == resolved_project_id)
        rows = self.session.execute(query.order_by(desc(ImportJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "import") for row in rows]

    def get_import_job(self, job_id: str, *, tenant_id: str | None = None, project_id: str | None = None, include_all_tenants: bool = False) -> Optional[Dict[str, Any]]:
        query = select(ImportJobModel).where(ImportJobModel.id == job_id)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ImportJobModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ImportJobModel.project_id == resolved_project_id)
        row = self.session.execute(query).scalar_one_or_none()
        return self._job_to_dict(row, "import") if row else None

    def update_import_job(self, job_id: str, patch: Dict[str, Any], *, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, Any]:
        row = self.session.execute(
            select(ImportJobModel).where(
                ImportJobModel.id == job_id,
                *self._job_scope_conditions(ImportJobModel, tenant_id, project_id),
            )
        ).scalar_one_or_none()
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch, tenant_id=tenant_id, project_id=project_id)
        self.session.flush()
        return self._job_to_dict(row, "import")

    def delete_import_job(self, job_id: str, *, tenant_id: str | None = None, project_id: str | None = None) -> bool:
        row = self.session.execute(
            select(ImportJobModel).where(
                ImportJobModel.id == job_id,
                *self._job_scope_conditions(ImportJobModel, tenant_id, project_id),
            )
        ).scalar_one_or_none()
        if row is None:
            return False
        self.session.execute(
            delete(IngestionCheckpointModel).where(
                IngestionCheckpointModel.job_id == job_id,
                IngestionCheckpointModel.tenant_id == row.tenant_id,
                IngestionCheckpointModel.project_id == row.project_id,
            )
        )
        self.session.delete(row)
        self.session.flush()
        return True

    def create_prediction_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._metadata(payload.get("tenant_id"), payload.get("project_id"))
        spec = self._augment_payload(payload.get("spec", {}), tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        row = PredictionJobModel(
            id=payload["id"],
            tenant_id=metadata["tenant_id"],
            project_id=metadata["project_id"],
            import_job_id=payload["import_job_id"],
            status=payload["status"],
            spec_json=_to_json_text(spec),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
            created_by=metadata["actor_id"],
            updated_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "prediction")

    def list_prediction_jobs(self, *, tenant_id: str | None = None, project_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(PredictionJobModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(PredictionJobModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(PredictionJobModel.project_id == resolved_project_id)
        rows = self.session.execute(query.order_by(desc(PredictionJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "prediction") for row in rows]

    def get_prediction_job(self, job_id: str, *, tenant_id: str | None = None, project_id: str | None = None, include_all_tenants: bool = False) -> Optional[Dict[str, Any]]:
        query = select(PredictionJobModel).where(PredictionJobModel.id == job_id)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(PredictionJobModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(PredictionJobModel.project_id == resolved_project_id)
        row = self.session.execute(query).scalar_one_or_none()
        return self._job_to_dict(row, "prediction") if row else None

    def update_prediction_job(self, job_id: str, patch: Dict[str, Any], *, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, Any]:
        row = self.session.execute(
            select(PredictionJobModel).where(
                PredictionJobModel.id == job_id,
                *self._job_scope_conditions(PredictionJobModel, tenant_id, project_id),
            )
        ).scalar_one_or_none()
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch, tenant_id=tenant_id, project_id=project_id)
        self.session.flush()
        return self._job_to_dict(row, "prediction")

    def delete_prediction_job(self, job_id: str, *, tenant_id: str | None = None, project_id: str | None = None) -> bool:
        row = self.session.execute(
            select(PredictionJobModel).where(
                PredictionJobModel.id == job_id,
                *self._job_scope_conditions(PredictionJobModel, tenant_id, project_id),
            )
        ).scalar_one_or_none()
        if row is None:
            return False
        self.session.execute(
            delete(ExportJobModel).where(
                ExportJobModel.prediction_job_id == job_id,
                ExportJobModel.tenant_id == row.tenant_id,
                ExportJobModel.project_id == row.project_id,
            )
        )
        self.session.delete(row)
        self.session.flush()
        return True

    def create_export_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._metadata(payload.get("tenant_id"), payload.get("project_id"))
        spec = self._augment_payload(payload.get("spec", {}), tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        row = ExportJobModel(
            id=payload["id"],
            tenant_id=metadata["tenant_id"],
            project_id=metadata["project_id"],
            prediction_job_id=payload.get("prediction_job_id"),
            status=payload["status"],
            spec_json=_to_json_text(spec),
            progress_json=_to_json_text(payload.get("progress", {})),
            error=payload.get("error"),
            created_by=metadata["actor_id"],
            updated_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
        )
        self.session.add(row)
        self.session.flush()
        return self._job_to_dict(row, "export")

    def list_export_jobs(self, *, tenant_id: str | None = None, project_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(ExportJobModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ExportJobModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ExportJobModel.project_id == resolved_project_id)
        rows = self.session.execute(query.order_by(desc(ExportJobModel.created_at))).scalars().all()
        return [self._job_to_dict(row, "export") for row in rows]

    def get_export_job(self, job_id: str, *, tenant_id: str | None = None, project_id: str | None = None, include_all_tenants: bool = False) -> Optional[Dict[str, Any]]:
        query = select(ExportJobModel).where(ExportJobModel.id == job_id)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ExportJobModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ExportJobModel.project_id == resolved_project_id)
        row = self.session.execute(query).scalar_one_or_none()
        return self._job_to_dict(row, "export") if row else None

    def update_export_job(self, job_id: str, patch: Dict[str, Any], *, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, Any]:
        row = self.session.execute(
            select(ExportJobModel).where(
                ExportJobModel.id == job_id,
                *self._job_scope_conditions(ExportJobModel, tenant_id, project_id),
            )
        ).scalar_one_or_none()
        if row is None:
            raise KeyError(job_id)
        self._apply_job_patch(row, patch, tenant_id=tenant_id, project_id=project_id)
        self.session.flush()
        return self._job_to_dict(row, "export")

    def upsert_checkpoint(self, payload: Dict[str, Any], *, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id or payload.get("project_id"))
        query = select(IngestionCheckpointModel).where(
            IngestionCheckpointModel.tenant_id == metadata["tenant_id"],
            IngestionCheckpointModel.project_id == metadata["project_id"],
            IngestionCheckpointModel.job_id == payload["job_id"],
            IngestionCheckpointModel.shard_index == int(payload["shard_index"]),
        )
        row = self.session.execute(query).scalar_one_or_none()
        enriched_payload = self._augment_payload(payload, tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = IngestionCheckpointModel(
                tenant_id=metadata["tenant_id"],
                project_id=metadata["project_id"],
                job_id=payload["job_id"],
                shard_index=int(payload["shard_index"]),
                source_name=payload["source_name"],
                status=payload["status"],
                cursor_value=payload.get("cursor"),
                gcs_uri=payload.get("gcs_uri"),
                message_id=payload.get("message_id"),
                payload_json=_to_json_text(enriched_payload),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            row.source_name = payload["source_name"]
            row.status = payload["status"]
            row.cursor_value = payload.get("cursor")
            row.gcs_uri = payload.get("gcs_uri")
            row.message_id = payload.get("message_id")
            enriched_payload["created_by"] = row.created_by
            enriched_payload["updated_by"] = metadata["actor_id"]
            row.payload_json = _to_json_text(enriched_payload)
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._checkpoint_to_dict(row)

    def list_checkpoints(self, job_id: str, *, tenant_id: str | None = None, project_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(IngestionCheckpointModel).where(IngestionCheckpointModel.job_id == job_id)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(IngestionCheckpointModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(IngestionCheckpointModel.project_id == resolved_project_id)
        rows = self.session.execute(
            query.order_by(IngestionCheckpointModel.shard_index.asc())
        ).scalars().all()
        return [self._checkpoint_to_dict(row) for row in rows]

    def record_action(self, action_type: str, resource_type: str, resource_id: Optional[str], payload: Dict[str, Any], *, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        enriched_payload = {
            "tenant_id": metadata["tenant_id"],
            "project_id": metadata["project_id"],
            "actor_id": metadata["actor_id"],
            "correlation_id": metadata["correlation_id"],
            **payload,
        }
        row = ActionHistoryModel(
            tenant_id=metadata["tenant_id"],
            project_id=metadata["project_id"],
            actor_id=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
            action_type=action_type,
            resource_type=resource_type,
            resource_id=resource_id,
            payload_json=_to_json_text(enriched_payload),
        )
        self.session.add(row)
        self.session.flush()
        return self._action_to_dict(row)

    def list_actions(self, limit: int = 200, *, tenant_id: str | None = None, project_id: str | None = None, include_all_tenants: bool = False) -> List[Dict[str, Any]]:
        query = select(ActionHistoryModel)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ActionHistoryModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ActionHistoryModel.project_id == resolved_project_id)
        rows = self.session.execute(
            query.order_by(desc(ActionHistoryModel.created_at)).limit(max(1, int(limit)))
        ).scalars().all()
        return [self._action_to_dict(row) for row in rows]

    def get_experiment_config(self, key: str = "default", *, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, Any]:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id, fallback_to_bootstrap=True)
        resolved_project_id = self._resolve_project_id(project_id, fallback_to_bootstrap=True)
        row = self.session.execute(
            select(ExperimentConfigModel).where(
                ExperimentConfigModel.tenant_id == resolved_tenant_id,
                ExperimentConfigModel.project_id == resolved_project_id,
                ExperimentConfigModel.config_key == key,
            )
        ).scalar_one_or_none()
        if row is None:
            if key != "default":
                return {}
            return {
                "experiment_id": "churn_engagement_v1",
                "enabled": True,
                "holdout_pct": 0.10,
                "tenant_id": resolved_tenant_id,
                "project_id": resolved_project_id,
            }
        return _from_json_text(row.config_json)

    def save_experiment_config(self, config: Dict[str, Any], key: str = "default", *, tenant_id: str | None = None, project_id: str | None = None) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        row = self.session.execute(
            select(ExperimentConfigModel).where(
                ExperimentConfigModel.tenant_id == metadata["tenant_id"],
                ExperimentConfigModel.project_id == metadata["project_id"],
                ExperimentConfigModel.config_key == key,
            )
        ).scalar_one_or_none()
        payload = self._augment_payload(config, tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = ExperimentConfigModel(
                tenant_id=metadata["tenant_id"],
                project_id=metadata["project_id"],
                config_key=key,
                config_json=_to_json_text(payload),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            payload["created_by"] = row.created_by
            payload["updated_by"] = metadata["actor_id"]
            row.config_json = _to_json_text(payload)
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return _from_json_text(row.config_json)

    def get_resource(
        self,
        resource_type: str,
        resource_id: str,
        *,
        tenant_id: str | None = None,
        project_id: str | None = None,
        include_all_tenants: bool = False,
    ) -> Optional[Dict[str, Any]]:
        query = select(ControlPlaneResourceModel).where(
            ControlPlaneResourceModel.resource_type == resource_type,
            ControlPlaneResourceModel.resource_id == resource_id,
        )
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ControlPlaneResourceModel.project_id == resolved_project_id)
        row = self.session.execute(query).scalar_one_or_none()
        return self._resource_to_dict(row) if row else None

    def list_resources(
        self,
        resource_type: str,
        *,
        name: str | None = None,
        tenant_id: str | None = None,
        project_id: str | None = None,
        include_all_tenants: bool = False,
    ) -> List[Dict[str, Any]]:
        query = select(ControlPlaneResourceModel).where(ControlPlaneResourceModel.resource_type == resource_type)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ControlPlaneResourceModel.project_id == resolved_project_id)
        if name is not None:
            query = query.where(ControlPlaneResourceModel.name == name)
        rows = self.session.execute(
            query.order_by(ControlPlaneResourceModel.updated_at.desc())
        ).scalars().all()
        return [self._resource_to_dict(row) for row in rows]

    def upsert_resource(
        self,
        resource_type: str,
        resource_id: str,
        *,
        status: str,
        payload: Dict[str, Any],
        name: str | None = None,
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        row = self._get_resource_row(
            resource_type,
            resource_id,
            tenant_id=metadata["tenant_id"],
            project_id=metadata["project_id"],
        )
        enriched_payload = self._augment_payload(payload, tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            pending_row = ControlPlaneResourceModel(
                tenant_id=metadata["tenant_id"],
                project_id=metadata["project_id"],
                resource_type=resource_type,
                resource_id=resource_id,
                name=name,
                status=status,
                payload_json=_to_json_text(enriched_payload),
                created_by=metadata["actor_id"],
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            try:
                with self.session.begin_nested():
                    self.session.add(pending_row)
                    self.session.flush()
                row = pending_row
            except IntegrityError:
                if pending_row in self.session:
                    self.session.expunge(pending_row)
                row = self._get_resource_row(
                    resource_type,
                    resource_id,
                    tenant_id=metadata["tenant_id"],
                    project_id=metadata["project_id"],
                )
                if row is None:
                    raise
                enriched_payload["created_by"] = row.created_by
                enriched_payload["updated_by"] = metadata["actor_id"]
                row.name = name if name is not None else row.name
                row.status = status
                row.payload_json = _to_json_text(enriched_payload)
                row.updated_by = metadata["actor_id"]
                row.correlation_id = metadata["correlation_id"]
                row.updated_at = datetime.utcnow()
        else:
            enriched_payload["created_by"] = row.created_by
            enriched_payload["updated_by"] = metadata["actor_id"]
            row.name = name if name is not None else row.name
            row.status = status
            row.payload_json = _to_json_text(enriched_payload)
            row.updated_by = metadata["actor_id"]
            row.correlation_id = metadata["correlation_id"]
            row.updated_at = datetime.utcnow()
        self.session.flush()
        return self._resource_to_dict(row)

    def delete_resource(self, resource_type: str, resource_id: str, *, tenant_id: str | None = None, project_id: str | None = None) -> bool:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id)
        resolved_project_id = self._resolve_project_id(project_id)
        query = select(ControlPlaneResourceModel).where(
            ControlPlaneResourceModel.resource_type == resource_type,
            ControlPlaneResourceModel.resource_id == resource_id,
        )
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ControlPlaneResourceModel.project_id == resolved_project_id)
        row = self.session.execute(query).scalar_one_or_none()
        if row is None:
            return False
        self.session.execute(
            delete(ControlPlaneResourceVersionModel).where(
                ControlPlaneResourceVersionModel.tenant_id == row.tenant_id,
                ControlPlaneResourceVersionModel.project_id == row.project_id,
                ControlPlaneResourceVersionModel.resource_type == resource_type,
                ControlPlaneResourceVersionModel.resource_id == resource_id,
            )
        )
        self.session.execute(
            delete(ControlPlaneResourceEventModel).where(
                ControlPlaneResourceEventModel.tenant_id == row.tenant_id,
                ControlPlaneResourceEventModel.project_id == row.project_id,
                ControlPlaneResourceEventModel.resource_type == resource_type,
                ControlPlaneResourceEventModel.resource_id == resource_id,
            )
        )
        self.session.delete(row)
        self.session.flush()
        return True

    def create_resource_version(
        self,
        resource_type: str,
        resource_id: str,
        *,
        version: int,
        payload: Dict[str, Any],
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        row = self.session.execute(
            select(ControlPlaneResourceVersionModel).where(
                ControlPlaneResourceVersionModel.tenant_id == metadata["tenant_id"],
                ControlPlaneResourceVersionModel.project_id == metadata["project_id"],
                ControlPlaneResourceVersionModel.resource_type == resource_type,
                ControlPlaneResourceVersionModel.resource_id == resource_id,
                ControlPlaneResourceVersionModel.version == int(version),
            )
        ).scalar_one_or_none()
        enriched_payload = self._augment_payload(payload, tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        if row is None:
            row = ControlPlaneResourceVersionModel(
                tenant_id=metadata["tenant_id"],
                project_id=metadata["project_id"],
                resource_type=resource_type,
                resource_id=resource_id,
                version=int(version),
                payload_json=_to_json_text(enriched_payload),
                created_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            self.session.add(row)
        else:
            enriched_payload["created_by"] = row.created_by
            row.payload_json = _to_json_text(enriched_payload)
            row.correlation_id = metadata["correlation_id"]
        self.session.flush()
        return self._resource_version_to_dict(row)

    def list_resource_versions(
        self,
        resource_type: str,
        resource_id: str,
        *,
        tenant_id: str | None = None,
        project_id: str | None = None,
        include_all_tenants: bool = False,
    ) -> List[Dict[str, Any]]:
        query = select(ControlPlaneResourceVersionModel).where(
            ControlPlaneResourceVersionModel.resource_type == resource_type,
            ControlPlaneResourceVersionModel.resource_id == resource_id,
        )
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceVersionModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ControlPlaneResourceVersionModel.project_id == resolved_project_id)
        rows = self.session.execute(
            query.order_by(ControlPlaneResourceVersionModel.version.desc())
        ).scalars().all()
        return [self._resource_version_to_dict(row) for row in rows]

    def record_resource_event(
        self,
        resource_type: str,
        resource_id: str,
        *,
        event_type: str,
        payload: Dict[str, Any],
        tenant_id: str | None = None,
        project_id: str | None = None,
    ) -> Dict[str, Any]:
        metadata = self._metadata(tenant_id, project_id)
        enriched_payload = self._augment_payload(payload, tenant_id=metadata["tenant_id"], project_id=metadata["project_id"], created_by=metadata["actor_id"], updated_by=metadata["actor_id"], correlation_id=metadata["correlation_id"])
        row = ControlPlaneResourceEventModel(
            tenant_id=metadata["tenant_id"],
            project_id=metadata["project_id"],
            resource_type=resource_type,
            resource_id=resource_id,
            event_type=event_type,
            payload_json=_to_json_text(enriched_payload),
            created_by=metadata["actor_id"],
            correlation_id=metadata["correlation_id"],
        )
        self.session.add(row)
        self.session.flush()
        return self._resource_event_to_dict(row)

    def list_resource_events(
        self,
        resource_type: str,
        resource_id: str | None = None,
        *,
        event_type: str | None = None,
        limit: int = 200,
        tenant_id: str | None = None,
        project_id: str | None = None,
        include_all_tenants: bool = False,
    ) -> List[Dict[str, Any]]:
        query = select(ControlPlaneResourceEventModel).where(ControlPlaneResourceEventModel.resource_type == resource_type)
        resolved_tenant_id = None if include_all_tenants else self._resolve_tenant_id(tenant_id)
        resolved_project_id = None if include_all_tenants else self._resolve_project_id(project_id)
        if resolved_tenant_id:
            query = query.where(ControlPlaneResourceEventModel.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            query = query.where(ControlPlaneResourceEventModel.project_id == resolved_project_id)
        if resource_id is not None:
            query = query.where(ControlPlaneResourceEventModel.resource_id == resource_id)
        if event_type is not None:
            query = query.where(ControlPlaneResourceEventModel.event_type == event_type)
        rows = self.session.execute(
            query.order_by(desc(ControlPlaneResourceEventModel.created_at)).limit(max(1, int(limit)))
        ).scalars().all()
        return [self._resource_event_to_dict(row) for row in rows]

    def _job_scope_conditions(self, model: Any, tenant_id: str | None, project_id: str | None) -> List[Any]:
        resolved_tenant_id = self._resolve_tenant_id(tenant_id)
        resolved_project_id = self._resolve_project_id(project_id)
        conditions: List[Any] = []
        if resolved_tenant_id:
            conditions.append(model.tenant_id == resolved_tenant_id)
        if resolved_project_id:
            conditions.append(model.project_id == resolved_project_id)
        return conditions

    def _tenant_to_dict(self, row: TenantModel) -> Dict[str, Any]:
        return {
            "organization_id": row.tenant_id,
            "tenant_id": row.tenant_id,
            "name": row.name,
            "status": row.status,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _platform_user_to_dict(self, row: PlatformUserModel) -> Dict[str, Any]:
        return {
            "user_id": row.user_id,
            "email": row.email,
            "display_name": row.display_name,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _tenant_membership_to_dict(self, row: TenantMembershipModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "organization_id": row.tenant_id,
            "tenant_id": row.tenant_id,
            "user_id": row.user_id,
            "role": row.role,
            "status": row.status,
            "joined_at": row.created_at.isoformat(),
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _project_to_dict(self, row: ProjectModel) -> Dict[str, Any]:
        return {
            "organization_id": row.tenant_id,
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "name": row.name,
            "description": row.description or "",
            "status": row.status,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _project_membership_to_dict(self, row: ProjectMembershipModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "organization_id": row.tenant_id,
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "user_id": row.user_id,
            "role": row.role,
            "status": row.status,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _project_invite_to_dict(self, row: ProjectInviteModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "organization_id": row.tenant_id,
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "invite_code": row.invite_code,
            "invite_url": f"/?invite_code={row.invite_code}&organization_id={row.tenant_id}&project_id={row.project_id}",
            "email": row.email,
            "display_name": row.display_name,
            "org_role": row.org_role,
            "project_role": row.project_role,
            "status": row.status,
            "created_by": row.created_by,
            "redeemed_by": row.redeemed_by,
            "correlation_id": row.correlation_id,
            "expires_at": row.expires_at.isoformat() if row.expires_at else None,
            "redeemed_at": row.redeemed_at.isoformat() if row.redeemed_at else None,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _organization_invite_to_dict(self, row: OrganizationInviteModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "organization_id": row.tenant_id,
            "tenant_id": row.tenant_id,
            "invite_code": row.invite_code,
            "invite_url": f"/?invite_code={row.invite_code}&organization_id={row.tenant_id}",
            "email": row.email,
            "display_name": row.display_name,
            "role": row.role,
            "status": row.status,
            "created_by": row.created_by,
            "redeemed_by": row.redeemed_by,
            "correlation_id": row.correlation_id,
            "expires_at": row.expires_at.isoformat() if row.expires_at else None,
            "redeemed_at": row.redeemed_at.isoformat() if row.redeemed_at else None,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _connector_to_dict(self, row: ConnectorConfigModel) -> Dict[str, Any]:
        payload = _from_json_text(row.config_json)
        return {
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "connector_id": row.connector_id,
            "name": row.name,
            "type": row.connector_type,
            "config": payload,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _job_to_dict(self, row: Any, job_type: str) -> Dict[str, Any]:
        spec = _from_json_text(row.spec_json)
        progress = _from_json_text(row.progress_json)
        payload = {
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "id": row.id,
            "type": job_type,
            "status": row.status,
            "spec": spec,
            "progress": progress,
            "error": row.error,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }
        if hasattr(row, "import_job_id"):
            payload["import_job_id"] = getattr(row, "import_job_id")
        if hasattr(row, "source_name"):
            payload["source_name"] = getattr(row, "source_name")
        return payload

    def _checkpoint_to_dict(self, row: IngestionCheckpointModel) -> Dict[str, Any]:
        payload = _from_json_text(row.payload_json)
        payload.setdefault("tenant_id", row.tenant_id)
        payload.setdefault("project_id", row.project_id)
        payload.setdefault("job_id", row.job_id)
        payload.setdefault("shard_index", row.shard_index)
        payload.setdefault("source_name", row.source_name)
        payload.setdefault("status", row.status)
        payload.setdefault("cursor", row.cursor_value)
        payload.setdefault("gcs_uri", row.gcs_uri)
        payload.setdefault("message_id", row.message_id)
        payload.setdefault("created_by", row.created_by)
        payload.setdefault("updated_by", row.updated_by)
        payload.setdefault("correlation_id", row.correlation_id)
        payload["created_at"] = row.created_at.isoformat()
        payload["updated_at"] = row.updated_at.isoformat()
        return payload

    def _action_to_dict(self, row: ActionHistoryModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "actor_id": row.actor_id,
            "correlation_id": row.correlation_id,
            "action_type": row.action_type,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "payload": _from_json_text(row.payload_json),
            "created_at": row.created_at.isoformat(),
        }

    def _resource_to_dict(self, row: ControlPlaneResourceModel) -> Dict[str, Any]:
        payload = _from_json_text(row.payload_json)
        return {
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "name": row.name,
            "status": row.status,
            "payload": payload,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
        }

    def _resource_version_to_dict(self, row: ControlPlaneResourceVersionModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "version": row.version,
            "payload": _from_json_text(row.payload_json),
            "created_by": row.created_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
        }

    def _resource_event_to_dict(self, row: ControlPlaneResourceEventModel) -> Dict[str, Any]:
        return {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "project_id": row.project_id,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "event_type": row.event_type,
            "payload": _from_json_text(row.payload_json),
            "created_by": row.created_by,
            "correlation_id": row.correlation_id,
            "created_at": row.created_at.isoformat(),
        }

    def _apply_job_patch(self, row: Any, patch: Dict[str, Any], *, tenant_id: str | None = None, project_id: str | None = None) -> None:
        metadata = self._metadata(tenant_id or getattr(row, "tenant_id", None), project_id or getattr(row, "project_id", None))
        if "status" in patch:
            row.status = patch["status"]
        if "import_job_id" in patch and hasattr(row, "import_job_id"):
            row.import_job_id = patch["import_job_id"]
        if "source_name" in patch and hasattr(row, "source_name"):
            row.source_name = patch["source_name"]
        if "spec" in patch:
            spec_payload = self._augment_payload(
                patch["spec"],
                tenant_id=metadata["tenant_id"],
                project_id=metadata["project_id"],
                created_by=getattr(row, "created_by", "system"),
                updated_by=metadata["actor_id"],
                correlation_id=metadata["correlation_id"],
            )
            row.spec_json = _to_json_text(spec_payload)
        if "progress" in patch:
            row.progress_json = _to_json_text(patch["progress"])
        if "error" in patch:
            row.error = patch["error"]
        row.updated_by = metadata["actor_id"]
        row.correlation_id = metadata["correlation_id"]
        row.updated_at = datetime.utcnow()
