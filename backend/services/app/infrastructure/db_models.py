from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base


class TenantModel(Base):
    __tablename__ = "tenants_v1"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="active", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PlatformUserModel(Base):
    __tablename__ = "platform_users_v1"

    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class TenantMembershipModel(Base):
    __tablename__ = "tenant_memberships_v1"
    __table_args__ = (UniqueConstraint("tenant_id", "user_id", name="uq_tenant_membership_tenant_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[str] = mapped_column(String(128), index=True)
    role: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="active", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProjectModel(Base):
    __tablename__ = "projects_v1"
    __table_args__ = (UniqueConstraint("tenant_id", "project_id", name="uq_projects_v1_tenant_project"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="active", index=True)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProjectMembershipModel(Base):
    __tablename__ = "project_memberships_v1"
    __table_args__ = (UniqueConstraint("tenant_id", "project_id", "user_id", name="uq_project_membership_tenant_project_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    user_id: Mapped[str] = mapped_column(String(128), index=True)
    role: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="active", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ProjectInviteModel(Base):
    __tablename__ = "project_invites_v1"
    __table_args__ = (UniqueConstraint("invite_code", name="uq_project_invites_v1_invite_code"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    invite_code: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    org_role: Mapped[str] = mapped_column(String(32), nullable=False, default="member", index=True)
    project_role: Mapped[str] = mapped_column(String(32), nullable=False, default="operator", index=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="pending", index=True)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    redeemed_by: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    redeemed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class OrganizationInviteModel(Base):
    __tablename__ = "organization_invites_v1"
    __table_args__ = (
        UniqueConstraint("invite_code", name="uq_organization_invites_v1_invite_code"),
        UniqueConstraint("tenant_id", "email", name="uq_organization_invites_v1_tenant_email"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    invite_code: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    role: Mapped[str] = mapped_column(String(32), nullable=False, default="member", index=True)
    status: Mapped[str] = mapped_column(String(64), nullable=False, default="pending", index=True)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    redeemed_by: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    redeemed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ConnectorConfigModel(Base):
    __tablename__ = "connector_configs"
    __table_args__ = (
        UniqueConstraint("tenant_id", "project_id", "connector_id", name="uq_connector_configs_tenant_project_connector_id"),
        UniqueConstraint("tenant_id", "project_id", "name", name="uq_connector_configs_tenant_project_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    connector_id: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    connector_type: Mapped[str] = mapped_column(String(128), index=True)
    config_json: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FieldMappingModel(Base):
    __tablename__ = "field_mappings_v2"
    __table_args__ = (UniqueConstraint("tenant_id", "project_id", "connector_name", name="uq_field_mappings_tenant_project_connector"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    connector_name: Mapped[str] = mapped_column(String(255), index=True)
    mapping_json: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ImportJobModel(Base):
    __tablename__ = "import_jobs_v2"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    source_name: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    spec_json: Mapped[str] = mapped_column(Text)
    progress_json: Mapped[str] = mapped_column(Text, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PredictionJobModel(Base):
    __tablename__ = "prediction_jobs_v2"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    import_job_id: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    spec_json: Mapped[str] = mapped_column(Text)
    progress_json: Mapped[str] = mapped_column(Text, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ExportJobModel(Base):
    __tablename__ = "export_jobs_v2"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    prediction_job_id: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    spec_json: Mapped[str] = mapped_column(Text)
    progress_json: Mapped[str] = mapped_column(Text, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ExperimentConfigModel(Base):
    __tablename__ = "experiment_configs"
    __table_args__ = (UniqueConstraint("tenant_id", "project_id", "config_key", name="uq_experiment_configs_tenant_project_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    config_key: Mapped[str] = mapped_column(String(64), index=True)
    config_json: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ActionHistoryModel(Base):
    __tablename__ = "action_history_v2"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    actor_id: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    action_type: Mapped[str] = mapped_column(String(128), index=True)
    resource_type: Mapped[str] = mapped_column(String(128), index=True)
    resource_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class IngestionCheckpointModel(Base):
    __tablename__ = "ingestion_checkpoints_v2"
    __table_args__ = (UniqueConstraint("tenant_id", "project_id", "job_id", "shard_index", name="uq_ingestion_checkpoint_tenant_project_job_shard"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    job_id: Mapped[str] = mapped_column(String(64), index=True)
    shard_index: Mapped[int] = mapped_column(Integer, index=True)
    source_name: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    cursor_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    gcs_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ControlPlaneResourceModel(Base):
    __tablename__ = "control_plane_resources_v1"
    __table_args__ = (UniqueConstraint("tenant_id", "project_id", "resource_type", "resource_id", name="uq_control_plane_resource_tenant_project_type_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    resource_type: Mapped[str] = mapped_column(String(64), index=True)
    resource_id: Mapped[str] = mapped_column(String(128), index=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(64), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    updated_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ControlPlaneResourceVersionModel(Base):
    __tablename__ = "control_plane_resource_versions_v1"
    __table_args__ = (
        UniqueConstraint("tenant_id", "project_id", "resource_type", "resource_id", "version", name="uq_control_plane_resource_tenant_project_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    resource_type: Mapped[str] = mapped_column(String(64), index=True)
    resource_id: Mapped[str] = mapped_column(String(128), index=True)
    version: Mapped[int] = mapped_column(Integer, index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class ControlPlaneResourceEventModel(Base):
    __tablename__ = "control_plane_resource_events_v1"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    resource_type: Mapped[str] = mapped_column(String(64), index=True)
    resource_id: Mapped[str] = mapped_column(String(128), index=True)
    event_type: Mapped[str] = mapped_column(String(128), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(128), default="system", index=True)
    correlation_id: Mapped[str] = mapped_column(String(128), default="", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class MockWarehouseRowModel(Base):
    __tablename__ = "mock_warehouse_rows_v1"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    target_name: Mapped[str] = mapped_column(String(64), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
