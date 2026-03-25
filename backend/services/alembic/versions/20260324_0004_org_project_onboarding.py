"""add org space + project onboarding schema

Revision ID: 20260324_0004
Revises: 20260322_0003
Create Date: 2026-03-24 00:04:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260324_0004"
down_revision = "20260322_0003"
branch_labels = None
depends_on = None


BOOTSTRAP_PROJECT_ID = "default"
BOOTSTRAP_PROJECT_NAME = "Default Project"
SYSTEM_ACTOR = "system"
SQLITE_BATCH_NAMING = {"pk": "pk_%(table_name)s"}


def upgrade() -> None:
    op.create_table(
        "projects_v1",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("project_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=64), nullable=False, server_default="active"),
        sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR),
        sa.Column("updated_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR),
        sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("tenant_id", "project_id", name="uq_projects_v1_tenant_project"),
    )
    op.create_index("ix_projects_v1_tenant_id", "projects_v1", ["tenant_id"], unique=False)
    op.create_index("ix_projects_v1_project_id", "projects_v1", ["project_id"], unique=False)
    op.create_index("ix_projects_v1_name", "projects_v1", ["name"], unique=False)
    op.create_index("ix_projects_v1_status", "projects_v1", ["status"], unique=False)
    op.create_index("ix_projects_v1_created_by", "projects_v1", ["created_by"], unique=False)
    op.create_index("ix_projects_v1_updated_by", "projects_v1", ["updated_by"], unique=False)
    op.create_index("ix_projects_v1_correlation_id", "projects_v1", ["correlation_id"], unique=False)

    op.create_table(
        "project_memberships_v1",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("project_id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("tenant_id", "project_id", "user_id", name="uq_project_membership_tenant_project_user"),
    )
    op.create_index("ix_project_memberships_v1_tenant_id", "project_memberships_v1", ["tenant_id"], unique=False)
    op.create_index("ix_project_memberships_v1_project_id", "project_memberships_v1", ["project_id"], unique=False)
    op.create_index("ix_project_memberships_v1_user_id", "project_memberships_v1", ["user_id"], unique=False)
    op.create_index("ix_project_memberships_v1_role", "project_memberships_v1", ["role"], unique=False)
    op.create_index("ix_project_memberships_v1_status", "project_memberships_v1", ["status"], unique=False)

    op.create_table(
        "project_invites_v1",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("project_id", sa.String(length=64), nullable=False),
        sa.Column("invite_code", sa.String(length=128), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("display_name", sa.String(length=255), nullable=True),
        sa.Column("org_role", sa.String(length=32), nullable=False, server_default="member"),
        sa.Column("project_role", sa.String(length=32), nullable=False, server_default="operator"),
        sa.Column("status", sa.String(length=64), nullable=False, server_default="pending"),
        sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR),
        sa.Column("redeemed_by", sa.String(length=128), nullable=True),
        sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("redeemed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("invite_code", name="uq_project_invites_v1_invite_code"),
    )
    op.create_index("ix_project_invites_v1_tenant_id", "project_invites_v1", ["tenant_id"], unique=False)
    op.create_index("ix_project_invites_v1_project_id", "project_invites_v1", ["project_id"], unique=False)
    op.create_index("ix_project_invites_v1_invite_code", "project_invites_v1", ["invite_code"], unique=False)
    op.create_index("ix_project_invites_v1_email", "project_invites_v1", ["email"], unique=False)
    op.create_index("ix_project_invites_v1_org_role", "project_invites_v1", ["org_role"], unique=False)
    op.create_index("ix_project_invites_v1_project_role", "project_invites_v1", ["project_role"], unique=False)
    op.create_index("ix_project_invites_v1_status", "project_invites_v1", ["status"], unique=False)
    op.create_index("ix_project_invites_v1_created_by", "project_invites_v1", ["created_by"], unique=False)
    op.create_index("ix_project_invites_v1_redeemed_by", "project_invites_v1", ["redeemed_by"], unique=False)
    op.create_index("ix_project_invites_v1_correlation_id", "project_invites_v1", ["correlation_id"], unique=False)

    bind = op.get_bind()
    bind.execute(
        sa.text(
            """
            INSERT INTO projects_v1 (
                tenant_id, project_id, name, description, status, created_by, updated_by, correlation_id, created_at, updated_at
            )
            SELECT tenant_id, :project_id, :project_name, '', 'active', :system_actor, :system_actor, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            FROM tenants_v1
            """
        ),
        {"project_id": BOOTSTRAP_PROJECT_ID, "project_name": BOOTSTRAP_PROJECT_NAME, "system_actor": SYSTEM_ACTOR},
    )
    bind.execute(
        sa.text(
            """
            INSERT INTO project_memberships_v1 (
                tenant_id, project_id, user_id, role, status, created_at, updated_at
            )
            SELECT tenant_id, :project_id, user_id,
                   CASE
                       WHEN lower(role) IN ('admin', 'analyst', 'operator') THEN lower(role)
                       ELSE 'operator'
                   END,
                   status, created_at, updated_at
            FROM tenant_memberships_v1
            """
        ),
        {"project_id": BOOTSTRAP_PROJECT_ID},
    )

    with op.batch_alter_table("connector_configs", recreate="always") as batch:
        batch.drop_constraint("uq_connector_configs_tenant_connector_id", type_="unique")
        batch.drop_constraint("uq_connector_configs_tenant_name", type_="unique")
        batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
        batch.create_index("ix_connector_configs_project_id", ["project_id"], unique=False)
        batch.create_unique_constraint("uq_connector_configs_tenant_project_connector_id", ["tenant_id", "project_id", "connector_id"])
        batch.create_unique_constraint("uq_connector_configs_tenant_project_name", ["tenant_id", "project_id", "name"])

    with op.batch_alter_table("field_mappings_v2", recreate="always", naming_convention=SQLITE_BATCH_NAMING) as batch:
        batch.drop_constraint("uq_field_mappings_tenant_connector", type_="unique")
        batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
        batch.create_index("ix_field_mappings_v2_project_id", ["project_id"], unique=False)
        batch.create_unique_constraint("uq_field_mappings_tenant_project_connector", ["tenant_id", "project_id", "connector_name"])

    for table_name in ("import_jobs_v2", "prediction_jobs_v2", "export_jobs_v2"):
        with op.batch_alter_table(table_name, recreate="always") as batch:
            batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
            batch.create_index(f"ix_{table_name}_project_id", ["project_id"], unique=False)

    with op.batch_alter_table("experiment_configs", recreate="always", naming_convention=SQLITE_BATCH_NAMING) as batch:
        batch.drop_constraint("uq_experiment_configs_tenant_key", type_="unique")
        batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
        batch.create_index("ix_experiment_configs_project_id", ["project_id"], unique=False)
        batch.create_unique_constraint("uq_experiment_configs_tenant_project_key", ["tenant_id", "project_id", "config_key"])

    with op.batch_alter_table("action_history_v2", recreate="always") as batch:
        batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
        batch.create_index("ix_action_history_v2_project_id", ["project_id"], unique=False)

    with op.batch_alter_table("ingestion_checkpoints_v2", recreate="always") as batch:
        batch.drop_constraint("uq_ingestion_checkpoint_tenant_job_shard", type_="unique")
        batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
        batch.create_index("ix_ingestion_checkpoints_v2_project_id", ["project_id"], unique=False)
        batch.create_unique_constraint("uq_ingestion_checkpoint_tenant_project_job_shard", ["tenant_id", "project_id", "job_id", "shard_index"])

    with op.batch_alter_table("control_plane_resources_v1", recreate="always") as batch:
        batch.drop_constraint("uq_control_plane_resource_tenant_type_id", type_="unique")
        batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
        batch.create_index("ix_control_plane_resources_v1_project_id", ["project_id"], unique=False)
        batch.create_unique_constraint("uq_control_plane_resource_tenant_project_type_id", ["tenant_id", "project_id", "resource_type", "resource_id"])

    with op.batch_alter_table("control_plane_resource_versions_v1", recreate="always") as batch:
        batch.drop_constraint("uq_control_plane_resource_tenant_version", type_="unique")
        batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
        batch.create_index("ix_control_plane_resource_versions_v1_project_id", ["project_id"], unique=False)
        batch.create_unique_constraint("uq_control_plane_resource_tenant_project_version", ["tenant_id", "project_id", "resource_type", "resource_id", "version"])

    with op.batch_alter_table("control_plane_resource_events_v1", recreate="always") as batch:
        batch.add_column(sa.Column("project_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_PROJECT_ID))
        batch.create_index("ix_control_plane_resource_events_v1_project_id", ["project_id"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("control_plane_resource_events_v1", recreate="always") as batch:
        batch.drop_index("ix_control_plane_resource_events_v1_project_id")
        batch.drop_column("project_id")

    with op.batch_alter_table("control_plane_resource_versions_v1", recreate="always") as batch:
        batch.drop_constraint("uq_control_plane_resource_tenant_project_version", type_="unique")
        batch.drop_index("ix_control_plane_resource_versions_v1_project_id")
        batch.drop_column("project_id")
        batch.create_unique_constraint("uq_control_plane_resource_tenant_version", ["tenant_id", "resource_type", "resource_id", "version"])

    with op.batch_alter_table("control_plane_resources_v1", recreate="always") as batch:
        batch.drop_constraint("uq_control_plane_resource_tenant_project_type_id", type_="unique")
        batch.drop_index("ix_control_plane_resources_v1_project_id")
        batch.drop_column("project_id")
        batch.create_unique_constraint("uq_control_plane_resource_tenant_type_id", ["tenant_id", "resource_type", "resource_id"])

    with op.batch_alter_table("ingestion_checkpoints_v2", recreate="always") as batch:
        batch.drop_constraint("uq_ingestion_checkpoint_tenant_project_job_shard", type_="unique")
        batch.drop_index("ix_ingestion_checkpoints_v2_project_id")
        batch.drop_column("project_id")
        batch.create_unique_constraint("uq_ingestion_checkpoint_tenant_job_shard", ["tenant_id", "job_id", "shard_index"])

    with op.batch_alter_table("action_history_v2", recreate="always") as batch:
        batch.drop_index("ix_action_history_v2_project_id")
        batch.drop_column("project_id")

    with op.batch_alter_table("experiment_configs", recreate="always", naming_convention=SQLITE_BATCH_NAMING) as batch:
        batch.drop_constraint("uq_experiment_configs_tenant_project_key", type_="unique")
        batch.drop_index("ix_experiment_configs_project_id")
        batch.drop_column("project_id")
        batch.create_unique_constraint("uq_experiment_configs_tenant_key", ["tenant_id", "config_key"])

    for table_name in ("export_jobs_v2", "prediction_jobs_v2", "import_jobs_v2"):
        with op.batch_alter_table(table_name, recreate="always") as batch:
            batch.drop_index(f"ix_{table_name}_project_id")
            batch.drop_column("project_id")

    with op.batch_alter_table("field_mappings_v2", recreate="always", naming_convention=SQLITE_BATCH_NAMING) as batch:
        batch.drop_constraint("uq_field_mappings_tenant_project_connector", type_="unique")
        batch.drop_index("ix_field_mappings_v2_project_id")
        batch.drop_column("project_id")
        batch.create_unique_constraint("uq_field_mappings_tenant_connector", ["tenant_id", "connector_name"])

    with op.batch_alter_table("connector_configs", recreate="always") as batch:
        batch.drop_constraint("uq_connector_configs_tenant_project_connector_id", type_="unique")
        batch.drop_constraint("uq_connector_configs_tenant_project_name", type_="unique")
        batch.drop_index("ix_connector_configs_project_id")
        batch.drop_column("project_id")
        batch.create_unique_constraint("uq_connector_configs_tenant_connector_id", ["tenant_id", "connector_id"])
        batch.create_unique_constraint("uq_connector_configs_tenant_name", ["tenant_id", "name"])

    for table_name in ("project_invites_v1", "project_memberships_v1", "projects_v1"):
        op.drop_table(table_name)
