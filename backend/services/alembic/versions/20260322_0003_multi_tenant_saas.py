"""add multi-tenant SaaS production-readiness schema

Revision ID: 20260322_0003
Revises: 20260310_0002
Create Date: 2026-03-22 00:03:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260322_0003"
down_revision = "20260310_0002"
branch_labels = None
depends_on = None


BOOTSTRAP_TENANT_ID = "default"
BOOTSTRAP_TENANT_NAME = "Default Tenant"
SYSTEM_ACTOR = "system"
SQLITE_BATCH_NAMING = {"pk": "pk_%(table_name)s"}


def upgrade() -> None:
    op.create_table(
        "tenants_v1",
        sa.Column("tenant_id", sa.String(length=64), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_tenants_v1_name", "tenants_v1", ["name"], unique=False)
    op.create_index("ix_tenants_v1_status", "tenants_v1", ["status"], unique=False)

    op.create_table(
        "platform_users_v1",
        sa.Column("user_id", sa.String(length=128), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("display_name", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_platform_users_v1_email", "platform_users_v1", ["email"], unique=False)

    op.create_table(
        "tenant_memberships_v1",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("tenant_id", "user_id", name="uq_tenant_membership_tenant_user"),
    )
    op.create_index("ix_tenant_memberships_v1_tenant_id", "tenant_memberships_v1", ["tenant_id"], unique=False)
    op.create_index("ix_tenant_memberships_v1_user_id", "tenant_memberships_v1", ["user_id"], unique=False)
    op.create_index("ix_tenant_memberships_v1_role", "tenant_memberships_v1", ["role"], unique=False)
    op.create_index("ix_tenant_memberships_v1_status", "tenant_memberships_v1", ["status"], unique=False)

    op.get_bind().execute(
        sa.text(
            """
            INSERT INTO tenants_v1 (tenant_id, name, status, created_at, updated_at)
            VALUES (:tenant_id, :name, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
        ),
        {"tenant_id": BOOTSTRAP_TENANT_ID, "name": BOOTSTRAP_TENANT_NAME},
    )

    with op.batch_alter_table("connector_configs", recreate="always") as batch:
        batch.drop_index("ix_connector_configs_name")
        batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
        batch.add_column(sa.Column("connector_id", sa.String(length=64), nullable=True))
        batch.add_column(sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("updated_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
        batch.create_index("ix_connector_configs_name", ["name"], unique=False)
        batch.create_index("ix_connector_configs_tenant_id", ["tenant_id"], unique=False)
        batch.create_index("ix_connector_configs_connector_id", ["connector_id"], unique=False)
        batch.create_index("ix_connector_configs_created_by", ["created_by"], unique=False)
        batch.create_index("ix_connector_configs_updated_by", ["updated_by"], unique=False)
        batch.create_index("ix_connector_configs_correlation_id", ["correlation_id"], unique=False)
        batch.create_unique_constraint("uq_connector_configs_tenant_connector_id", ["tenant_id", "connector_id"])
        batch.create_unique_constraint("uq_connector_configs_tenant_name", ["tenant_id", "name"])
    op.execute(sa.text("UPDATE connector_configs SET connector_id = name WHERE connector_id IS NULL OR connector_id = ''"))

    with op.batch_alter_table("field_mappings_v2", recreate="always", naming_convention=SQLITE_BATCH_NAMING) as batch:
        batch.add_column(sa.Column("id", sa.Integer(), autoincrement=True, nullable=False))
        batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
        batch.add_column(sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("updated_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
        batch.drop_constraint("pk_field_mappings_v2", type_="primary")
        batch.create_primary_key("pk_field_mappings_v2", ["id"])
        batch.create_index("ix_field_mappings_v2_tenant_id", ["tenant_id"], unique=False)
        batch.create_index("ix_field_mappings_v2_connector_name", ["connector_name"], unique=False)
        batch.create_index("ix_field_mappings_v2_created_by", ["created_by"], unique=False)
        batch.create_index("ix_field_mappings_v2_updated_by", ["updated_by"], unique=False)
        batch.create_index("ix_field_mappings_v2_correlation_id", ["correlation_id"], unique=False)
        batch.create_unique_constraint("uq_field_mappings_tenant_connector", ["tenant_id", "connector_name"])

    for table_name in ("import_jobs_v2", "prediction_jobs_v2", "export_jobs_v2"):
        with op.batch_alter_table(table_name, recreate="always") as batch:
            batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
            batch.add_column(sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
            batch.add_column(sa.Column("updated_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
            batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
            batch.create_index(f"ix_{table_name}_tenant_id", ["tenant_id"], unique=False)
            batch.create_index(f"ix_{table_name}_created_by", ["created_by"], unique=False)
            batch.create_index(f"ix_{table_name}_updated_by", ["updated_by"], unique=False)
            batch.create_index(f"ix_{table_name}_correlation_id", ["correlation_id"], unique=False)

    with op.batch_alter_table("experiment_configs", recreate="always", naming_convention=SQLITE_BATCH_NAMING) as batch:
        batch.add_column(sa.Column("id", sa.Integer(), autoincrement=True, nullable=False))
        batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
        batch.add_column(sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("updated_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
        batch.drop_constraint("pk_experiment_configs", type_="primary")
        batch.create_primary_key("pk_experiment_configs", ["id"])
        batch.create_index("ix_experiment_configs_tenant_id", ["tenant_id"], unique=False)
        batch.create_index("ix_experiment_configs_config_key", ["config_key"], unique=False)
        batch.create_index("ix_experiment_configs_created_by", ["created_by"], unique=False)
        batch.create_index("ix_experiment_configs_updated_by", ["updated_by"], unique=False)
        batch.create_index("ix_experiment_configs_correlation_id", ["correlation_id"], unique=False)
        batch.create_unique_constraint("uq_experiment_configs_tenant_key", ["tenant_id", "config_key"])

    with op.batch_alter_table("action_history_v2", recreate="always") as batch:
        batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
        batch.add_column(sa.Column("actor_id", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
        batch.create_index("ix_action_history_v2_tenant_id", ["tenant_id"], unique=False)
        batch.create_index("ix_action_history_v2_actor_id", ["actor_id"], unique=False)
        batch.create_index("ix_action_history_v2_correlation_id", ["correlation_id"], unique=False)
        batch.create_index("ix_action_history_v2_action_type", ["action_type"], unique=False)
        batch.create_index("ix_action_history_v2_resource_type", ["resource_type"], unique=False)
        batch.create_index("ix_action_history_v2_resource_id", ["resource_id"], unique=False)

    with op.batch_alter_table("ingestion_checkpoints_v2", recreate="always") as batch:
        batch.drop_constraint("uq_ingestion_checkpoint_job_shard", type_="unique")
        batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
        batch.add_column(sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("updated_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
        batch.create_index("ix_ingestion_checkpoints_v2_tenant_id", ["tenant_id"], unique=False)
        batch.create_index("ix_ingestion_checkpoints_v2_created_by", ["created_by"], unique=False)
        batch.create_index("ix_ingestion_checkpoints_v2_updated_by", ["updated_by"], unique=False)
        batch.create_index("ix_ingestion_checkpoints_v2_correlation_id", ["correlation_id"], unique=False)
        batch.create_unique_constraint("uq_ingestion_checkpoint_tenant_job_shard", ["tenant_id", "job_id", "shard_index"])

    with op.batch_alter_table("control_plane_resources_v1", recreate="always") as batch:
        batch.drop_constraint("uq_control_plane_resource_type_id", type_="unique")
        batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
        batch.add_column(sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("updated_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
        batch.create_index("ix_control_plane_resources_v1_tenant_id", ["tenant_id"], unique=False)
        batch.create_index("ix_control_plane_resources_v1_created_by", ["created_by"], unique=False)
        batch.create_index("ix_control_plane_resources_v1_updated_by", ["updated_by"], unique=False)
        batch.create_index("ix_control_plane_resources_v1_correlation_id", ["correlation_id"], unique=False)
        batch.create_unique_constraint("uq_control_plane_resource_tenant_type_id", ["tenant_id", "resource_type", "resource_id"])

    with op.batch_alter_table("control_plane_resource_versions_v1", recreate="always") as batch:
        batch.drop_constraint("uq_control_plane_resource_version", type_="unique")
        batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
        batch.add_column(sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
        batch.create_index("ix_control_plane_resource_versions_v1_tenant_id", ["tenant_id"], unique=False)
        batch.create_index("ix_control_plane_resource_versions_v1_created_by", ["created_by"], unique=False)
        batch.create_index("ix_control_plane_resource_versions_v1_correlation_id", ["correlation_id"], unique=False)
        batch.create_unique_constraint("uq_control_plane_resource_tenant_version", ["tenant_id", "resource_type", "resource_id", "version"])

    with op.batch_alter_table("control_plane_resource_events_v1", recreate="always") as batch:
        batch.add_column(sa.Column("tenant_id", sa.String(length=64), nullable=False, server_default=BOOTSTRAP_TENANT_ID))
        batch.add_column(sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR))
        batch.add_column(sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""))
        batch.create_index("ix_control_plane_resource_events_v1_tenant_id", ["tenant_id"], unique=False)
        batch.create_index("ix_control_plane_resource_events_v1_created_by", ["created_by"], unique=False)
        batch.create_index("ix_control_plane_resource_events_v1_correlation_id", ["correlation_id"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("control_plane_resource_events_v1", recreate="always") as batch:
        batch.drop_index("ix_control_plane_resource_events_v1_correlation_id")
        batch.drop_index("ix_control_plane_resource_events_v1_created_by")
        batch.drop_index("ix_control_plane_resource_events_v1_tenant_id")
        batch.drop_column("correlation_id")
        batch.drop_column("created_by")
        batch.drop_column("tenant_id")

    with op.batch_alter_table("control_plane_resource_versions_v1", recreate="always") as batch:
        batch.drop_constraint("uq_control_plane_resource_tenant_version", type_="unique")
        batch.drop_index("ix_control_plane_resource_versions_v1_correlation_id")
        batch.drop_index("ix_control_plane_resource_versions_v1_created_by")
        batch.drop_index("ix_control_plane_resource_versions_v1_tenant_id")
        batch.drop_column("correlation_id")
        batch.drop_column("created_by")
        batch.drop_column("tenant_id")
        batch.create_unique_constraint("uq_control_plane_resource_version", ["resource_type", "resource_id", "version"])

    with op.batch_alter_table("control_plane_resources_v1", recreate="always") as batch:
        batch.drop_constraint("uq_control_plane_resource_tenant_type_id", type_="unique")
        batch.drop_index("ix_control_plane_resources_v1_correlation_id")
        batch.drop_index("ix_control_plane_resources_v1_updated_by")
        batch.drop_index("ix_control_plane_resources_v1_created_by")
        batch.drop_index("ix_control_plane_resources_v1_tenant_id")
        batch.drop_column("correlation_id")
        batch.drop_column("updated_by")
        batch.drop_column("created_by")
        batch.drop_column("tenant_id")
        batch.create_unique_constraint("uq_control_plane_resource_type_id", ["resource_type", "resource_id"])

    with op.batch_alter_table("ingestion_checkpoints_v2", recreate="always") as batch:
        batch.drop_constraint("uq_ingestion_checkpoint_tenant_job_shard", type_="unique")
        batch.drop_index("ix_ingestion_checkpoints_v2_correlation_id")
        batch.drop_index("ix_ingestion_checkpoints_v2_updated_by")
        batch.drop_index("ix_ingestion_checkpoints_v2_created_by")
        batch.drop_index("ix_ingestion_checkpoints_v2_tenant_id")
        batch.drop_column("correlation_id")
        batch.drop_column("updated_by")
        batch.drop_column("created_by")
        batch.drop_column("tenant_id")
        batch.create_unique_constraint("uq_ingestion_checkpoint_job_shard", ["job_id", "shard_index"])

    with op.batch_alter_table("action_history_v2", recreate="always") as batch:
        batch.drop_index("ix_action_history_v2_resource_id")
        batch.drop_index("ix_action_history_v2_resource_type")
        batch.drop_index("ix_action_history_v2_action_type")
        batch.drop_index("ix_action_history_v2_correlation_id")
        batch.drop_index("ix_action_history_v2_actor_id")
        batch.drop_index("ix_action_history_v2_tenant_id")
        batch.drop_column("correlation_id")
        batch.drop_column("actor_id")
        batch.drop_column("tenant_id")

    with op.batch_alter_table("experiment_configs", recreate="always") as batch:
        batch.drop_constraint("uq_experiment_configs_tenant_key", type_="unique")
        batch.drop_index("ix_experiment_configs_correlation_id")
        batch.drop_index("ix_experiment_configs_updated_by")
        batch.drop_index("ix_experiment_configs_created_by")
        batch.drop_index("ix_experiment_configs_config_key")
        batch.drop_index("ix_experiment_configs_tenant_id")
        batch.drop_constraint("pk_experiment_configs", type_="primary")
        batch.create_primary_key(None, ["config_key"])
        batch.drop_column("correlation_id")
        batch.drop_column("updated_by")
        batch.drop_column("created_by")
        batch.drop_column("tenant_id")
        batch.drop_column("id")

    for table_name in ("export_jobs_v2", "prediction_jobs_v2", "import_jobs_v2"):
        with op.batch_alter_table(table_name, recreate="always") as batch:
            batch.drop_index(f"ix_{table_name}_correlation_id")
            batch.drop_index(f"ix_{table_name}_updated_by")
            batch.drop_index(f"ix_{table_name}_created_by")
            batch.drop_index(f"ix_{table_name}_tenant_id")
            batch.drop_column("correlation_id")
            batch.drop_column("updated_by")
            batch.drop_column("created_by")
            batch.drop_column("tenant_id")

    with op.batch_alter_table("field_mappings_v2", recreate="always") as batch:
        batch.drop_constraint("uq_field_mappings_tenant_connector", type_="unique")
        batch.drop_index("ix_field_mappings_v2_correlation_id")
        batch.drop_index("ix_field_mappings_v2_updated_by")
        batch.drop_index("ix_field_mappings_v2_created_by")
        batch.drop_index("ix_field_mappings_v2_connector_name")
        batch.drop_index("ix_field_mappings_v2_tenant_id")
        batch.drop_constraint("pk_field_mappings_v2", type_="primary")
        batch.create_primary_key(None, ["connector_name"])
        batch.drop_column("correlation_id")
        batch.drop_column("updated_by")
        batch.drop_column("created_by")
        batch.drop_column("tenant_id")
        batch.drop_column("id")

    with op.batch_alter_table("connector_configs", recreate="always") as batch:
        batch.drop_constraint("uq_connector_configs_tenant_name", type_="unique")
        batch.drop_constraint("uq_connector_configs_tenant_connector_id", type_="unique")
        batch.drop_index("ix_connector_configs_correlation_id")
        batch.drop_index("ix_connector_configs_updated_by")
        batch.drop_index("ix_connector_configs_created_by")
        batch.drop_index("ix_connector_configs_connector_id")
        batch.drop_index("ix_connector_configs_tenant_id")
        batch.drop_column("correlation_id")
        batch.drop_column("updated_by")
        batch.drop_column("created_by")
        batch.drop_column("connector_id")
        batch.drop_column("tenant_id")
        batch.drop_index("ix_connector_configs_name")
        batch.create_index("ix_connector_configs_name", ["name"], unique=True)

    for table_name in ("tenant_memberships_v1", "platform_users_v1", "tenants_v1"):
        op.drop_table(table_name)
