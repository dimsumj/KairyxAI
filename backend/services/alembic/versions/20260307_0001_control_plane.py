"""create control plane tables

Revision ID: 20260307_0001
Revises:
Create Date: 2026-03-07 00:01:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260307_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "connector_configs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("connector_type", sa.String(length=128), nullable=False),
        sa.Column("config_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_connector_configs_name", "connector_configs", ["name"], unique=True)
    op.create_index("ix_connector_configs_type", "connector_configs", ["connector_type"], unique=False)

    op.create_table(
        "field_mappings_v2",
        sa.Column("connector_name", sa.String(length=255), primary_key=True),
        sa.Column("mapping_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )

    for table_name, extra_columns in (
        ("import_jobs_v2", [sa.Column("source_name", sa.String(length=255), nullable=False)]),
        ("prediction_jobs_v2", [sa.Column("import_job_id", sa.String(length=64), nullable=False)]),
        ("export_jobs_v2", [sa.Column("prediction_job_id", sa.String(length=64), nullable=True)]),
    ):
        op.create_table(
            table_name,
            sa.Column("id", sa.String(length=64), primary_key=True),
            *extra_columns,
            sa.Column("status", sa.String(length=64), nullable=False),
            sa.Column("spec_json", sa.Text(), nullable=False),
            sa.Column("progress_json", sa.Text(), nullable=False),
            sa.Column("error", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index(f"ix_{table_name}_status", table_name, ["status"], unique=False)

    op.create_table(
        "experiment_configs",
        sa.Column("config_key", sa.String(length=64), primary_key=True),
        sa.Column("config_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "action_history_v2",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("action_type", sa.String(length=128), nullable=False),
        sa.Column("resource_type", sa.String(length=128), nullable=False),
        sa.Column("resource_id", sa.String(length=128), nullable=True),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_action_history_v2_created_at", "action_history_v2", ["created_at"], unique=False)

    op.create_table(
        "ingestion_checkpoints_v2",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("job_id", sa.String(length=64), nullable=False),
        sa.Column("shard_index", sa.Integer(), nullable=False),
        sa.Column("source_name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("cursor_value", sa.String(length=255), nullable=True),
        sa.Column("gcs_uri", sa.Text(), nullable=True),
        sa.Column("message_id", sa.String(length=255), nullable=True),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("job_id", "shard_index", name="uq_ingestion_checkpoint_job_shard"),
    )
    op.create_index("ix_ingestion_checkpoints_v2_job_id", "ingestion_checkpoints_v2", ["job_id"], unique=False)


def downgrade() -> None:
    for table_name in (
        "ingestion_checkpoints_v2",
        "action_history_v2",
        "experiment_configs",
        "export_jobs_v2",
        "prediction_jobs_v2",
        "import_jobs_v2",
        "field_mappings_v2",
        "connector_configs",
    ):
        op.drop_table(table_name)
