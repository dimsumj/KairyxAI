"""add v1 control-plane resource tables

Revision ID: 20260310_0002
Revises: 20260307_0001
Create Date: 2026-03-10 00:02:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260310_0002"
down_revision = "20260307_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "control_plane_resources_v1",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("resource_type", "resource_id", name="uq_control_plane_resource_type_id"),
    )
    op.create_index("ix_control_plane_resources_v1_resource_type", "control_plane_resources_v1", ["resource_type"], unique=False)
    op.create_index("ix_control_plane_resources_v1_resource_id", "control_plane_resources_v1", ["resource_id"], unique=False)
    op.create_index("ix_control_plane_resources_v1_name", "control_plane_resources_v1", ["name"], unique=False)
    op.create_index("ix_control_plane_resources_v1_status", "control_plane_resources_v1", ["status"], unique=False)

    op.create_table(
        "control_plane_resource_versions_v1",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("resource_type", "resource_id", "version", name="uq_control_plane_resource_version"),
    )
    op.create_index("ix_control_plane_resource_versions_v1_resource_type", "control_plane_resource_versions_v1", ["resource_type"], unique=False)
    op.create_index("ix_control_plane_resource_versions_v1_resource_id", "control_plane_resource_versions_v1", ["resource_id"], unique=False)
    op.create_index("ix_control_plane_resource_versions_v1_version", "control_plane_resource_versions_v1", ["version"], unique=False)
    op.create_index("ix_control_plane_resource_versions_v1_created_at", "control_plane_resource_versions_v1", ["created_at"], unique=False)

    op.create_table(
        "control_plane_resource_events_v1",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_control_plane_resource_events_v1_resource_type", "control_plane_resource_events_v1", ["resource_type"], unique=False)
    op.create_index("ix_control_plane_resource_events_v1_resource_id", "control_plane_resource_events_v1", ["resource_id"], unique=False)
    op.create_index("ix_control_plane_resource_events_v1_event_type", "control_plane_resource_events_v1", ["event_type"], unique=False)
    op.create_index("ix_control_plane_resource_events_v1_created_at", "control_plane_resource_events_v1", ["created_at"], unique=False)


def downgrade() -> None:
    for table_name in (
        "control_plane_resource_events_v1",
        "control_plane_resource_versions_v1",
        "control_plane_resources_v1",
    ):
        op.drop_table(table_name)
