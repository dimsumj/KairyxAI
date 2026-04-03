"""add organization invites for org-level access

Revision ID: 20260402_0005
Revises: 20260324_0004
Create Date: 2026-04-02 00:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260402_0005"
down_revision = "20260324_0004"
branch_labels = None
depends_on = None


SYSTEM_ACTOR = "system"


def upgrade() -> None:
    op.create_table(
        "organization_invites_v1",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("invite_code", sa.String(length=128), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=True),
        sa.Column("role", sa.String(length=32), nullable=False, server_default="member"),
        sa.Column("status", sa.String(length=64), nullable=False, server_default="pending"),
        sa.Column("created_by", sa.String(length=128), nullable=False, server_default=SYSTEM_ACTOR),
        sa.Column("redeemed_by", sa.String(length=128), nullable=True),
        sa.Column("correlation_id", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("redeemed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("invite_code", name="uq_organization_invites_v1_invite_code"),
        sa.UniqueConstraint("tenant_id", "email", name="uq_organization_invites_v1_tenant_email"),
    )
    op.create_index("ix_organization_invites_v1_tenant_id", "organization_invites_v1", ["tenant_id"], unique=False)
    op.create_index("ix_organization_invites_v1_invite_code", "organization_invites_v1", ["invite_code"], unique=False)
    op.create_index("ix_organization_invites_v1_email", "organization_invites_v1", ["email"], unique=False)
    op.create_index("ix_organization_invites_v1_role", "organization_invites_v1", ["role"], unique=False)
    op.create_index("ix_organization_invites_v1_status", "organization_invites_v1", ["status"], unique=False)
    op.create_index("ix_organization_invites_v1_created_by", "organization_invites_v1", ["created_by"], unique=False)
    op.create_index("ix_organization_invites_v1_redeemed_by", "organization_invites_v1", ["redeemed_by"], unique=False)
    op.create_index("ix_organization_invites_v1_correlation_id", "organization_invites_v1", ["correlation_id"], unique=False)


def downgrade() -> None:
    op.drop_table("organization_invites_v1")
