"""dead_letter table (Phase 8)

Revision ID: a1b2c3d4e5f6
Revises: f6179be56627
Create Date: 2026-06-26

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "f6179be56627"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dead_letter",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("topic", sa.String(), nullable=False),
        sa.Column("partition", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("offset", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("trace_id", sa.String(), nullable=True),
        sa.Column("client_id", sa.String(), nullable=True),
        sa.Column("stage", sa.String(), nullable=False),
        sa.Column("error_type", sa.String(), nullable=False),
        sa.Column("error_msg", sa.String(), nullable=False),
        sa.Column("payload", JSONB(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("dead_letter")
