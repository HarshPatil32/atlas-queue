"""create workers table

Revision ID: b4e91f6c7a2d
Revises: a7d2e9f01b3c
Create Date: 2026-07-08 00:10:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b4e91f6c7a2d"
down_revision: str | None = "a7d2e9f01b3c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "workers",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("worker_name", sa.String(length=255), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column(
            "queues",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            server_default="idle",
            nullable=False,
        ),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("current_job_id", sa.BigInteger(), nullable=True),
        sa.CheckConstraint(
            "status IN ('idle', 'running', 'offline')",
            name="ck_workers_status",
        ),
        sa.ForeignKeyConstraint(
            ["current_job_id"],
            ["jobs.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("worker_name"),
    )
    op.create_index(
        "ix_workers_status_last_heartbeat_at",
        "workers",
        ["status", "last_heartbeat_at"],
        unique=False,
    )
    op.create_index(
        "ix_workers_current_job_id",
        "workers",
        ["current_job_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_workers_current_job_id", table_name="workers")
    op.drop_index("ix_workers_status_last_heartbeat_at", table_name="workers")
    op.drop_table("workers")
