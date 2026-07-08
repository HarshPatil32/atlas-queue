"""add priority to jobs claim index

Revision ID: d5f2c8a1b3e7
Revises: b4e91f6c7a2d
Create Date: 2026-07-08 10:45:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d5f2c8a1b3e7"
down_revision: str | None = "b4e91f6c7a2d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_index("ix_jobs_queue_status_next_run_at", table_name="jobs")
    op.create_index(
        "ix_jobs_queue_status_priority_next_run_at",
        "jobs",
        ["queue", "status", sa.text("priority DESC"), "next_run_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_jobs_queue_status_priority_next_run_at", table_name="jobs")
    op.create_index(
        "ix_jobs_queue_status_next_run_at",
        "jobs",
        ["queue", "status", "next_run_at"],
        unique=False,
    )
