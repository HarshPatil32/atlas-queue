"""update jobs claim index for created_at tiebreaker

Revision ID: f8c1d2e3a4b5
Revises: e6a3d9c4f102
Create Date: 2026-07-21 16:55:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f8c1d2e3a4b5"
down_revision: str | None = "e6a3d9c4f102"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_index("ix_jobs_queue_status_priority_next_run_at", table_name="jobs")
    op.create_index(
        "ix_jobs_queue_status_priority_next_run_at",
        "jobs",
        ["queue", sa.text("priority DESC"), "next_run_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("status IN ('queued', 'scheduled', 'retrying')"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_jobs_queue_status_priority_next_run_at", table_name="jobs")
    op.create_index(
        "ix_jobs_queue_status_priority_next_run_at",
        "jobs",
        ["queue", "status", sa.text("priority DESC"), "next_run_at"],
        unique=False,
    )
