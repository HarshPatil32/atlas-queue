"""make idempotency_key unique per queue

Revision ID: e6a3d9c4f102
Revises: d5f2c8a1b3e7
Create Date: 2026-07-16 11:40:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "e6a3d9c4f102"
down_revision: str | None = "d5f2c8a1b3e7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint("jobs_idempotency_key_key", "jobs", type_="unique")
    op.create_unique_constraint(
        "uq_jobs_queue_idempotency_key",
        "jobs",
        ["queue", "idempotency_key"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("uq_jobs_queue_idempotency_key", "jobs", type_="unique")
    op.create_unique_constraint(None, "jobs", ["idempotency_key"])
