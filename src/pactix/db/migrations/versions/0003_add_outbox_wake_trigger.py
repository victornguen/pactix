"""add outbox wake trigger

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-29

Opt-in: only needed for ``NotifyMode.TRIGGER``; coalesced mode notifies from
the application instead.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = '0003'
down_revision: str | None = '0002'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION outbox_wake() RETURNS trigger AS $$
        BEGIN
            PERFORM pg_notify('outbox_wake', '');
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER outbox_message_wake
        AFTER INSERT ON outbox_message
        FOR EACH ROW
        EXECUTE FUNCTION outbox_wake()
        """
    )


def downgrade() -> None:
    op.execute('DROP TRIGGER IF EXISTS outbox_message_wake ON outbox_message')
    op.execute('DROP FUNCTION IF EXISTS outbox_wake()')
