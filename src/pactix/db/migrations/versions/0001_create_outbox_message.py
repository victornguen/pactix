"""create outbox_message

Revision ID: 0001
Revises:
Create Date: 2026-06-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '0001'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'outbox_message',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('message_id', sa.Text(), nullable=False, unique=True),
        sa.Column('event_type', sa.Text(), nullable=False),
        sa.Column('message_key', sa.Text(), nullable=True),
        sa.Column('ordering_key', sa.Text(), nullable=True),
        sa.Column('correlation_id', sa.Text(), nullable=True),
        sa.Column('payload', postgresql.JSONB(), nullable=False),
        sa.Column(
            'headers',
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column('status', sa.Text(), nullable=False),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default=sa.text('0')),
        sa.Column('max_attempts', sa.Integer(), nullable=False),
        sa.Column('available_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('lease_until', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('published_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'processing', 'published', 'failed')",
            name='outbox_message_status_check',
        ),
    )
    op.create_index(
        'idx_outbox_message_ready',
        'outbox_message',
        ['status', 'available_at'],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        'idx_outbox_message_fifo',
        'outbox_message',
        ['event_type', 'ordering_key', 'created_at'],
        postgresql_where=sa.text('ordering_key IS NOT NULL'),
    )
    op.create_index(
        'idx_outbox_message_lease',
        'outbox_message',
        ['lease_until'],
        postgresql_where=sa.text("status = 'processing'"),
    )
    op.create_index(
        'idx_outbox_message_correlation',
        'outbox_message',
        ['correlation_id'],
        postgresql_where=sa.text('correlation_id IS NOT NULL'),
    )


def downgrade() -> None:
    op.drop_table('outbox_message')
