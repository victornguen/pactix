"""create inbox_message

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '0002'
down_revision: str | None = '0001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'inbox_message',
        sa.Column('id', postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column('source', sa.Text(), nullable=False),
        sa.Column('message_id', sa.Text(), nullable=False),
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
        sa.Column('received_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('processed_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'processing', 'processed', 'failed')",
            name='inbox_message_status_check',
        ),
        sa.UniqueConstraint('source', 'message_id', name='inbox_message_source_message_id_key'),
    )
    op.create_index(
        'idx_inbox_message_ready',
        'inbox_message',
        ['status', 'available_at'],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        'idx_inbox_message_fifo',
        'inbox_message',
        ['event_type', 'ordering_key', 'received_at'],
        postgresql_where=sa.text('ordering_key IS NOT NULL'),
    )
    op.create_index(
        'idx_inbox_message_lease',
        'inbox_message',
        ['lease_until'],
        postgresql_where=sa.text("status = 'processing'"),
    )
    op.create_index(
        'idx_inbox_message_correlation',
        'inbox_message',
        ['correlation_id'],
        postgresql_where=sa.text('correlation_id IS NOT NULL'),
    )


def downgrade() -> None:
    op.drop_table('inbox_message')
