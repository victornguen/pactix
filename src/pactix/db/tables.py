"""SQLAlchemy table definitions for the outbox and inbox.

PostgreSQL only (JSONB, partial indexes). The library never applies these automatically; a
consuming service owns when to run the migrations.
"""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID

metadata = MetaData()

OUTBOX_STATUSES = ('pending', 'processing', 'published', 'failed')
INBOX_STATUSES = ('pending', 'processing', 'processed', 'failed')


def _timestamptz() -> TIMESTAMP:
    return TIMESTAMP(timezone=True)


outbox_message = Table(
    'outbox_message',
    metadata,
    Column('id', UUID(as_uuid=True), primary_key=True),
    Column('message_id', Text, nullable=False, unique=True),
    Column('event_type', Text, nullable=False),
    Column('message_key', Text, nullable=True),
    Column('ordering_key', Text, nullable=True),
    Column('correlation_id', Text, nullable=True),
    Column('payload', JSONB, nullable=False),
    Column('headers', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column('status', Text, nullable=False),
    Column('attempts', Integer, nullable=False, server_default=text('0')),
    Column('max_attempts', Integer, nullable=False),
    Column('available_at', _timestamptz(), nullable=False),
    Column('lease_until', _timestamptz(), nullable=True),
    Column('last_error', Text, nullable=True),
    Column('created_at', _timestamptz(), nullable=False),
    Column('updated_at', _timestamptz(), nullable=False),
    Column('published_at', _timestamptz(), nullable=True),
    CheckConstraint(
        "status IN ('pending', 'processing', 'published', 'failed')",
        name='outbox_message_status_check',
    ),
    Index(
        'idx_outbox_message_ready',
        'status',
        'available_at',
        postgresql_where=text("status = 'pending'"),
    ),
    Index(
        'idx_outbox_message_fifo',
        'event_type',
        'ordering_key',
        'created_at',
        postgresql_where=text('ordering_key IS NOT NULL'),
    ),
    Index(
        'idx_outbox_message_lease',
        'lease_until',
        postgresql_where=text("status = 'processing'"),
    ),
    Index(
        'idx_outbox_message_correlation',
        'correlation_id',
        postgresql_where=text('correlation_id IS NOT NULL'),
    ),
)


inbox_message = Table(
    'inbox_message',
    metadata,
    Column('id', UUID(as_uuid=True), primary_key=True),
    Column('source', Text, nullable=False),
    Column('message_id', Text, nullable=False),
    Column('event_type', Text, nullable=False),
    Column('message_key', Text, nullable=True),
    Column('ordering_key', Text, nullable=True),
    Column('correlation_id', Text, nullable=True),
    Column('payload', JSONB, nullable=False),
    Column('headers', JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column('status', Text, nullable=False),
    Column('attempts', Integer, nullable=False, server_default=text('0')),
    Column('max_attempts', Integer, nullable=False),
    Column('available_at', _timestamptz(), nullable=False),
    Column('lease_until', _timestamptz(), nullable=True),
    Column('last_error', Text, nullable=True),
    Column('received_at', _timestamptz(), nullable=False),
    Column('updated_at', _timestamptz(), nullable=False),
    Column('processed_at', _timestamptz(), nullable=True),
    CheckConstraint(
        "status IN ('pending', 'processing', 'processed', 'failed')",
        name='inbox_message_status_check',
    ),
    UniqueConstraint('source', 'message_id', name='inbox_message_source_message_id_key'),
    Index(
        'idx_inbox_message_ready',
        'status',
        'available_at',
        postgresql_where=text("status = 'pending'"),
    ),
    Index(
        'idx_inbox_message_fifo',
        'event_type',
        'ordering_key',
        'received_at',
        postgresql_where=text('ordering_key IS NOT NULL'),
    ),
    Index(
        'idx_inbox_message_lease',
        'lease_until',
        postgresql_where=text("status = 'processing'"),
    ),
    Index(
        'idx_inbox_message_correlation',
        'correlation_id',
        postgresql_where=text('correlation_id IS NOT NULL'),
    ),
)


__all__ = ['metadata', 'outbox_message', 'inbox_message', 'OUTBOX_STATUSES', 'INBOX_STATUSES']
