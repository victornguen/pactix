"""SQLAlchemy table definitions for the outbox and inbox.

PostgreSQL stays the reference DDL (JSONB, partial indexes); MySQL 8 is served
through ``.with_variant(...)`` types (JSON, CHAR uuid, DATETIME(6), VARCHAR(255)
on indexed/unique text) and the ``postgresql_where`` partial indexes degrade to
plain indexes there. The library never applies these automatically; a
consuming service owns when to run the migrations.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Column,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.expression import TextClause
from sqlalchemy.types import TypeEngine

metadata = MetaData()

OUTBOX_STATUSES = ('pending', 'processing', 'published', 'failed')
INBOX_STATUSES = ('pending', 'processing', 'processed', 'failed')


class _JsonEmptyObject(TextClause):
    """Empty-JSON-object server default rendered per dialect.

    PostgreSQL renders ``'{}'::jsonb`` (byte-identical to before); MySQL only
    accepts expression defaults on JSON columns, rendered as
    ``(JSON_OBJECT())``.
    """

    __visit_name__ = 'json_empty_object'
    inherit_cache = True


@compiles(_JsonEmptyObject)
def _compile_json_empty_object(element: _JsonEmptyObject, compiler: SQLCompiler, **kw: Any) -> str:
    return "'{}'::jsonb"


@compiles(_JsonEmptyObject, 'mysql')
def _compile_json_empty_object_mysql(element: _JsonEmptyObject, compiler: SQLCompiler, **kw: Any) -> str:
    return '(JSON_OBJECT())'


def _uuid() -> TypeEngine[Any]:
    # MySQL stores uuids as CHAR(32) hex, round-tripping uuid.UUID in Python.
    return UUID(as_uuid=True).with_variant(Uuid(as_uuid=True, native_uuid=False), 'mysql')


def _jsonb() -> TypeEngine[Any]:
    return JSONB().with_variant(mysql.JSON(), 'mysql')


def _timestamptz() -> TypeEngine[Any]:
    # Microsecond precision is required for FIFO tie-breaks on created_at/received_at.
    return TIMESTAMP(timezone=True).with_variant(mysql.DATETIME(fsp=6), 'mysql')


def _text() -> TypeEngine[Any]:
    return Text()


def _indexed_text() -> TypeEngine[Any]:
    # MySQL cannot index unbounded TEXT; indexed/unique text is VARCHAR(255) there.
    return Text().with_variant(String(255), 'mysql')


def _json_default() -> _JsonEmptyObject:
    return _JsonEmptyObject('{}')


outbox_message = Table(
    'outbox_message',
    metadata,
    Column('id', _uuid(), primary_key=True),
    Column('message_id', _indexed_text(), nullable=False, unique=True),
    Column('event_type', _indexed_text(), nullable=False),
    Column('message_key', _text(), nullable=True),
    Column('ordering_key', _indexed_text(), nullable=True),
    Column('correlation_id', _indexed_text(), nullable=True),
    Column('payload', _jsonb(), nullable=False),
    Column('headers', _jsonb(), nullable=False, server_default=_json_default()),
    Column('status', _indexed_text(), nullable=False),
    Column('attempts', Integer, nullable=False, server_default=text('0')),
    Column('max_attempts', Integer, nullable=False),
    Column('available_at', _timestamptz(), nullable=False),
    Column('lease_until', _timestamptz(), nullable=True),
    Column('last_error', _text(), nullable=True),
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
    Column('id', _uuid(), primary_key=True),
    Column('source', _indexed_text(), nullable=False),
    Column('message_id', _indexed_text(), nullable=False),
    Column('event_type', _indexed_text(), nullable=False),
    Column('message_key', _text(), nullable=True),
    Column('ordering_key', _indexed_text(), nullable=True),
    Column('correlation_id', _indexed_text(), nullable=True),
    Column('payload', _jsonb(), nullable=False),
    Column('headers', _jsonb(), nullable=False, server_default=_json_default()),
    Column('status', _indexed_text(), nullable=False),
    Column('attempts', Integer, nullable=False, server_default=text('0')),
    Column('max_attempts', Integer, nullable=False),
    Column('available_at', _timestamptz(), nullable=False),
    Column('lease_until', _timestamptz(), nullable=True),
    Column('last_error', _text(), nullable=True),
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
