"""Outbox persistence.

PostgreSQL-specific claim query preserved as
textual SQL; inserts/updates use SQLAlchemy Core. On MySQL 8 the claim
branches to an equivalent ``SELECT ... FOR UPDATE SKIP LOCKED`` followed by an
``UPDATE ... WHERE id IN (...)`` in the caller's transaction (MySQL has no
``RETURNING``), and datetimes are stored as naive UTC (``DATETIME``).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import RowMapping

from pactix._dialect import MYSQL, aware_utc, dialect_name, naive_utc
from pactix._executor import Executor
from pactix.config import PactixConfig, RetryPolicy
from pactix.db.tables import outbox_message
from pactix.message import MessageEnvelope

from .model import OutboxMessageRecord, OutboxStatus

# Claim ready rows, preserving FIFO per (event_type, ordering_key) and leasing
# them with FOR UPDATE SKIP LOCKED so concurrent workers never double-claim.
_CLAIM_READY_SQL = text(
    """
    WITH claimable AS (
        SELECT candidate.id
        FROM outbox_message AS candidate
        WHERE candidate.status = 'pending'
          AND candidate.available_at <= :now
          AND (
                candidate.ordering_key IS NULL
                OR NOT EXISTS (
                    SELECT 1
                    FROM outbox_message AS older
                    WHERE older.event_type = candidate.event_type
                      AND older.ordering_key = candidate.ordering_key
                      AND older.created_at < candidate.created_at
                      AND older.status IN ('pending', 'processing')
                )
          )
        ORDER BY candidate.created_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE outbox_message AS claimed
    SET status = 'processing',
        lease_until = :lease_until,
        updated_at = :now
    FROM claimable
    WHERE claimed.id = claimable.id
    RETURNING claimed.*
    """
).columns(*outbox_message.c)

# MySQL 8 equivalent of _CLAIM_READY_SQL with the same FIFO gate: no RETURNING
# in MySQL, so the rows are selected first (FOR UPDATE SKIP LOCKED) and marked
# claimed by a separate UPDATE in the same transaction.
_CLAIM_READY_MYSQL_SQL = text(
    """
    SELECT candidate.*
    FROM outbox_message AS candidate
    WHERE candidate.status = 'pending'
      AND candidate.available_at <= :now
      AND (
            candidate.ordering_key IS NULL
            OR NOT EXISTS (
                SELECT 1
                FROM outbox_message AS older
                WHERE older.event_type = candidate.event_type
                  AND older.ordering_key = candidate.ordering_key
                  AND older.created_at < candidate.created_at
                  AND older.status IN ('pending', 'processing')
            )
      )
    ORDER BY candidate.created_at ASC
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
    """
).columns(*outbox_message.c)


class OutboxStore:
    """Reads and writes ``outbox_message`` rows."""

    def __init__(self, config: PactixConfig) -> None:
        self._config = config

    @property
    def config(self) -> PactixConfig:
        return self._config

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._config.retry_policy

    async def append(self, executor: Executor, envelope: MessageEnvelope) -> OutboxMessageRecord:
        """Insert a new pending row using the caller's executor/transaction."""
        record = OutboxMessageRecord.pending(envelope.metadata, envelope.payload, self._config.retry_policy)
        dialect = dialect_name(executor)
        values = _record_values(record, dialect)
        if dialect == MYSQL:
            # MySQL 8 has no INSERT ... RETURNING; the inserted values fully
            # describe the row, so the record is returned as-is.
            await executor.execute(outbox_message.insert().values(**values))
            return record
        stmt = outbox_message.insert().values(**values).returning(*outbox_message.c)
        row = (await executor.execute(stmt)).mappings().one()
        return _map_record(row)

    async def list_by_status(self, executor: Executor, status: OutboxStatus) -> list[OutboxMessageRecord]:
        stmt = (
            outbox_message.select()
            .where(outbox_message.c.status == status.as_str())
            .order_by(outbox_message.c.created_at.asc())
        )
        rows = (await executor.execute(stmt)).mappings().all()
        return [_map_record(row) for row in rows]

    async def reclaim_expired(self, executor: Executor, now: datetime) -> int:
        if dialect_name(executor) == MYSQL:
            now = naive_utc(now)
        stmt = (
            outbox_message.update()
            .where(outbox_message.c.status == 'processing')
            .where(outbox_message.c.lease_until < now)
            .values(status='pending', lease_until=None, updated_at=now)
        )
        result = await executor.execute(stmt)
        return cast(int, result.rowcount)  # type: ignore[attr-defined]

    async def claim_ready(self, executor: Executor, now: datetime) -> list[OutboxMessageRecord]:
        if dialect_name(executor) == MYSQL:
            return await self._claim_ready_mysql(executor, now)
        lease_until = now + self._config.lease_duration
        rows = (
            (
                await executor.execute(
                    _CLAIM_READY_SQL,
                    {'now': now, 'limit': self._config.outbox_batch_size, 'lease_until': lease_until},
                )
            )
            .mappings()
            .all()
        )
        return [_map_record(row) for row in rows]

    async def _claim_ready_mysql(self, executor: Executor, now: datetime) -> list[OutboxMessageRecord]:
        db_now = naive_utc(now)
        lease_until = now + self._config.lease_duration
        rows = (
            (
                await executor.execute(
                    _CLAIM_READY_MYSQL_SQL,
                    {'now': db_now, 'limit': self._config.outbox_batch_size},
                )
            )
            .mappings()
            .all()
        )
        if not rows:
            return []
        stmt = (
            outbox_message.update()
            .where(outbox_message.c.id.in_([row['id'] for row in rows]))
            .values(status='processing', lease_until=naive_utc(lease_until), updated_at=db_now)
        )
        await executor.execute(stmt)
        records: list[OutboxMessageRecord] = []
        for row in rows:
            record = _map_record(row)
            # Mirror the UPDATE so the returned records match the stored rows.
            record.status = OutboxStatus.PROCESSING
            record.lease_until = lease_until
            record.updated_at = now
            records.append(record)
        return records

    async def mark_published(self, executor: Executor, id: uuid.UUID, published_at: datetime) -> None:
        if dialect_name(executor) == MYSQL:
            published_at = naive_utc(published_at)
        stmt = (
            outbox_message.update()
            .where(outbox_message.c.id == id)
            .values(
                status='published',
                lease_until=None,
                published_at=published_at,
                updated_at=published_at,
            )
        )
        await executor.execute(stmt)

    async def mark_retry(self, executor: Executor, id: uuid.UUID, error: str, available_at: datetime) -> None:
        updated_at = _now()
        if dialect_name(executor) == MYSQL:
            available_at = naive_utc(available_at)
            updated_at = naive_utc(updated_at)
        stmt = (
            outbox_message.update()
            .where(outbox_message.c.id == id)
            .values(
                status='pending',
                attempts=outbox_message.c.attempts + 1,
                available_at=available_at,
                lease_until=None,
                last_error=error,
                updated_at=updated_at,
            )
        )
        await executor.execute(stmt)

    async def mark_failed(self, executor: Executor, id: uuid.UUID, error: str) -> None:
        updated_at = _now()
        if dialect_name(executor) == MYSQL:
            updated_at = naive_utc(updated_at)
        stmt = (
            outbox_message.update()
            .where(outbox_message.c.id == id)
            .values(
                status='failed',
                attempts=outbox_message.c.attempts + 1,
                lease_until=None,
                last_error=error,
                updated_at=updated_at,
            )
        )
        await executor.execute(stmt)


def _now() -> datetime:
    return datetime.now(UTC)


def _record_values(record: OutboxMessageRecord, dialect: str) -> dict[str, Any]:
    values: dict[str, Any] = {
        'id': record.id,
        'message_id': record.message_id,
        'event_type': record.event_type,
        'message_key': record.message_key,
        'ordering_key': record.ordering_key,
        'correlation_id': record.correlation_id,
        'payload': record.payload,
        'headers': dict(sorted(record.headers.items())),
        'status': record.status.as_str(),
        'attempts': record.attempts,
        'max_attempts': record.max_attempts,
        'available_at': record.available_at,
        'lease_until': record.lease_until,
        'last_error': record.last_error,
        'created_at': record.created_at,
        'updated_at': record.updated_at,
        'published_at': record.published_at,
    }
    if dialect == MYSQL:
        # MySQL DATETIME is timezone-naive; strip to naive UTC on write.
        for key in ('available_at', 'lease_until', 'created_at', 'updated_at', 'published_at'):
            value = values[key]
            if value is not None:
                values[key] = naive_utc(value)
    return values


def _map_record(row: RowMapping) -> OutboxMessageRecord:
    return OutboxMessageRecord(
        id=row['id'],
        message_id=row['message_id'],
        event_type=row['event_type'],
        message_key=row['message_key'],
        ordering_key=row['ordering_key'],
        correlation_id=row['correlation_id'],
        payload=row['payload'],
        headers=dict(row['headers']),
        status=OutboxStatus.from_str(row['status']),
        attempts=row['attempts'],
        max_attempts=row['max_attempts'],
        available_at=aware_utc(row['available_at']),
        lease_until=aware_utc(row['lease_until']) if row['lease_until'] is not None else None,
        last_error=row['last_error'],
        created_at=aware_utc(row['created_at']),
        updated_at=aware_utc(row['updated_at']),
        published_at=aware_utc(row['published_at']) if row['published_at'] is not None else None,
    )


__all__ = ['OutboxStore']
