"""Inbox persistence.

Deduplication uses an ``ON CONFLICT`` upsert with the ``(xmax = 0)``
trick to distinguish a fresh insert from a duplicate;
the claim queries preserve PostgreSQL FIFO / skip-locked semantics as textual
SQL.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import RowMapping

from pactix._executor import Executor
from pactix.config import PactixConfig, RetryPolicy
from pactix.db.tables import inbox_message
from pactix.message import MessageEnvelope

from .model import InboxMessageRecord, InboxStatus


@dataclass(frozen=True)
class ReceiveOutcome:
    """Result of :meth:`InboxStore.save_received`."""

    record: InboxMessageRecord
    inserted: bool

    @property
    def is_inserted(self) -> bool:
        return self.inserted

    @property
    def is_duplicate(self) -> bool:
        return not self.inserted


_CLAIM_READY_FIFO_SQL = text(
    """
    WITH claimable AS (
        SELECT candidate.id
        FROM inbox_message AS candidate
        WHERE candidate.status = 'pending'
          AND candidate.available_at <= :now
          AND (
                candidate.ordering_key IS NULL
                OR NOT EXISTS (
                    SELECT 1
                    FROM inbox_message AS older
                    WHERE older.event_type = candidate.event_type
                      AND older.ordering_key = candidate.ordering_key
                      AND older.received_at < candidate.received_at
                      AND older.status IN ('pending', 'processing')
                )
          )
        ORDER BY candidate.received_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE inbox_message AS claimed
    SET status = 'processing',
        lease_until = :lease_until,
        updated_at = :now
    FROM claimable
    WHERE claimed.id = claimable.id
    RETURNING claimed.*
    """
).columns(*inbox_message.c)


_CLAIM_READY_UNORDERED_SQL = text(
    """
    WITH claimable AS (
        SELECT candidate.id
        FROM inbox_message AS candidate
        WHERE candidate.status = 'pending'
          AND candidate.available_at <= :now
        ORDER BY candidate.received_at ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE inbox_message AS claimed
    SET status = 'processing',
        lease_until = :lease_until,
        updated_at = :now
    FROM claimable
    WHERE claimed.id = claimable.id
    RETURNING claimed.*
    """
).columns(*inbox_message.c)


class InboxStore:
    """Reads and writes ``inbox_message`` rows."""

    def __init__(self, config: PactixConfig) -> None:
        self._config = config

    @property
    def config(self) -> PactixConfig:
        return self._config

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._config.retry_policy

    async def save_received(self, executor: Executor, source: str, envelope: MessageEnvelope) -> ReceiveOutcome:
        """Durably store a received message, deduplicating on (source, message_id)."""
        record = InboxMessageRecord.pending(source, envelope.metadata, envelope.payload, self._config.retry_policy)
        stmt = (
            pg_insert(inbox_message)
            .values(**_record_values(record))
            .on_conflict_do_update(
                index_elements=['source', 'message_id'],
                set_={'source': inbox_message.c.source},
            )
            .returning(*inbox_message.c, text('(xmax = 0) AS inserted'))
        )
        row = (await executor.execute(stmt)).mappings().one()
        return ReceiveOutcome(record=_map_record(row), inserted=bool(row['inserted']))

    async def list_by_status(self, executor: Executor, status: InboxStatus) -> list[InboxMessageRecord]:
        stmt = (
            inbox_message.select()
            .where(inbox_message.c.status == status.as_str())
            .order_by(inbox_message.c.received_at.asc())
        )
        rows = (await executor.execute(stmt)).mappings().all()
        return [_map_record(row) for row in rows]

    async def reclaim_expired(self, executor: Executor, now: datetime) -> int:
        stmt = (
            inbox_message.update()
            .where(inbox_message.c.status == 'processing')
            .where(inbox_message.c.lease_until < now)
            .values(status='pending', lease_until=None, updated_at=now)
        )
        result = await executor.execute(stmt)
        return cast(int, result.rowcount)  # type: ignore[attr-defined]

    async def claim_ready(self, executor: Executor, now: datetime) -> list[InboxMessageRecord]:
        return await self._claim(executor, now, _CLAIM_READY_FIFO_SQL)

    async def claim_ready_unordered(self, executor: Executor, now: datetime) -> list[InboxMessageRecord]:
        return await self._claim(executor, now, _CLAIM_READY_UNORDERED_SQL)

    async def _claim(self, executor: Executor, now: datetime, query: Any) -> list[InboxMessageRecord]:
        lease_until = now + self._config.lease_duration
        rows = (
            (
                await executor.execute(
                    query,
                    {'now': now, 'limit': self._config.inbox_batch_size, 'lease_until': lease_until},
                )
            )
            .mappings()
            .all()
        )
        return [_map_record(row) for row in rows]

    async def mark_processed(self, executor: Executor, id: uuid.UUID, processed_at: datetime) -> None:
        stmt = (
            inbox_message.update()
            .where(inbox_message.c.id == id)
            .values(
                status='processed',
                lease_until=None,
                processed_at=processed_at,
                updated_at=processed_at,
            )
        )
        await executor.execute(stmt)

    async def mark_retry(self, executor: Executor, id: uuid.UUID, error: str, available_at: datetime) -> None:
        stmt = (
            inbox_message.update()
            .where(inbox_message.c.id == id)
            .values(
                status='pending',
                attempts=inbox_message.c.attempts + 1,
                available_at=available_at,
                lease_until=None,
                last_error=error,
                updated_at=_now(),
            )
        )
        await executor.execute(stmt)

    async def mark_failed(self, executor: Executor, id: uuid.UUID, error: str) -> None:
        stmt = (
            inbox_message.update()
            .where(inbox_message.c.id == id)
            .values(
                status='failed',
                attempts=inbox_message.c.attempts + 1,
                lease_until=None,
                last_error=error,
                updated_at=_now(),
            )
        )
        await executor.execute(stmt)


def _now() -> datetime:
    return datetime.now(UTC)


def _record_values(record: InboxMessageRecord) -> dict[str, Any]:
    return {
        'id': record.id,
        'source': record.source,
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
        'received_at': record.received_at,
        'updated_at': record.updated_at,
        'processed_at': record.processed_at,
    }


def _map_record(row: RowMapping) -> InboxMessageRecord:
    return InboxMessageRecord(
        id=row['id'],
        source=row['source'],
        message_id=row['message_id'],
        event_type=row['event_type'],
        message_key=row['message_key'],
        ordering_key=row['ordering_key'],
        correlation_id=row['correlation_id'],
        payload=row['payload'],
        headers=dict(row['headers']),
        status=InboxStatus.from_str(row['status']),
        attempts=row['attempts'],
        max_attempts=row['max_attempts'],
        available_at=row['available_at'],
        lease_until=row['lease_until'],
        last_error=row['last_error'],
        received_at=row['received_at'],
        updated_at=row['updated_at'],
        processed_at=row['processed_at'],
    )


__all__ = ['InboxStore', 'ReceiveOutcome']
