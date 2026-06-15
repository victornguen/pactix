"""Outbox status and in-memory record."""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from pactix.config import RetryPolicy
from pactix.errors import PersistenceError
from pactix.message import JsonValue, MessageEnvelope, MessageMetadata


def _utcnow() -> datetime:
    return datetime.now(UTC)


class OutboxStatus(enum.Enum):
    """Lifecycle state of an outbox row."""

    PENDING = 'pending'
    PROCESSING = 'processing'
    PUBLISHED = 'published'
    FAILED = 'failed'

    def as_str(self) -> str:
        return self.value

    @classmethod
    def from_str(cls, value: str) -> OutboxStatus:
        try:
            return cls(value)
        except ValueError as error:
            raise PersistenceError(f"unknown outbox status '{value}'") from error


@dataclass
class OutboxMessageRecord:
    """In-memory representation of a row from ``outbox_message``."""

    id: uuid.UUID
    message_id: str
    event_type: str
    message_key: str | None
    ordering_key: str | None
    correlation_id: str | None
    payload: JsonValue
    headers: dict[str, str]
    status: OutboxStatus
    attempts: int
    max_attempts: int
    available_at: datetime
    lease_until: datetime | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    published_at: datetime | None = None

    @classmethod
    def pending(
        cls,
        metadata: MessageMetadata,
        payload: JsonValue,
        retry_policy: RetryPolicy,
    ) -> OutboxMessageRecord:
        """Create a new ``pending`` record from validated metadata and payload."""
        envelope = MessageEnvelope(metadata, payload)
        now = _utcnow()
        return cls(
            id=uuid.uuid4(),
            message_id=envelope.metadata.message_id,
            event_type=envelope.metadata.event_type,
            message_key=envelope.metadata.message_key,
            ordering_key=envelope.metadata.ordering_key,
            correlation_id=envelope.metadata.correlation_id,
            payload=envelope.payload,
            headers=dict(envelope.metadata.headers),
            status=OutboxStatus.PENDING,
            attempts=0,
            max_attempts=retry_policy.max_attempts,
            available_at=now,
            lease_until=None,
            last_error=None,
            created_at=now,
            updated_at=now,
            published_at=None,
        )

    def mark_processing(self, lease_until: datetime) -> None:
        self.status = OutboxStatus.PROCESSING
        self.lease_until = lease_until
        self.updated_at = _utcnow()

    def mark_retry(self, error: str, available_at: datetime) -> None:
        self.status = OutboxStatus.PENDING
        self.attempts += 1
        self.available_at = available_at
        self.lease_until = None
        self.last_error = error
        self.updated_at = _utcnow()

    def mark_failed(self, error: str) -> None:
        self.status = OutboxStatus.FAILED
        self.attempts += 1
        self.lease_until = None
        self.last_error = error
        self.updated_at = _utcnow()

    def mark_published(self, published_at: datetime) -> None:
        self.status = OutboxStatus.PUBLISHED
        self.lease_until = None
        self.published_at = published_at
        self.updated_at = _utcnow()


__all__ = ['OutboxStatus', 'OutboxMessageRecord']
