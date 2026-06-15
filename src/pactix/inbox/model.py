"""Inbox status and in-memory record."""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from pactix.config import RetryPolicy
from pactix.errors import PersistenceError, ValidationError
from pactix.message import JsonValue, MessageEnvelope, MessageMetadata


def _utcnow() -> datetime:
    return datetime.now(UTC)


class InboxStatus(enum.Enum):
    """Lifecycle state of an inbox row."""

    PENDING = 'pending'
    PROCESSING = 'processing'
    PROCESSED = 'processed'
    FAILED = 'failed'

    def as_str(self) -> str:
        return self.value

    @classmethod
    def from_str(cls, value: str) -> InboxStatus:
        try:
            return cls(value)
        except ValueError as error:
            raise PersistenceError(f"unknown inbox status '{value}'") from error


@dataclass
class InboxMessageRecord:
    """In-memory representation of a row from ``inbox_message``."""

    id: uuid.UUID
    source: str
    message_id: str
    event_type: str
    message_key: str | None
    ordering_key: str | None
    correlation_id: str | None
    payload: JsonValue
    headers: dict[str, str]
    status: InboxStatus
    attempts: int
    max_attempts: int
    available_at: datetime
    lease_until: datetime | None
    last_error: str | None
    received_at: datetime
    updated_at: datetime
    processed_at: datetime | None = None

    @classmethod
    def pending(
        cls,
        source: str,
        metadata: MessageMetadata,
        payload: JsonValue,
        retry_policy: RetryPolicy,
    ) -> InboxMessageRecord:
        """Create a new ``pending`` record after validating source and envelope."""
        if not source.strip():
            raise ValidationError('source must not be empty')

        envelope = MessageEnvelope(metadata, payload)
        now = _utcnow()
        return cls(
            id=uuid.uuid4(),
            source=source,
            message_id=envelope.metadata.message_id,
            event_type=envelope.metadata.event_type,
            message_key=envelope.metadata.message_key,
            ordering_key=envelope.metadata.ordering_key,
            correlation_id=envelope.metadata.correlation_id,
            payload=envelope.payload,
            headers=dict(envelope.metadata.headers),
            status=InboxStatus.PENDING,
            attempts=0,
            max_attempts=retry_policy.max_attempts,
            available_at=now,
            lease_until=None,
            last_error=None,
            received_at=now,
            updated_at=now,
            processed_at=None,
        )

    def mark_processing(self, lease_until: datetime) -> None:
        self.status = InboxStatus.PROCESSING
        self.lease_until = lease_until
        self.updated_at = _utcnow()

    def mark_retry(self, error: str, available_at: datetime) -> None:
        self.status = InboxStatus.PENDING
        self.attempts += 1
        self.available_at = available_at
        self.lease_until = None
        self.last_error = error
        self.updated_at = _utcnow()

    def mark_failed(self, error: str) -> None:
        self.status = InboxStatus.FAILED
        self.attempts += 1
        self.lease_until = None
        self.last_error = error
        self.updated_at = _utcnow()

    def mark_processed(self, processed_at: datetime) -> None:
        self.status = InboxStatus.PROCESSED
        self.lease_until = None
        self.processed_at = processed_at
        self.updated_at = _utcnow()


__all__ = ['InboxStatus', 'InboxMessageRecord']
