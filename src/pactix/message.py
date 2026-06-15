"""Message metadata and envelope value objects.

``MessageMetadata`` carries delivery metadata and validates itself;
``MessageEnvelope`` pairs validated metadata with a JSON payload.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from pactix.errors import ValidationError

# A JSON-serializable payload value. PostgreSQL stores it as JSONB.
JsonValue = Any


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class MessageMetadata:
    """Delivery metadata for a single message.

    Headers preserve a stable (sorted) key ordering when persisted or
    transported, matching the upstream ``BTreeMap`` intent.
    """

    message_id: str
    event_type: str
    message_key: str | None = None
    ordering_key: str | None = None
    correlation_id: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=_utcnow)

    @classmethod
    def for_event_type(cls, event_type: str) -> MessageMetadata:
        """Build metadata with a freshly generated UUIDv4 message id."""
        return cls(message_id=str(uuid.uuid4()), event_type=event_type)

    def with_message_key(self, message_key: str) -> MessageMetadata:
        """Return a copy with ``message_key`` set."""
        return replace(self, message_key=message_key)

    def with_ordering_key(self, ordering_key: str) -> MessageMetadata:
        """Return a copy with ``ordering_key`` set."""
        return replace(self, ordering_key=ordering_key)

    def with_correlation_id(self, correlation_id: str) -> MessageMetadata:
        """Return a copy with ``correlation_id`` set."""
        return replace(self, correlation_id=correlation_id)

    def with_headers(self, headers: Mapping[str, str]) -> MessageMetadata:
        """Return a copy with ``headers`` replaced."""
        return replace(self, headers=dict(headers))

    def validate(self, requires_ordering_key: bool = False) -> None:
        """Validate the metadata, raising :class:`ValidationError` if invalid.

        ``message_id`` and ``event_type`` must not be blank. A provided
        ``ordering_key`` must not be blank. When ``requires_ordering_key`` is
        set, a non-blank ``ordering_key`` must be present.
        """
        if not self.message_id.strip():
            raise ValidationError('message_id must not be empty')

        if not self.event_type.strip():
            raise ValidationError('event_type must not be empty')

        has_ordering_key = self.ordering_key is not None and bool(self.ordering_key.strip())

        if requires_ordering_key and not has_ordering_key:
            raise ValidationError('ordering_key is required for ordered event types')

        if self.ordering_key is not None and not self.ordering_key.strip():
            raise ValidationError('ordering_key must not be empty when provided')


@dataclass(frozen=True)
class MessageEnvelope:
    """A validated message metadata paired with its JSON payload."""

    metadata: MessageMetadata
    payload: JsonValue

    def __post_init__(self) -> None:
        self.metadata.validate(requires_ordering_key=False)


__all__ = ['MessageMetadata', 'MessageEnvelope', 'JsonValue']
