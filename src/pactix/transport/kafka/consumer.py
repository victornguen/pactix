"""Optional Kafka inbox consumer."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine

from pactix.errors import ValidationError
from pactix.inbox import InboxStore, ReceiveOutcome
from pactix.message import JsonValue, MessageEnvelope, MessageMetadata


@dataclass(frozen=True)
class KafkaInboundMessage:
    """A consumed Kafka message normalized into transport-agnostic fields."""

    topic: str
    partition: int
    offset: int
    key: str | None
    payload: JsonValue
    headers: dict[str, str] = field(default_factory=dict)

    def fallback_message_id(self) -> str:
        return f'{self.topic}:{self.partition}:{self.offset}'

    def to_envelope(self) -> MessageEnvelope:
        event_type = self.headers.get('event_type')
        if event_type is None:
            raise ValidationError('event_type header is required')

        message_id = self.headers.get('message_id') or self.fallback_message_id()
        metadata = MessageMetadata(message_id=message_id, event_type=event_type)
        if self.key is not None:
            metadata = metadata.with_message_key(self.key)
        if 'ordering_key' in self.headers:
            metadata = metadata.with_ordering_key(self.headers['ordering_key'])
        if 'correlation_id' in self.headers:
            metadata = metadata.with_correlation_id(self.headers['correlation_id'])
        metadata = metadata.with_headers(self.headers)
        return MessageEnvelope(metadata, self.payload)

    @classmethod
    def from_record(cls, record: Any) -> KafkaInboundMessage:
        """Build from an ``aiokafka`` ``ConsumerRecord``."""
        if record.value is None:
            raise ValidationError('Kafka payload is required')
        payload = json.loads(record.value)
        headers: dict[str, str] = {}
        for key, value in record.headers or ():
            headers[key] = value.decode('utf-8') if value is not None else ''
        key_value = record.key.decode('utf-8') if record.key is not None else None
        return cls(
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=key_value,
            payload=payload,
            headers=headers,
        )


class KafkaOffsetCommitter:
    """Protocol for committing a consumed offset."""

    async def commit(self, topic: str, partition: int, offset: int) -> None:  # pragma: no cover - protocol
        raise NotImplementedError


class KafkaInboxConsumer:
    """Durably stores inbound Kafka messages, then commits the offset."""

    def __init__(self, source: str, store: InboxStore) -> None:
        self._source = source
        self._store = store

    def normalize_message(self, message: KafkaInboundMessage) -> MessageEnvelope:
        return message.to_envelope()

    async def save_and_commit(
        self,
        engine: AsyncEngine,
        message: KafkaInboundMessage,
        committer: KafkaOffsetCommitter,
    ) -> ReceiveOutcome:
        envelope = self.normalize_message(message)
        async with engine.begin() as conn:
            outcome = await self._store.save_received(conn, self._source, envelope)
        await committer.commit(message.topic, message.partition, message.offset)
        return outcome


class AIOKafkaOffsetCommitter(KafkaOffsetCommitter):
    """Concrete committer backed by an ``aiokafka`` consumer (commits offset+1)."""

    def __init__(self, consumer: object) -> None:
        self._consumer = consumer

    async def commit(self, topic: str, partition: int, offset: int) -> None:
        from aiokafka import TopicPartition  # lazy import

        await self._consumer.commit(  # type: ignore[attr-defined]
            {TopicPartition(topic, partition): offset + 1}
        )


__all__ = [
    'KafkaInboundMessage',
    'KafkaOffsetCommitter',
    'KafkaInboxConsumer',
    'AIOKafkaOffsetCommitter',
]
