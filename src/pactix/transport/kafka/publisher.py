"""Optional Kafka outbox publisher.

`aiokafka`` is imported lazily so the core library never requires it.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field

from pactix.errors import RoutingError
from pactix.outbox import OutboxMessageRecord, PublishOutcome


@dataclass(frozen=True)
class KafkaProducedMessage:
    """A message ready to hand to a Kafka producer."""

    topic: str
    key: str | None
    payload: str
    headers: dict[str, str] = field(default_factory=dict)


class KafkaProducerClient:
    """Protocol for sending a produced message to Kafka."""

    async def send(self, message: KafkaProducedMessage) -> None:  # pragma: no cover - protocol
        raise NotImplementedError


class KafkaTopicRouter:
    """Resolves a Kafka topic from an ``event_type``."""

    def __init__(self) -> None:
        self._topics: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def register(self, event_type: str, topic: str) -> None:
        async with self._lock:
            self._topics[event_type] = topic

    async def resolve(self, event_type: str) -> str:
        async with self._lock:
            topic = self._topics.get(event_type)
        if topic is None:
            raise RoutingError(f"no Kafka topic registered for '{event_type}'")
        return topic


class KafkaOutboxPublisher:
    """Outbox publisher that produces records to Kafka."""

    def __init__(self, client: KafkaProducerClient, router: KafkaTopicRouter) -> None:
        self._client = client
        self._router = router

    async def build_message(self, record: OutboxMessageRecord) -> KafkaProducedMessage:
        topic = await self._router.resolve(record.event_type)
        headers = dict(record.headers)
        headers['message_id'] = record.message_id
        headers['event_type'] = record.event_type
        if record.ordering_key is not None:
            headers['ordering_key'] = record.ordering_key
        if record.correlation_id is not None:
            headers['correlation_id'] = record.correlation_id
        return KafkaProducedMessage(
            topic=topic,
            key=record.message_key,
            payload=json.dumps(record.payload),
            headers=headers,
        )

    async def publish(self, record: OutboxMessageRecord) -> PublishOutcome:
        message = await self.build_message(record)
        try:
            await self._client.send(message)
        except Exception as error:  # noqa: BLE001 - transport errors become retryable
            return PublishOutcome.retryable(str(error))
        return PublishOutcome.published()


class AIOKafkaProducerClient(KafkaProducerClient):
    """Concrete producer client backed by ``aiokafka``."""

    def __init__(self, producer: object) -> None:
        self._producer = producer

    @classmethod
    def from_brokers(cls, bootstrap_servers: str) -> AIOKafkaProducerClient:
        from aiokafka import AIOKafkaProducer  # lazy import

        return cls(AIOKafkaProducer(bootstrap_servers=bootstrap_servers))

    async def send(self, message: KafkaProducedMessage) -> None:
        headers = [(key, value.encode('utf-8')) for key, value in message.headers.items()]
        await self._producer.send_and_wait(  # type: ignore[attr-defined]
            message.topic,
            value=message.payload.encode('utf-8'),
            key=message.key.encode('utf-8') if message.key is not None else None,
            headers=headers,
        )


__all__ = [
    'KafkaProducedMessage',
    'KafkaProducerClient',
    'KafkaTopicRouter',
    'KafkaOutboxPublisher',
    'AIOKafkaProducerClient',
]
