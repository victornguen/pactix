"""Optional Kafka transport adapters (extra: ``kafka``)."""

from __future__ import annotations

from .consumer import (
    AIOKafkaOffsetCommitter,
    KafkaInboundMessage,
    KafkaInboxConsumer,
    KafkaOffsetCommitter,
)
from .publisher import (
    AIOKafkaProducerClient,
    KafkaOutboxPublisher,
    KafkaProducedMessage,
    KafkaProducerClient,
    KafkaTopicRouter,
)

__all__ = [
    'AIOKafkaOffsetCommitter',
    'KafkaInboundMessage',
    'KafkaInboxConsumer',
    'KafkaOffsetCommitter',
    'AIOKafkaProducerClient',
    'KafkaOutboxPublisher',
    'KafkaProducedMessage',
    'KafkaProducerClient',
    'KafkaTopicRouter',
]
