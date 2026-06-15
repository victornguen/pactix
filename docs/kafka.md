# Kafka transport

Optional, behind the `kafka` extra (`pip install "pactix[kafka]"`, backed by
`aiokafka`). The core library never imports Kafka.

```python
from pactix.transport.kafka import (
    AIOKafkaProducerClient,
    KafkaInboundMessage,
    KafkaInboxConsumer,
    KafkaOutboxPublisher,
    KafkaTopicRouter,
)
```

## Outbox → Kafka

Map event types to topics and use `KafkaOutboxPublisher` as your outbox
publisher. The produced message carries the record `headers` plus `message_id`,
`event_type`, and (when present) `ordering_key` and `correlation_id`; the key is
the record's `message_key`.

```python
router = KafkaTopicRouter()
await router.register('order.created', 'orders')

client = AIOKafkaProducerClient.from_brokers('localhost:9092')
publisher = KafkaOutboxPublisher(client, router)

await registry.register('order.created', publisher)
```

A successful send yields `Published`; a send error yields `Retryable`.

## Kafka → inbox

Normalize a consumed message and durably store it, then commit the offset. The
`event_type` header is required; `message_id` falls back to
`topic:partition:offset` when absent.

```python
consumer = KafkaInboxConsumer('kafka:orders', inbox_store)

async for record in aiokafka_consumer:
    message = KafkaInboundMessage.from_record(record)
    await consumer.save_and_commit(engine, message, committer)
```

Provide a `KafkaOffsetCommitter` (e.g. `AIOKafkaOffsetCommitter`, which commits
`offset + 1`). The message is saved to the inbox **before** the offset is
committed, so a crash between the two replays the message — which the inbox
deduplicates on `(source, message_id)`.
