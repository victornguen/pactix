"""Database-backed tests for the Kafka and HTTP transports."""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from pactix.config import PactixConfig
from pactix.inbox import InboxStatus, InboxStore
from pactix.transport.http import (
    EVENT_TYPE_HEADER,
    MESSAGE_ID_HEADER,
    make_inbox_endpoint,
)
from pactix.transport.kafka import KafkaInboundMessage, KafkaInboxConsumer

pytestmark = pytest.mark.postgres


class _RecordingCommitter:
    def __init__(self) -> None:
        self.commits: list[tuple[str, int, int]] = []

    async def commit(self, topic: str, partition: int, offset: int) -> None:
        self.commits.append((topic, partition, offset))


async def test_kafka_save_and_commit(engine: AsyncEngine) -> None:
    store = InboxStore(PactixConfig())
    consumer = KafkaInboxConsumer('kafka:orders', store)
    committer = _RecordingCommitter()
    message = KafkaInboundMessage(
        topic='orders',
        partition=0,
        offset=7,
        key='k',
        payload={'a': 1},
        headers={'event_type': 'order.created', 'message_id': 'm-1'},
    )

    outcome = await consumer.save_and_commit(engine, message, committer)
    assert outcome.is_inserted
    assert committer.commits == [('orders', 0, 7)]
    async with engine.connect() as conn:
        pending = await store.list_by_status(conn, InboxStatus.PENDING)
    assert len(pending) == 1 and pending[0].source == 'kafka:orders'


class _FakeRequest:
    def __init__(self, headers: dict[str, str], payload: object) -> None:
        self.headers = headers
        self._payload = payload

    async def json(self) -> object:
        return self._payload


async def test_http_inbox_endpoint_stores_message(engine: AsyncEngine) -> None:
    store = InboxStore(PactixConfig())
    handler = make_inbox_endpoint('http:orders', store, engine)
    request = _FakeRequest({MESSAGE_ID_HEADER: 'm-1', EVENT_TYPE_HEADER: 'order.created'}, {'a': 1})
    response = await handler(request)  # type: ignore[arg-type]
    assert response.status_code == 202
    async with engine.connect() as conn:
        pending = await store.list_by_status(conn, InboxStatus.PENDING)
    assert len(pending) == 1 and pending[0].source == 'http:orders'


async def test_http_inbox_endpoint_rejects_missing_header(engine: AsyncEngine) -> None:
    store = InboxStore(PactixConfig())
    handler = make_inbox_endpoint('http:orders', store, engine)
    request = _FakeRequest({EVENT_TYPE_HEADER: 'order.created'}, {'a': 1})  # no message id
    response = await handler(request)  # type: ignore[arg-type]
    assert response.status_code == 400
    async with engine.connect() as conn:
        pending = await store.list_by_status(conn, InboxStatus.PENDING)
    assert pending == []
