"""Unit tests for the transports that need no database."""

from __future__ import annotations

import json

import pytest

from pactix.config import RetryPolicy
from pactix.errors import RoutingError, ValidationError
from pactix.message import MessageMetadata
from pactix.outbox.model import OutboxMessageRecord
from pactix.transport.http import (
    EVENT_TYPE_HEADER,
    MESSAGE_ID_HEADER,
    HttpEndpoint,
    HttpEndpointRouter,
    HttpInboundMessage,
    HttpOutboxPublisher,
    HttpResponse,
)
from pactix.transport.kafka import (
    KafkaInboundMessage,
    KafkaOutboxPublisher,
    KafkaTopicRouter,
)


def _outbox_record() -> OutboxMessageRecord:
    meta = (
        MessageMetadata('m-1', 'order.created')
        .with_message_key('key-1')
        .with_ordering_key('o-1')
        .with_correlation_id('c-1')
    )
    return OutboxMessageRecord.pending(meta, {'order_id': 42}, RetryPolicy())


# --- Kafka ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_kafka_build_message() -> None:
    router = KafkaTopicRouter()
    await router.register('order.created', 'orders-topic')
    publisher = KafkaOutboxPublisher(client=_FailingKafkaClient(), router=router)
    message = await publisher.build_message(_outbox_record())
    assert message.topic == 'orders-topic'
    assert message.key == 'key-1'
    assert json.loads(message.payload) == {'order_id': 42}
    assert message.headers['message_id'] == 'm-1'
    assert message.headers['event_type'] == 'order.created'
    assert message.headers['ordering_key'] == 'o-1'
    assert message.headers['correlation_id'] == 'c-1'


@pytest.mark.asyncio
async def test_kafka_send_failure_is_retryable() -> None:
    router = KafkaTopicRouter()
    await router.register('order.created', 'orders-topic')
    publisher = KafkaOutboxPublisher(client=_FailingKafkaClient(), router=router)
    outcome = await publisher.publish(_outbox_record())
    assert outcome.is_retryable
    assert 'boom' in (outcome.message or '')


@pytest.mark.asyncio
async def test_kafka_topic_router_unmapped_raises() -> None:
    router = KafkaTopicRouter()
    with pytest.raises(RoutingError):
        await router.resolve('unmapped')


def test_kafka_missing_event_type_header_rejected() -> None:
    message = KafkaInboundMessage(topic='t', partition=0, offset=5, key=None, payload={}, headers={})
    with pytest.raises(ValidationError, match='event_type header is required'):
        message.to_envelope()


def test_kafka_message_id_fallback() -> None:
    message = KafkaInboundMessage(topic='t', partition=2, offset=9, key=None, payload={}, headers={'event_type': 'e'})
    envelope = message.to_envelope()
    assert envelope.metadata.message_id == 't:2:9'


class _FailingKafkaClient:
    async def send(self, message: object) -> None:
        raise RuntimeError('boom')


# --- HTTP ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_build_request_headers() -> None:
    router = HttpEndpointRouter()
    await router.register('order.created', HttpEndpoint('POST', 'https://x/y'))
    publisher = HttpOutboxPublisher(client=_StaticHttpClient(HttpResponse(200)), router=router)
    request = await publisher.build_request(_outbox_record())
    assert request.method == 'POST'
    assert request.headers[MESSAGE_ID_HEADER] == 'm-1'
    assert request.headers[EVENT_TYPE_HEADER] == 'order.created'
    assert request.headers['content-type'] == 'application/json'


def test_http_classify_2xx_published() -> None:
    assert HttpOutboxPublisher.classify_response(HttpResponse(200)).is_published


def test_http_classify_5xx_retryable() -> None:
    outcome = HttpOutboxPublisher.classify_response(HttpResponse(503, 'down'))
    assert outcome.is_retryable
    assert '503' in (outcome.message or '')


def test_http_classify_4xx_terminal() -> None:
    assert HttpOutboxPublisher.classify_response(HttpResponse(400)).is_terminal


def test_http_classify_408_429_retryable() -> None:
    assert HttpOutboxPublisher.classify_response(HttpResponse(408)).is_retryable
    assert HttpOutboxPublisher.classify_response(HttpResponse(429)).is_retryable


@pytest.mark.asyncio
async def test_http_send_error_is_retryable() -> None:
    router = HttpEndpointRouter()
    await router.register('order.created', HttpEndpoint('POST', 'https://x/y'))
    publisher = HttpOutboxPublisher(client=_RaisingHttpClient(), router=router)
    outcome = await publisher.publish(_outbox_record())
    assert outcome.is_retryable


def test_http_inbound_missing_header_rejected() -> None:
    with pytest.raises(ValidationError):
        HttpInboundMessage.from_headers({EVENT_TYPE_HEADER: 'e'}, {})  # missing message id


def test_http_inbound_round_trip() -> None:
    headers = {MESSAGE_ID_HEADER: 'm-1', EVENT_TYPE_HEADER: 'order.created'}
    message = HttpInboundMessage.from_headers(headers, {'a': 1})
    envelope = message.to_envelope()
    assert envelope.metadata.message_id == 'm-1'
    assert envelope.metadata.event_type == 'order.created'
    assert envelope.payload == {'a': 1}


class _StaticHttpClient:
    def __init__(self, response: HttpResponse) -> None:
        self._response = response

    async def send(self, request: object) -> HttpResponse:
        return self._response


class _RaisingHttpClient:
    async def send(self, request: object) -> HttpResponse:
        raise RuntimeError('network down')
