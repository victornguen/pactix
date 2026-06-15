"""Optional HTTP outbox publisher.

``httpx`` is imported lazily so the core library never requires it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace

from pactix.errors import RoutingError
from pactix.message import JsonValue
from pactix.outbox import OutboxMessageRecord, PublishOutcome

from .headers import (
    CORRELATION_ID_HEADER,
    EVENT_TYPE_HEADER,
    MESSAGE_ID_HEADER,
    MESSAGE_KEY_HEADER,
    ORDERING_KEY_HEADER,
)


@dataclass(frozen=True)
class HttpEndpoint:
    """Per-event-type HTTP target."""

    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)

    def with_header(self, name: str, value: str) -> HttpEndpoint:
        merged = dict(self.headers)
        merged[name] = value
        return replace(self, headers=merged)


@dataclass(frozen=True)
class HttpOutboundRequest:
    """An outbound HTTP request built from an outbox row."""

    method: str
    url: str
    payload: JsonValue
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class HttpResponse:
    """A simplified HTTP response."""

    status: int
    body: str | None = None


class HttpRequestClient:
    """Protocol for sending an HTTP request."""

    async def send(self, request: HttpOutboundRequest) -> HttpResponse:  # pragma: no cover - protocol
        raise NotImplementedError


class HttpEndpointRouter:
    """Resolves an :class:`HttpEndpoint` from an ``event_type``."""

    def __init__(self) -> None:
        self._endpoints: dict[str, HttpEndpoint] = {}
        self._lock = asyncio.Lock()

    async def register(self, event_type: str, endpoint: HttpEndpoint) -> None:
        async with self._lock:
            self._endpoints[event_type] = endpoint

    async def resolve(self, event_type: str) -> HttpEndpoint:
        async with self._lock:
            endpoint = self._endpoints.get(event_type)
        if endpoint is None:
            raise RoutingError(f"no HTTP endpoint registered for '{event_type}'")
        return endpoint


class HttpOutboxPublisher:
    """Outbox publisher that turns outbox rows into JSON HTTP requests."""

    def __init__(self, client: HttpRequestClient, router: HttpEndpointRouter) -> None:
        self._client = client
        self._router = router

    async def build_request(self, record: OutboxMessageRecord) -> HttpOutboundRequest:
        endpoint = await self._router.resolve(record.event_type)
        headers = dict(endpoint.headers)
        headers.update(record.headers)
        headers['content-type'] = 'application/json'
        headers[MESSAGE_ID_HEADER] = record.message_id
        headers[EVENT_TYPE_HEADER] = record.event_type
        if record.message_key is not None:
            headers[MESSAGE_KEY_HEADER] = record.message_key
        if record.ordering_key is not None:
            headers[ORDERING_KEY_HEADER] = record.ordering_key
        if record.correlation_id is not None:
            headers[CORRELATION_ID_HEADER] = record.correlation_id
        return HttpOutboundRequest(
            method=endpoint.method,
            url=endpoint.url,
            payload=record.payload,
            headers=headers,
        )

    @staticmethod
    def classify_response(response: HttpResponse) -> PublishOutcome:
        if 200 <= response.status < 300:
            return PublishOutcome.published()

        body = (response.body or '').strip()
        suffix = f': {body}' if body else ''
        message = f'HTTP transport returned {response.status}{suffix}'
        if response.status in (408, 429) or response.status >= 500:
            return PublishOutcome.retryable(message)
        return PublishOutcome.terminal(message)

    async def publish(self, record: OutboxMessageRecord) -> PublishOutcome:
        request = await self.build_request(record)
        try:
            response = await self._client.send(request)
        except Exception as error:  # noqa: BLE001 - transport errors become retryable
            return PublishOutcome.retryable(str(error))
        return self.classify_response(response)


class HttpxRequestClient(HttpRequestClient):
    """Concrete request client backed by ``httpx.AsyncClient``."""

    def __init__(self, client: object | None = None) -> None:
        if client is None:
            import httpx  # lazy import

            client = httpx.AsyncClient()
        self._client = client

    async def send(self, request: HttpOutboundRequest) -> HttpResponse:
        response = await self._client.request(  # type: ignore[attr-defined]
            request.method,
            request.url,
            json=request.payload,
            headers=request.headers,
        )
        return HttpResponse(status=response.status_code, body=response.text)


__all__ = [
    'HttpEndpoint',
    'HttpEndpointRouter',
    'HttpOutboundRequest',
    'HttpResponse',
    'HttpRequestClient',
    'HttpOutboxPublisher',
    'HttpxRequestClient',
]
