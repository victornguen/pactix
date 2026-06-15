"""Optional HTTP inbox endpoint helpers.

Starlette is imported lazily so the
HTTP outbox side can be installed without a web framework.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncEngine

from pactix.errors import PactixError, ValidationError
from pactix.inbox import InboxStore, ReceiveOutcome
from pactix.message import JsonValue, MessageEnvelope, MessageMetadata

from .headers import (
    CORRELATION_ID_HEADER,
    EVENT_TYPE_HEADER,
    MESSAGE_ID_HEADER,
    MESSAGE_KEY_HEADER,
    ORDERING_KEY_HEADER,
)

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response


@dataclass(frozen=True)
class HttpInboundMessage:
    """Metadata plus payload extracted from an HTTP request."""

    message_id: str
    event_type: str
    message_key: str | None
    ordering_key: str | None
    correlation_id: str | None
    headers: dict[str, str]
    payload: JsonValue = field(default=None)

    @classmethod
    def from_headers(cls, headers: Mapping[str, str], payload: JsonValue) -> HttpInboundMessage:
        normalized = {key.lower(): value for key, value in headers.items()}

        message_id = normalized.get(MESSAGE_ID_HEADER)
        if message_id is None:
            raise ValidationError(f'{MESSAGE_ID_HEADER} header is required')
        event_type = normalized.get(EVENT_TYPE_HEADER)
        if event_type is None:
            raise ValidationError(f'{EVENT_TYPE_HEADER} header is required')

        return cls(
            message_id=message_id,
            event_type=event_type,
            message_key=normalized.get(MESSAGE_KEY_HEADER),
            ordering_key=normalized.get(ORDERING_KEY_HEADER),
            correlation_id=normalized.get(CORRELATION_ID_HEADER),
            headers=normalized,
            payload=payload,
        )

    def to_envelope(self) -> MessageEnvelope:
        metadata = MessageMetadata(message_id=self.message_id, event_type=self.event_type)
        if self.message_key is not None:
            metadata = metadata.with_message_key(self.message_key)
        if self.ordering_key is not None:
            metadata = metadata.with_ordering_key(self.ordering_key)
        if self.correlation_id is not None:
            metadata = metadata.with_correlation_id(self.correlation_id)
        metadata = metadata.with_headers(self.headers)
        return MessageEnvelope(metadata, self.payload)


class HttpInboxEndpoint:
    """Durable-receipt adapter for HTTP inbox flows."""

    def __init__(self, source: str, store: InboxStore) -> None:
        self._source = source
        self._store = store

    def normalize_message(self, message: HttpInboundMessage) -> MessageEnvelope:
        return message.to_envelope()

    async def save_message(self, engine: AsyncEngine, message: HttpInboundMessage) -> ReceiveOutcome:
        envelope = self.normalize_message(message)
        async with engine.begin() as conn:
            return await self._store.save_received(conn, self._source, envelope)

    async def save(self, engine: AsyncEngine, headers: Mapping[str, str], payload: JsonValue) -> ReceiveOutcome:
        message = HttpInboundMessage.from_headers(headers, payload)
        return await self.save_message(engine, message)


def make_inbox_endpoint(
    source: str, store: InboxStore, engine: AsyncEngine
) -> Callable[[Request], Awaitable[Response]]:
    """Build a Starlette endpoint that durably stores inbound events.

    Validation errors map to 400, persistence/routing errors to 500, and a
    successful save to 202 Accepted.
    """
    from starlette.responses import PlainTextResponse, Response  # lazy import

    endpoint = HttpInboxEndpoint(source, store)

    async def handler(request: Request) -> Response:
        payload = await request.json()
        try:
            await endpoint.save(engine, dict(request.headers), payload)
        except ValidationError as error:
            return PlainTextResponse(str(error), status_code=400)
        except PactixError as error:
            return PlainTextResponse(str(error), status_code=500)
        return Response(status_code=202)

    return handler


def inbox_route(path: str, source: str, store: InboxStore, engine: AsyncEngine) -> Any:
    """Build a Starlette POST ``Route`` for the inbox endpoint."""
    from starlette.routing import Route  # lazy import

    return Route(path, make_inbox_endpoint(source, store, engine), methods=['POST'])


__all__ = [
    'HttpInboundMessage',
    'HttpInboxEndpoint',
    'make_inbox_endpoint',
    'inbox_route',
]
