"""Optional HTTP transport adapters (extra: ``http``).

The outbox publisher (``httpx``) and inbox endpoint (``starlette``) can be used
independently; each imports its framework lazily.
"""

from __future__ import annotations

from .headers import (
    CORRELATION_ID_HEADER,
    EVENT_TYPE_HEADER,
    MESSAGE_ID_HEADER,
    MESSAGE_KEY_HEADER,
    ORDERING_KEY_HEADER,
)
from .inbox import HttpInboundMessage, HttpInboxEndpoint, inbox_route, make_inbox_endpoint
from .publisher import (
    HttpEndpoint,
    HttpEndpointRouter,
    HttpOutboundRequest,
    HttpOutboxPublisher,
    HttpRequestClient,
    HttpResponse,
    HttpxRequestClient,
)

__all__ = [
    'EVENT_TYPE_HEADER',
    'MESSAGE_ID_HEADER',
    'MESSAGE_KEY_HEADER',
    'ORDERING_KEY_HEADER',
    'CORRELATION_ID_HEADER',
    'HttpInboundMessage',
    'HttpInboxEndpoint',
    'inbox_route',
    'make_inbox_endpoint',
    'HttpEndpoint',
    'HttpEndpointRouter',
    'HttpOutboundRequest',
    'HttpOutboxPublisher',
    'HttpRequestClient',
    'HttpResponse',
    'HttpxRequestClient',
]
