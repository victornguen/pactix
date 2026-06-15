"""Canonical HTTP metadata header names shared by both HTTP sides."""

from __future__ import annotations

EVENT_TYPE_HEADER = 'x-pactix-event-type'
MESSAGE_ID_HEADER = 'x-pactix-message-id'
MESSAGE_KEY_HEADER = 'x-pactix-message-key'
ORDERING_KEY_HEADER = 'x-pactix-ordering-key'
CORRELATION_ID_HEADER = 'x-pactix-correlation-id'

__all__ = [
    'EVENT_TYPE_HEADER',
    'MESSAGE_ID_HEADER',
    'MESSAGE_KEY_HEADER',
    'ORDERING_KEY_HEADER',
    'CORRELATION_ID_HEADER',
]
