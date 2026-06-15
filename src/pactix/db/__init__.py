"""Database schema for the outbox and inbox tables."""

from __future__ import annotations

from .tables import inbox_message, metadata, outbox_message

__all__ = ['inbox_message', 'metadata', 'outbox_message']
