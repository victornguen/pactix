"""Inbox domain types and worker."""

from __future__ import annotations

from .handler import HandleOutcome, InboxHandler, inbox_handler
from .idempotency import (
    IdempotencyDecision,
    IdempotencyHook,
    ProcessAll,
    StaticIdempotency,
    idempotency_hook,
)
from .model import InboxMessageRecord, InboxStatus
from .runner import InboxRunner, RunSummary
from .stale import AcceptAllEvents, StaleEventDecision, StaleEventFilter, stale_event_filter
from .store import InboxStore, ReceiveOutcome

__all__ = [
    'HandleOutcome',
    'InboxHandler',
    'inbox_handler',
    'IdempotencyDecision',
    'IdempotencyHook',
    'ProcessAll',
    'StaticIdempotency',
    'idempotency_hook',
    'InboxMessageRecord',
    'InboxStatus',
    'InboxRunner',
    'RunSummary',
    'AcceptAllEvents',
    'StaleEventDecision',
    'StaleEventFilter',
    'stale_event_filter',
    'InboxStore',
    'ReceiveOutcome',
]
