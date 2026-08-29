"""pactix: transactional outbox and inbox primitives for PostgreSQL, on SQLAlchemy."""

from __future__ import annotations

from pactix.config import NotifyMode, PactixConfig, RetryPolicy, WakeupPolicy
from pactix.errors import PactixError, PersistenceError, RoutingError, ValidationError
from pactix.inbox import (
    AcceptAllEvents,
    HandleOutcome,
    IdempotencyDecision,
    IdempotencyHook,
    InboxHandler,
    InboxMessageRecord,
    InboxRunner,
    InboxStatus,
    InboxStore,
    ProcessAll,
    ReceiveOutcome,
    StaleEventDecision,
    StaleEventFilter,
    StaticIdempotency,
    idempotency_hook,
    inbox_handler,
    stale_event_filter,
)
from pactix.message import MessageEnvelope, MessageMetadata
from pactix.outbox import (
    CoalescedNotifier,
    FallbackBackoff,
    LocalWakeup,
    OutboxMessageRecord,
    OutboxPublisher,
    OutboxRunner,
    OutboxStatus,
    OutboxStore,
    PostgresWakeListener,
    PublisherRegistry,
    PublishOutcome,
    WakeupRunner,
    outbox_publisher,
)

__version__ = '0.1.0'

__all__ = [
    '__version__',
    # config
    'PactixConfig',
    'RetryPolicy',
    'NotifyMode',
    'WakeupPolicy',
    # errors
    'PactixError',
    'ValidationError',
    'PersistenceError',
    'RoutingError',
    # message
    'MessageMetadata',
    'MessageEnvelope',
    # outbox
    'OutboxMessageRecord',
    'OutboxStatus',
    'OutboxStore',
    'OutboxPublisher',
    'PublisherRegistry',
    'PublishOutcome',
    'outbox_publisher',
    'OutboxRunner',
    'LocalWakeup',
    'FallbackBackoff',
    'PostgresWakeListener',
    'CoalescedNotifier',
    'WakeupRunner',
    # inbox
    'InboxMessageRecord',
    'InboxStatus',
    'InboxStore',
    'ReceiveOutcome',
    'InboxHandler',
    'HandleOutcome',
    'inbox_handler',
    'IdempotencyHook',
    'IdempotencyDecision',
    'idempotency_hook',
    'ProcessAll',
    'StaticIdempotency',
    'StaleEventFilter',
    'StaleEventDecision',
    'stale_event_filter',
    'AcceptAllEvents',
    'InboxRunner',
]
