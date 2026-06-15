"""Tests for outbox/inbox status enums and in-memory record transitions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pactix.config import RetryPolicy
from pactix.errors import PersistenceError, ValidationError
from pactix.inbox.model import InboxMessageRecord, InboxStatus
from pactix.message import MessageMetadata
from pactix.outbox.model import OutboxMessageRecord, OutboxStatus


def test_outbox_status_round_trip() -> None:
    for status in OutboxStatus:
        assert OutboxStatus.from_str(status.as_str()) is status


def test_outbox_status_unknown_rejected() -> None:
    with pytest.raises(PersistenceError, match="unknown outbox status 'bogus'"):
        OutboxStatus.from_str('bogus')


def test_inbox_status_round_trip() -> None:
    for status in InboxStatus:
        assert InboxStatus.from_str(status.as_str()) is status


def test_outbox_pending_starts_pending() -> None:
    record = OutboxMessageRecord.pending(MessageMetadata('m-1', 'order.created'), {'a': 1}, RetryPolicy())
    assert record.status is OutboxStatus.PENDING
    assert record.attempts == 0
    assert record.max_attempts == 10


def test_outbox_retry_transition() -> None:
    record = OutboxMessageRecord.pending(MessageMetadata('m-1', 'order.created'), {'a': 1}, RetryPolicy())
    future = datetime.now(UTC) + timedelta(seconds=5)
    record.mark_processing(future)
    record.mark_retry('temporary', future)
    assert record.status is OutboxStatus.PENDING
    assert record.attempts == 1
    assert record.available_at == future
    assert record.last_error == 'temporary'


def test_outbox_published_transition() -> None:
    record = OutboxMessageRecord.pending(MessageMetadata('m-1', 'order.created'), {'a': 1}, RetryPolicy())
    now = datetime.now(UTC)
    record.mark_published(now)
    assert record.status is OutboxStatus.PUBLISHED
    assert record.published_at == now
    assert record.lease_until is None


def test_inbox_reject_empty_source() -> None:
    with pytest.raises(ValidationError, match='source must not be empty'):
        InboxMessageRecord.pending('  ', MessageMetadata('m-1', 'order.created'), {}, RetryPolicy())


def test_inbox_processed_transition() -> None:
    record = InboxMessageRecord.pending('kafka:orders', MessageMetadata('m-1', 'order.created'), {}, RetryPolicy())
    now = datetime.now(UTC)
    record.mark_processing(now)
    record.mark_processed(now)
    assert record.status is InboxStatus.PROCESSED
    assert record.processed_at == now
    assert record.lease_until is None
