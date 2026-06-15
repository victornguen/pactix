"""Tests for the outbox publisher registry resolution."""

from __future__ import annotations

import pytest

from pactix.config import RetryPolicy
from pactix.errors import RoutingError
from pactix.message import MessageMetadata
from pactix.outbox import PublisherRegistry, PublishOutcome, outbox_publisher
from pactix.outbox.model import OutboxMessageRecord


def _record(event_type: str) -> OutboxMessageRecord:
    return OutboxMessageRecord.pending(MessageMetadata('m-1', event_type), {}, RetryPolicy())


@pytest.mark.asyncio
async def test_exact_registration_takes_precedence_over_catch_all() -> None:
    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(lambda r: _async(PublishOutcome.terminal('catch'))))
    await registry.register('order.created', outbox_publisher(lambda r: _async(PublishOutcome.published())))

    outcome = await registry.publish(_record('order.created'))
    assert outcome.is_published


@pytest.mark.asyncio
async def test_catch_all_used_when_no_exact() -> None:
    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(lambda r: _async(PublishOutcome.published())))
    outcome = await registry.publish(_record('unmapped'))
    assert outcome.is_published


@pytest.mark.asyncio
async def test_register_many() -> None:
    registry = PublisherRegistry()
    await registry.register_many(['a', 'b'], outbox_publisher(lambda r: _async(PublishOutcome.published())))
    assert (await registry.resolve('a')) is (await registry.resolve('b'))


@pytest.mark.asyncio
async def test_unresolvable_event_type_raises() -> None:
    registry = PublisherRegistry()
    with pytest.raises(RoutingError, match="no publisher registered for 'nope'"):
        await registry.resolve('nope')


async def _async(value: PublishOutcome) -> PublishOutcome:
    return value
