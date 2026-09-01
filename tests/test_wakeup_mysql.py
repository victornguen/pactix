"""MySQL-backed tests for the wakeup-driven outbox scheduling.

MySQL has no LISTEN/NOTIFY, so the wake listener is silently inactive there
(regardless of ``notify_mode``) and scheduling degrades to the in-process
``LocalWakeup`` signal plus adaptive fallback polling. The disabled-policy
``ValidationError`` case is covered without a database in
``test_wakeup_units.py``.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from time import monotonic

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from pactix import (
    LocalWakeup,
    MessageEnvelope,
    MessageMetadata,
    NotifyMode,
    OutboxMessageRecord,
    OutboxRunner,
    OutboxStore,
    PactixConfig,
    PublisherRegistry,
    PublishOutcome,
    WakeupPolicy,
    WakeupRunner,
    outbox_publisher,
)

pytestmark = pytest.mark.mysql


def _envelope(message_id: str) -> MessageEnvelope:
    return MessageEnvelope(MessageMetadata(message_id, 'order.created'), {'message_id': message_id})


async def _wait_until_published(published: list[str], timeout: float = 2.0) -> None:
    deadline = monotonic() + timeout
    while not published and monotonic() < deadline:
        await asyncio.sleep(0.05)


async def _start_wakeup_runner(
    engine: AsyncEngine,
    published: list[str],
    policy: WakeupPolicy,
    local_wakeup: LocalWakeup | None = None,
) -> asyncio.Task[None]:
    async def publish(record: OutboxMessageRecord) -> PublishOutcome:
        published.append(record.message_id)
        return PublishOutcome.published()

    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(publish))
    runner = OutboxRunner(OutboxStore(PactixConfig()), registry)
    return asyncio.create_task(WakeupRunner(runner, policy, local_wakeup).run(engine))


async def _stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def test_local_wakeup_publishes_without_listener(mysql_engine: AsyncEngine) -> None:
    # COALESCED on MySQL: the wake listener stays silently inactive, so the
    # publish below can only come from the local wake (or fallback polling).
    published: list[str] = []
    local = LocalWakeup()
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.COALESCED,
        fallback_initial_interval=timedelta(milliseconds=200),
    )
    task = await _start_wakeup_runner(mysql_engine, published, policy, local)
    try:
        async with mysql_engine.begin() as conn:
            await OutboxStore(PactixConfig()).append(conn, _envelope('m-local-1'))
        local.wake()
        await _wait_until_published(published)
        assert published == ['m-local-1']
    finally:
        await _stop(task)


async def test_fallback_polling_alone_publishes(mysql_engine: AsyncEngine) -> None:
    # No local wake at all: the adaptive fallback poll must still drain the row.
    published: list[str] = []
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.COALESCED,
        fallback_initial_interval=timedelta(milliseconds=200),
    )
    task = await _start_wakeup_runner(mysql_engine, published, policy)
    try:
        async with mysql_engine.begin() as conn:
            await OutboxStore(PactixConfig()).append(conn, _envelope('m-fallback-1'))
        await _wait_until_published(published)
        assert published == ['m-fallback-1']
    finally:
        await _stop(task)
