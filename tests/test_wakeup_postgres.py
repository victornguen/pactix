"""Postgres-backed tests for the wakeup-driven outbox scheduling."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from time import monotonic

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from pactix import (
    MessageEnvelope,
    MessageMetadata,
    NotifyMode,
    OutboxMessageRecord,
    OutboxRunner,
    OutboxStore,
    PactixConfig,
    PostgresWakeListener,
    PublisherRegistry,
    PublishOutcome,
    WakeupPolicy,
    WakeupRunner,
    outbox_publisher,
)

pytestmark = pytest.mark.postgres

# The SQL of migration 0003 (opt-in, only needed for TRIGGER mode), applied
# directly so the test can reuse the async ``engine`` fixture instead of the
# sync alembic dance from test_migrations.py.
_CREATE_WAKE_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION outbox_wake() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('outbox_wake', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql
"""

_CREATE_WAKE_TRIGGER_SQL = """
CREATE OR REPLACE TRIGGER outbox_message_wake
AFTER INSERT ON outbox_message
FOR EACH ROW
EXECUTE FUNCTION outbox_wake()
"""


def _envelope(message_id: str) -> MessageEnvelope:
    return MessageEnvelope(MessageMetadata(message_id, 'order.created'), {'message_id': message_id})


async def _wait_until_published(published: list[str], timeout: float = 5.0) -> None:
    deadline = monotonic() + timeout
    while not published and monotonic() < deadline:
        await asyncio.sleep(0.05)


async def _start_wakeup_runner(
    engine: AsyncEngine,
    published: list[str],
    policy: WakeupPolicy,
) -> asyncio.Task[None]:
    async def publish(record: OutboxMessageRecord) -> PublishOutcome:
        published.append(record.message_id)
        return PublishOutcome.published()

    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(publish))
    runner = OutboxRunner(OutboxStore(PactixConfig()), registry)
    return asyncio.create_task(WakeupRunner(runner, policy).run(engine))


async def _stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def test_listener_receives_pg_notify(engine: AsyncEngine) -> None:
    listener = PostgresWakeListener(engine, 'outbox_wake')
    try:
        assert await listener.wait(timeout=0.2) is False  # nothing notified yet
        wait_task = asyncio.create_task(listener.wait(timeout=5.0))
        await asyncio.sleep(0.3)  # LISTEN is per-session; make sure it is registered first
        async with engine.begin() as conn:
            await conn.execute(text("SELECT pg_notify('outbox_wake', '')"))
        assert await wait_task is True
    finally:
        await listener.aclose()


async def test_wakeup_runner_publishes_on_notify(engine: AsyncEngine) -> None:
    published: list[str] = []
    # A 60s fallback interval proves the wake below came from NOTIFY, not polling.
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.COALESCED,
        fallback_initial_interval=timedelta(seconds=60),
        fallback_max_interval=timedelta(seconds=60),
    )
    task = await _start_wakeup_runner(engine, published, policy)
    try:
        await asyncio.sleep(0.3)  # let the runner register LISTEN before the NOTIFY
        async with engine.begin() as conn:
            await OutboxStore(PactixConfig()).append(conn, _envelope('m-notify-1'))
        async with engine.begin() as conn:
            await conn.execute(text("SELECT pg_notify('outbox_wake', '')"))
        await _wait_until_published(published)
        assert published == ['m-notify-1']
    finally:
        await _stop(task)


async def test_wakeup_runner_falls_back_to_polling(engine: AsyncEngine) -> None:
    published: list[str] = []
    policy = WakeupPolicy(
        enabled=True,
        notify_mode=NotifyMode.OFF,
        fallback_initial_interval=timedelta(milliseconds=200),
    )
    task = await _start_wakeup_runner(engine, published, policy)
    try:
        async with engine.begin() as conn:
            await OutboxStore(PactixConfig()).append(conn, _envelope('m-fallback-1'))
        await _wait_until_published(published)
        assert published == ['m-fallback-1']
    finally:
        await _stop(task)


async def test_insert_trigger_wakes_listener(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.exec_driver_sql(_CREATE_WAKE_FUNCTION_SQL)
        await conn.exec_driver_sql(_CREATE_WAKE_TRIGGER_SQL)
    listener = PostgresWakeListener(engine, 'outbox_wake')
    try:
        wait_task = asyncio.create_task(listener.wait(timeout=5.0))
        await asyncio.sleep(0.3)
        async with engine.begin() as conn:
            await OutboxStore(PactixConfig()).append(conn, _envelope('m-trigger-1'))
        # the AFTER INSERT trigger emitted pg_notify('outbox_wake', '') on commit
        assert await wait_task is True
    finally:
        await listener.aclose()
