"""MySQL-backed tests for the outbox-processing capability.

Mirrors the core behaviors of ``test_outbox_db.py`` against MySQL 8, exercising
the MySQL-specific code paths: the SELECT-then-UPDATE claim (no RETURNING), the
append without RETURNING, and the tz-naive ``DATETIME(6)`` storage with UTC
re-attachment on read.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from pactix.config import PactixConfig, RetryPolicy
from pactix.db.tables import outbox_message
from pactix.message import MessageEnvelope, MessageMetadata
from pactix.outbox import (
    OutboxMessageRecord,
    OutboxRunner,
    OutboxStatus,
    OutboxStore,
    PublisherRegistry,
    PublishOutcome,
    outbox_publisher,
)

pytestmark = pytest.mark.mysql


def _config(**kw: object) -> PactixConfig:
    return PactixConfig(**kw)  # type: ignore[arg-type]


def _envelope(message_id: str, event_type: str = 'order.created', **meta_kw: str) -> MessageEnvelope:
    meta = MessageMetadata(message_id, event_type)
    for key, value in meta_kw.items():
        meta = getattr(meta, f'with_{key}')(value)
    return MessageEnvelope(meta, {'message_id': message_id})


def _naive(value: datetime) -> datetime:
    """Strip to naive UTC for direct table writes; MySQL DATETIME is tz-naive."""
    return value.astimezone(UTC).replace(tzinfo=None)


async def test_append_returns_stored_record(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    async with mysql_engine.begin() as conn:
        record = await store.append(conn, _envelope('m-1'))
    assert record.message_id == 'm-1'
    assert record.status is OutboxStatus.PENDING
    assert record.id is not None


async def test_append_joins_caller_transaction_rollback(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    conn = await mysql_engine.connect()
    trans = await conn.begin()
    await store.append(conn, _envelope('m-1'))
    await trans.rollback()
    await conn.close()

    async with mysql_engine.connect() as conn:
        pending = await store.list_by_status(conn, OutboxStatus.PENDING)
    assert pending == []


async def test_datetimes_read_back_tz_aware_utc(mysql_engine: AsyncEngine) -> None:
    # MySQL stores DATETIME tz-naive; reads must re-attach UTC.
    store = OutboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))
    async with mysql_engine.connect() as conn:
        (record,) = await store.list_by_status(conn, OutboxStatus.PENDING)
    for value in (record.available_at, record.created_at, record.updated_at):
        assert value.tzinfo is UTC


async def test_claim_batch_limit(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config(outbox_batch_size=2))
    for i in range(3):
        async with mysql_engine.begin() as conn:
            await store.append(conn, _envelope(f'm-{i}'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    assert len(claimed) == 2
    for record in claimed:
        assert record.status is OutboxStatus.PROCESSING
        assert record.lease_until is not None and record.lease_until.tzinfo is UTC


async def test_claim_respects_available_at(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    async with mysql_engine.begin() as conn:
        record = await store.append(conn, _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        await conn.execute(
            outbox_message.update()
            .where(outbox_message.c.id == record.id)
            .values(available_at=_naive(now + timedelta(hours=1)))
        )
    async with mysql_engine.begin() as conn:
        assert await store.claim_ready(conn, now) == []

    async with mysql_engine.begin() as conn:
        await conn.execute(
            outbox_message.update()
            .where(outbox_message.c.id == record.id)
            .values(available_at=_naive(now - timedelta(seconds=1)))
        )
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    assert [r.message_id for r in claimed] == ['m-1']


async def test_fifo_gating_within_ordering_key(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.append(conn, _envelope('older', ordering_key='o-1'))
    async with mysql_engine.begin() as conn:
        await store.append(conn, _envelope('newer', ordering_key='o-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    assert [r.message_id for r in claimed] == ['older']

    # publish the older one, then the newer becomes claimable
    async with mysql_engine.begin() as conn:
        await store.mark_published(conn, claimed[0].id, now)
    async with mysql_engine.begin() as conn:
        claimed2 = await store.claim_ready(conn, now)
    assert [r.message_id for r in claimed2] == ['newer']


async def test_concurrent_workers_do_not_double_claim(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    for i in range(10):
        async with mysql_engine.begin() as conn:
            await store.append(conn, _envelope(f'm-{i}'))

    now = datetime.now(UTC)

    async def claim() -> list[str]:
        async with mysql_engine.begin() as conn:
            rows = await store.claim_ready(conn, now)
            return [r.message_id for r in rows]

    a, b = await asyncio.gather(claim(), claim())
    assert set(a).isdisjoint(set(b))
    assert len(set(a) | set(b)) == len(a) + len(b)


async def test_mark_published(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    async with mysql_engine.begin() as conn:
        await store.mark_published(conn, claimed[0].id, now)

    async with mysql_engine.connect() as conn:
        published = await store.list_by_status(conn, OutboxStatus.PUBLISHED)
    assert len(published) == 1
    assert published[0].published_at is not None
    assert published[0].published_at.tzinfo is UTC


async def test_mark_retry_reschedules(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config(retry_policy=RetryPolicy(max_attempts=5)))
    async with mysql_engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    async with mysql_engine.begin() as conn:
        await store.mark_retry(conn, claimed[0].id, 'boom', now - timedelta(seconds=1))

    async with mysql_engine.connect() as conn:
        pending = await store.list_by_status(conn, OutboxStatus.PENDING)
    assert len(pending) == 1
    assert pending[0].attempts == 1
    assert pending[0].last_error == 'boom'
    assert pending[0].lease_until is None


async def test_mark_failed(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    async with mysql_engine.begin() as conn:
        await store.mark_failed(conn, claimed[0].id, 'bad')

    async with mysql_engine.connect() as conn:
        failed = await store.list_by_status(conn, OutboxStatus.FAILED)
    assert len(failed) == 1
    assert failed[0].attempts == 1
    assert failed[0].last_error == 'bad'


async def test_reclaim_expired_lease(mysql_engine: AsyncEngine) -> None:
    store = OutboxStore(_config(lease_duration=timedelta(seconds=30)))
    async with mysql_engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)  # lease_until = now + 30s
    assert len(claimed) == 1

    future = now + timedelta(hours=1)  # lease has since expired
    async with mysql_engine.begin() as conn:
        reclaimed = await store.reclaim_expired(conn, future)
    assert reclaimed == 1
    async with mysql_engine.connect() as conn:
        pending = await store.list_by_status(conn, OutboxStatus.PENDING)
    assert len(pending) == 1


async def _run(mysql_engine: AsyncEngine, outcome: PublishOutcome, published: list[str]) -> object:
    store = OutboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))

    async def publish(record: OutboxMessageRecord) -> PublishOutcome:
        published.append(record.message_id)
        return outcome

    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(publish))
    runner = OutboxRunner(store, registry)
    return await runner.run_once(mysql_engine)


async def test_runner_publishes(mysql_engine: AsyncEngine) -> None:
    published: list[str] = []
    summary = await _run(mysql_engine, PublishOutcome.published(), published)
    assert summary.claimed == 1
    assert summary.published == 1
    assert summary.published <= summary.claimed
    assert published == ['m-1']


async def test_runner_retryable_reschedules(mysql_engine: AsyncEngine) -> None:
    published: list[str] = []
    summary = await _run(mysql_engine, PublishOutcome.retryable('again'), published)
    assert summary.retry_scheduled == 1
    assert published == ['m-1']  # the publisher was called, the row rescheduled
    store = OutboxStore(_config())
    async with mysql_engine.connect() as conn:
        pending = await store.list_by_status(conn, OutboxStatus.PENDING)
    assert len(pending) == 1 and pending[0].attempts == 1
