"""Database-backed tests for the outbox-processing capability."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from pactix.config import PactixConfig, RetryPolicy
from pactix.message import MessageEnvelope, MessageMetadata
from pactix.outbox import (
    OutboxRunner,
    OutboxStatus,
    OutboxStore,
    PublisherRegistry,
    PublishOutcome,
    outbox_publisher,
)

pytestmark = pytest.mark.postgres


def _config(**kw: object) -> PactixConfig:
    return PactixConfig(**kw)  # type: ignore[arg-type]


def _envelope(message_id: str, event_type: str = 'order.created', **meta_kw: str) -> MessageEnvelope:
    meta = MessageMetadata(message_id, event_type)
    for key, value in meta_kw.items():
        meta = getattr(meta, f'with_{key}')(value)
    return MessageEnvelope(meta, {'message_id': message_id})


async def test_append_joins_caller_transaction_rollback(engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    conn = await engine.connect()
    trans = await conn.begin()
    await store.append(conn, _envelope('m-1'))
    await trans.rollback()
    await conn.close()

    async with engine.connect() as conn:
        pending = await store.list_by_status(conn, OutboxStatus.PENDING)
    assert pending == []


async def test_append_returns_stored_record(engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    async with engine.begin() as conn:
        record = await store.append(conn, _envelope('m-1'))
    assert record.message_id == 'm-1'
    assert record.status is OutboxStatus.PENDING
    assert record.id is not None


async def test_fifo_gating_within_ordering_key(engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    async with engine.begin() as conn:
        await store.append(conn, _envelope('older', ordering_key='o-1'))
    async with engine.begin() as conn:
        await store.append(conn, _envelope('newer', ordering_key='o-1'))

    now = datetime.now(UTC)
    async with engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    assert [r.message_id for r in claimed] == ['older']

    # publish the older one, then the newer becomes claimable
    async with engine.begin() as conn:
        await store.mark_published(conn, claimed[0].id, now)
    async with engine.begin() as conn:
        claimed2 = await store.claim_ready(conn, now)
    assert [r.message_id for r in claimed2] == ['newer']


async def test_concurrent_workers_do_not_double_claim(engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    for i in range(10):
        async with engine.begin() as conn:
            await store.append(conn, _envelope(f'm-{i}'))

    now = datetime.now(UTC)

    async def claim() -> list[str]:
        async with engine.begin() as conn:
            rows = await store.claim_ready(conn, now)
            return [r.message_id for r in rows]

    a, b = await asyncio.gather(claim(), claim())
    assert set(a).isdisjoint(set(b))
    assert len(set(a) | set(b)) == len(a) + len(b)


async def test_reclaim_expired_lease(engine: AsyncEngine) -> None:
    store = OutboxStore(_config(lease_duration=timedelta(seconds=30)))
    async with engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))

    now = datetime.now(UTC)
    async with engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)  # lease_until = now + 30s
    assert len(claimed) == 1

    future = now + timedelta(hours=1)  # lease has since expired
    async with engine.begin() as conn:
        reclaimed = await store.reclaim_expired(conn, future)
    assert reclaimed == 1
    async with engine.connect() as conn:
        pending = await store.list_by_status(conn, OutboxStatus.PENDING)
    assert len(pending) == 1


async def _run(engine: AsyncEngine, outcome: PublishOutcome) -> object:
    store = OutboxStore(_config())
    async with engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))
    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(lambda r: _async(outcome)))
    runner = OutboxRunner(store, registry)
    return await runner.run_once(engine)


async def test_runner_publishes(engine: AsyncEngine) -> None:
    summary = await _run(engine, PublishOutcome.published())
    assert summary.claimed == 1
    assert summary.published == 1
    assert summary.published <= summary.claimed


async def test_runner_terminal_fails_immediately(engine: AsyncEngine) -> None:
    store = OutboxStore(_config())
    async with engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))
    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(lambda r: _async(PublishOutcome.terminal('bad'))))
    runner = OutboxRunner(store, registry)
    summary = await runner.run_once(engine)
    assert summary.failed == 1
    async with engine.connect() as conn:
        failed = await store.list_by_status(conn, OutboxStatus.FAILED)
    assert len(failed) == 1 and failed[0].attempts == 1


async def test_runner_retryable_reschedules(engine: AsyncEngine) -> None:
    store = OutboxStore(_config(retry_policy=RetryPolicy(max_attempts=5)))
    async with engine.begin() as conn:
        await store.append(conn, _envelope('m-1'))
    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(lambda r: _async(PublishOutcome.retryable('again'))))
    runner = OutboxRunner(store, registry)
    summary = await runner.run_once(engine)
    assert summary.retry_scheduled == 1
    async with engine.connect() as conn:
        pending = await store.list_by_status(conn, OutboxStatus.PENDING)
    assert len(pending) == 1 and pending[0].attempts == 1


async def _async(value: PublishOutcome) -> PublishOutcome:
    return value
