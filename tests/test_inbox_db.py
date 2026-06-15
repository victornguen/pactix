"""Database-backed tests for the inbox-processing capability."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from pactix.config import PactixConfig
from pactix.inbox import (
    HandleOutcome,
    InboxRunner,
    InboxStatus,
    InboxStore,
    ProcessAll,
    StaticIdempotency,
    inbox_handler,
    stale_event_filter,
)
from pactix.inbox.stale import StaleEventDecision
from pactix.message import MessageEnvelope, MessageMetadata

pytestmark = pytest.mark.postgres


def _config(**kw: object) -> PactixConfig:
    return PactixConfig(**kw)  # type: ignore[arg-type]


def _envelope(message_id: str, event_type: str = 'order.created', **meta_kw: str) -> MessageEnvelope:
    meta = MessageMetadata(message_id, event_type)
    for key, value in meta_kw.items():
        meta = getattr(meta, f'with_{key}')(value)
    return MessageEnvelope(meta, {'message_id': message_id})


async def test_first_receipt_inserts_then_duplicate(engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with engine.begin() as conn:
        first = await store.save_received(conn, 'kafka:orders', _envelope('m-1'))
    assert first.is_inserted

    async with engine.begin() as conn:
        second = await store.save_received(conn, 'kafka:orders', _envelope('m-1'))
    assert second.is_duplicate

    async with engine.connect() as conn:
        pending = await store.list_by_status(conn, InboxStatus.PENDING)
    assert len(pending) == 1


async def test_same_message_id_different_source_not_duplicate(engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with engine.begin() as conn:
        a = await store.save_received(conn, 'source-a', _envelope('m-1'))
        b = await store.save_received(conn, 'source-b', _envelope('m-1'))
    assert a.is_inserted and b.is_inserted


async def test_fifo_gating_within_ordering_key(engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('older', ordering_key='o-1'))
    async with engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('newer', ordering_key='o-1'))

    now = datetime.now(UTC)
    async with engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    assert [r.message_id for r in claimed] == ['older']


async def test_unordered_claim_ignores_fifo(engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('older', ordering_key='o-1'))
        await store.save_received(conn, 's', _envelope('newer', ordering_key='o-1'))

    now = datetime.now(UTC)
    async with engine.begin() as conn:
        claimed = await store.claim_ready_unordered(conn, now)
    assert {r.message_id for r in claimed} == {'older', 'newer'}


async def test_reclaim_expired_lease(engine: AsyncEngine) -> None:
    store = InboxStore(_config(lease_duration=timedelta(seconds=30)))
    async with engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    now = datetime.now(UTC)
    async with engine.begin() as conn:
        await store.claim_ready(conn, now)
    future = now + timedelta(hours=1)
    async with engine.begin() as conn:
        reclaimed = await store.reclaim_expired(conn, future)
    assert reclaimed == 1


async def test_runner_processes_with_process_all(engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    handled: list[str] = []

    async def handle(record: object) -> HandleOutcome:
        handled.append('called')
        return HandleOutcome.processed()

    runner = InboxRunner(store, inbox_handler(handle), ProcessAll())
    summary = await runner.run_once(engine)
    assert summary.processed == 1
    assert summary.processed <= summary.claimed
    assert handled == ['called']


async def test_runner_skip_bypasses_handler(engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    handled: list[str] = []

    async def handle(record: object) -> HandleOutcome:
        handled.append('called')
        return HandleOutcome.processed()

    runner = InboxRunner(store, inbox_handler(handle), StaticIdempotency.skip())
    summary = await runner.run_once(engine)
    assert summary.processed == 1
    assert handled == []
    async with engine.connect() as conn:
        processed = await store.list_by_status(conn, InboxStatus.PROCESSED)
    assert len(processed) == 1


async def test_runner_unordered_stale_skip(engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    handled: list[str] = []

    async def handle(record: object) -> HandleOutcome:
        handled.append('called')
        return HandleOutcome.processed()

    async def is_stale(record: object) -> StaleEventDecision:
        return StaleEventDecision.STALE

    runner = InboxRunner.new_unordered(store, inbox_handler(handle), ProcessAll(), stale_event_filter(is_stale))
    summary = await runner.run_once(engine)
    assert summary.stale_skipped == 1
    assert summary.processed == 1
    assert handled == []
