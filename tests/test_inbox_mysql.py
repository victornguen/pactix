"""MySQL-backed tests for the inbox-processing capability.

Mirrors the core behaviors of ``test_inbox_db.py`` against MySQL 8, exercising
the MySQL-specific code paths: ``INSERT IGNORE`` dedup with read-back of the
existing row, the SELECT-then-UPDATE claims (no RETURNING), and the tz-naive
``DATETIME(6)`` storage with UTC re-attachment on read.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from pactix.config import PactixConfig, RetryPolicy
from pactix.inbox import (
    HandleOutcome,
    InboxRunner,
    InboxStatus,
    InboxStore,
    ProcessAll,
    inbox_handler,
)
from pactix.message import MessageEnvelope, MessageMetadata

pytestmark = pytest.mark.mysql


def _config(**kw: object) -> PactixConfig:
    return PactixConfig(**kw)  # type: ignore[arg-type]


def _envelope(message_id: str, event_type: str = 'order.created', **meta_kw: str) -> MessageEnvelope:
    meta = MessageMetadata(message_id, event_type)
    for key, value in meta_kw.items():
        meta = getattr(meta, f'with_{key}')(value)
    return MessageEnvelope(meta, {'message_id': message_id})


async def test_first_receipt_inserts_then_duplicate(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with mysql_engine.begin() as conn:
        first = await store.save_received(conn, 'kafka:orders', _envelope('m-1'))
    assert first.is_inserted

    async with mysql_engine.begin() as conn:
        second = await store.save_received(conn, 'kafka:orders', _envelope('m-1'))
    assert second.is_duplicate
    # the existing row is returned, not the freshly built record
    assert second.record.id == first.record.id
    assert second.record.message_id == 'm-1'

    async with mysql_engine.connect() as conn:
        pending = await store.list_by_status(conn, InboxStatus.PENDING)
    assert len(pending) == 1


async def test_same_message_id_different_source_not_duplicate(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with mysql_engine.begin() as conn:
        a = await store.save_received(conn, 'source-a', _envelope('m-1'))
        b = await store.save_received(conn, 'source-b', _envelope('m-1'))
    assert a.is_inserted and b.is_inserted


async def test_datetimes_read_back_tz_aware_utc(mysql_engine: AsyncEngine) -> None:
    # MySQL stores DATETIME tz-naive; reads must re-attach UTC.
    store = InboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))
    async with mysql_engine.connect() as conn:
        (record,) = await store.list_by_status(conn, InboxStatus.PENDING)
    for value in (record.available_at, record.received_at, record.updated_at):
        assert value.tzinfo is UTC


async def test_fifo_gating_within_ordering_key(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('older', ordering_key='o-1'))
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('newer', ordering_key='o-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    assert [r.message_id for r in claimed] == ['older']


async def test_unordered_claim_ignores_fifo(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('older', ordering_key='o-1'))
        await store.save_received(conn, 's', _envelope('newer', ordering_key='o-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready_unordered(conn, now)
    assert {r.message_id for r in claimed} == {'older', 'newer'}


async def test_mark_processed(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    async with mysql_engine.begin() as conn:
        await store.mark_processed(conn, claimed[0].id, now)

    async with mysql_engine.connect() as conn:
        processed = await store.list_by_status(conn, InboxStatus.PROCESSED)
    assert len(processed) == 1
    assert processed[0].processed_at is not None
    assert processed[0].processed_at.tzinfo is UTC


async def test_mark_retry_reschedules(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config(retry_policy=RetryPolicy(max_attempts=5)))
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    async with mysql_engine.begin() as conn:
        await store.mark_retry(conn, claimed[0].id, 'boom', now - timedelta(seconds=1))

    async with mysql_engine.connect() as conn:
        pending = await store.list_by_status(conn, InboxStatus.PENDING)
    assert len(pending) == 1
    assert pending[0].attempts == 1
    assert pending[0].last_error == 'boom'
    assert pending[0].lease_until is None


async def test_mark_failed(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    async with mysql_engine.begin() as conn:
        await store.mark_failed(conn, claimed[0].id, 'bad')

    async with mysql_engine.connect() as conn:
        failed = await store.list_by_status(conn, InboxStatus.FAILED)
    assert len(failed) == 1
    assert failed[0].attempts == 1
    assert failed[0].last_error == 'bad'


async def test_reclaim_expired_lease(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config(lease_duration=timedelta(seconds=30)))
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    now = datetime.now(UTC)
    async with mysql_engine.begin() as conn:
        claimed = await store.claim_ready(conn, now)
    assert len(claimed) == 1

    future = now + timedelta(hours=1)
    async with mysql_engine.begin() as conn:
        reclaimed = await store.reclaim_expired(conn, future)
    assert reclaimed == 1


async def test_runner_processes_with_process_all(mysql_engine: AsyncEngine) -> None:
    store = InboxStore(_config())
    async with mysql_engine.begin() as conn:
        await store.save_received(conn, 's', _envelope('m-1'))

    handled: list[str] = []

    async def handle(record: object) -> HandleOutcome:
        handled.append('called')
        return HandleOutcome.processed()

    runner = InboxRunner(store, inbox_handler(handle), ProcessAll())
    summary = await runner.run_once(mysql_engine)
    assert summary.processed == 1
    assert summary.processed <= summary.claimed
    assert handled == ['called']
    async with mysql_engine.connect() as conn:
        processed = await store.list_by_status(conn, InboxStatus.PROCESSED)
    assert len(processed) == 1
