"""Benchmark: standard polling vs wakeup-driven outbox scheduling on PostgreSQL.

Three scheduling scenarios run against a fresh schema on a testcontainers
PostgreSQL 16, each under two load profiles:

- ``polling`` — caller-owned loop around ``OutboxRunner.run_once`` at the
  default ``outbox_poll_interval`` (250ms), mirroring ``examples/worker_loop.py``.
- ``wakeup-trigger`` — migration 0003's trigger DDL applied verbatim, then
  ``WakeupRunner`` with ``NotifyMode.TRIGGER``.
- ``wakeup-coalesced`` — ``WakeupRunner`` with ``NotifyMode.COALESCED``; the
  producer calls ``CoalescedNotifier.notify()`` after each commit.

No ``LocalWakeup`` is wired in: the point is the cross-process NOTIFY path
that distinguishes the two wakeup modes (an in-process signal would dominate
both). Adaptive fallback polling stays on as the safety net in all cases.

Metrics per run: per-event latency (``published_at - created_at`` read back
from the DB after drain; both timestamps are written by this same process, so
the clock is identical), throughput over the wall window from first append to
last observed publish, worker-side DB statement count (``before_cursor_execute``
on the worker engine only), whole-process CPU from ``getrusage``, and the
worker's statement rate during a 5s post-drain idle window.

Run with ``uv run python -m benchmarks.bench_outbox`` (Docker required);
writes ``benchmarks/results.json`` and prints a table.
"""

from __future__ import annotations

import asyncio
import json
import math
import platform
import re
import resource
import subprocess
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import event, func, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer

from pactix import (
    CoalescedNotifier,
    MessageEnvelope,
    MessageMetadata,
    NotifyMode,
    OutboxMessageRecord,
    OutboxRunner,
    OutboxStatus,
    OutboxStore,
    PactixConfig,
    PublisherRegistry,
    PublishOutcome,
    WakeupPolicy,
    WakeupRunner,
    outbox_publisher,
)
from pactix.db.tables import metadata, outbox_message

RESULTS_PATH = Path(__file__).resolve().parent / 'results.json'
POSTGRES_IMAGE = 'postgres:16-alpine'

# Trigger DDL copied verbatim from the upgrade() of
# src/pactix/db/migrations/versions/0003_add_outbox_wake_trigger.py.
TRIGGER_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION outbox_wake() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('outbox_wake', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql
"""
TRIGGER_SQL = """
CREATE TRIGGER outbox_message_wake
AFTER INSERT ON outbox_message
FOR EACH ROW
EXECUTE FUNCTION outbox_wake()
"""

SCENARIOS = ('polling', 'wakeup-trigger', 'wakeup-coalesced')
PROFILES = ('steady', 'burst')

STEADY_EVENTS = 1000
STEADY_RATE_PER_S = 100.0
BURST_EVENTS = 2000
BURST_EVENTS_PER_TXN = 50

DRAIN_POLL_S = 0.025
DRAIN_TIMEOUT_S = 120.0
WORKER_STARTUP_GRACE_S = 0.5
# Post-drain window observing how busy an idle worker stays (fixed-interval
# polling vs backoff-driven fallback).
IDLE_WINDOW_S = 5.0


@dataclass
class RunResult:
    """Metrics for one scenario x profile run."""

    scenario: str
    profile: str
    events: int
    wall_s: float
    throughput_eps: float
    latency_mean_ms: float
    latency_p50_ms: float
    latency_p99_ms: float
    worker_queries: int
    worker_queries_per_s: float
    cpu_percent: float
    idle_window_s: float
    idle_queries: int
    idle_queries_per_s: float


class QueryCounter:
    """Counts DBAPI statements executed through one engine."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.count = 0
        event.listen(engine.sync_engine, 'before_cursor_execute', self._on_execute)

    def _on_execute(
        self,
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        self.count += 1


def _to_asyncpg_url(url: str) -> str:
    return re.sub(r'^postgresql(\+\w+)?://', 'postgresql+asyncpg://', url)


def _cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    rank = (len(sorted_values) - 1) * pct / 100.0
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return sorted_values[low]
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (rank - low)


async def _publish(record: OutboxMessageRecord) -> PublishOutcome:
    return PublishOutcome.published()


async def _append(conn: AsyncConnection, store: OutboxStore, seq: int) -> None:
    envelope = MessageEnvelope(MessageMetadata.for_event_type('bench.event'), {'seq': seq})
    await store.append(conn, envelope)


async def _produce_steady(engine: AsyncEngine, store: OutboxStore, notifier: CoalescedNotifier | None) -> None:
    """One event per transaction, deadline-paced at ~100 events/s."""
    start = time.perf_counter()
    for i in range(STEADY_EVENTS):
        async with engine.begin() as conn:
            await _append(conn, store, i)
        if notifier is not None:
            await notifier.notify()
        delay = start + (i + 1) / STEADY_RATE_PER_S - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)


async def _produce_burst(engine: AsyncEngine, store: OutboxStore, notifier: CoalescedNotifier | None) -> None:
    """As fast as possible, 50 events per transaction."""
    for batch_start in range(0, BURST_EVENTS, BURST_EVENTS_PER_TXN):
        async with engine.begin() as conn:
            for i in range(batch_start, batch_start + BURST_EVENTS_PER_TXN):
                await _append(conn, store, i)
        if notifier is not None:
            await notifier.notify()


async def _polling_worker(engine: AsyncEngine, runner: OutboxRunner, interval_s: float) -> None:
    """The caller-owned fixed-interval loop from examples/worker_loop.py."""
    while True:
        await runner.run_once(engine)
        await asyncio.sleep(interval_s)


async def _fresh_schema(engine: AsyncEngine, with_trigger: bool) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.run_sync(metadata.create_all)
        if with_trigger:
            await conn.execute(text(TRIGGER_FUNCTION_SQL))
            await conn.execute(text(TRIGGER_SQL))


async def _wait_drained(engine: AsyncEngine, expected: int, worker: asyncio.Task[None]) -> None:
    """Poll the published count (on the uncounted producer engine) until all rows are published."""
    deadline = time.monotonic() + DRAIN_TIMEOUT_S
    published = 0
    while True:
        if worker.done():
            worker.result()  # re-raise a worker failure
            raise RuntimeError('worker stopped before the outbox drained')
        async with engine.connect() as conn:
            published = await conn.scalar(
                select(func.count()).select_from(outbox_message).where(outbox_message.c.status == 'published')
            )
        if published is not None and published >= expected:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f'only {published}/{expected} published within {DRAIN_TIMEOUT_S}s')
        await asyncio.sleep(DRAIN_POLL_S)


async def _latencies_ms(engine: AsyncEngine, store: OutboxStore) -> list[float]:
    """Per-event publish latency read back from the DB (same process wrote both timestamps)."""
    async with engine.connect() as conn:
        records = await store.list_by_status(conn, OutboxStatus.PUBLISHED)
    latencies = []
    for record in records:
        if record.published_at is None:
            raise RuntimeError(f'{record.message_id} has status published but no published_at')
        latencies.append((record.published_at - record.created_at).total_seconds() * 1000.0)
    return latencies


async def _execute_run(postgres_url: str, scenario: str, profile: str) -> RunResult:
    config = PactixConfig()
    store = OutboxStore(config)
    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(_publish))
    runner = OutboxRunner(store, registry)

    producer = create_async_engine(postgres_url)
    worker_engine = create_async_engine(postgres_url)
    counter = QueryCounter(worker_engine)
    worker: asyncio.Task[None] | None = None
    try:
        await _fresh_schema(producer, with_trigger=scenario == 'wakeup-trigger')

        notifier: CoalescedNotifier | None = None
        if scenario == 'polling':
            interval_s = config.outbox_poll_interval.total_seconds()
            worker = asyncio.create_task(_polling_worker(worker_engine, runner, interval_s))
        else:
            mode = NotifyMode.TRIGGER if scenario == 'wakeup-trigger' else NotifyMode.COALESCED
            policy = WakeupPolicy(enabled=True, notify_mode=mode)
            if mode is NotifyMode.COALESCED:
                notifier = CoalescedNotifier(producer, policy.channel, policy.coalesce_interval)
            worker = asyncio.create_task(WakeupRunner(runner, policy).run(worker_engine))

        # Give the polling loop its first pass and the wake listener time to
        # establish LISTEN before the measured window opens.
        await asyncio.sleep(WORKER_STARTUP_GRACE_S)

        events = STEADY_EVENTS if profile == 'steady' else BURST_EVENTS
        produce: Callable[[AsyncEngine, OutboxStore, CoalescedNotifier | None], Awaitable[None]] = (
            _produce_steady if profile == 'steady' else _produce_burst
        )

        wall_start = time.perf_counter()
        cpu_start = _cpu_seconds()
        await produce(producer, store, notifier)
        await _wait_drained(producer, events, worker)
        wall_s = time.perf_counter() - wall_start
        cpu_s = _cpu_seconds() - cpu_start
        active_queries = counter.count

        # Keep the worker running against an empty outbox: fixed-interval
        # polling keeps polling, the wakeup fallback backs off.
        await asyncio.sleep(IDLE_WINDOW_S)
        idle_queries = counter.count - active_queries

        latencies = sorted(await _latencies_ms(producer, store))
        return RunResult(
            scenario=scenario,
            profile=profile,
            events=events,
            wall_s=round(wall_s, 3),
            throughput_eps=round(events / wall_s, 1),
            latency_mean_ms=round(sum(latencies) / len(latencies), 2),
            latency_p50_ms=round(_percentile(latencies, 50), 2),
            latency_p99_ms=round(_percentile(latencies, 99), 2),
            worker_queries=active_queries,
            worker_queries_per_s=round(active_queries / wall_s, 1),
            cpu_percent=round(cpu_s / wall_s * 100.0, 1),
            idle_window_s=IDLE_WINDOW_S,
            idle_queries=idle_queries,
            idle_queries_per_s=round(idle_queries / IDLE_WINDOW_S, 1),
        )
    finally:
        if worker is not None:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker
        await worker_engine.dispose()
        await producer.dispose()


def _cpu_model() -> str:
    try:
        return subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string'], text=True).strip()
    except Exception:  # noqa: BLE001 - best-effort environment info
        return platform.processor() or 'unknown'


def _format_table(results: list[RunResult]) -> str:
    header = (
        f'{"scenario":<18}{"profile":<8}{"events":>7}{"wall_s":>8}{"eps":>8}'
        f'{"mean_ms":>9}{"p50_ms":>9}{"p99_ms":>9}{"queries":>9}{"q/s":>8}{"cpu%":>7}{"idle_q/s":>9}'
    )
    lines = [header, '-' * len(header)]
    for r in results:
        lines.append(
            f'{r.scenario:<18}{r.profile:<8}{r.events:>7}{r.wall_s:>8.2f}{r.throughput_eps:>8.1f}'
            f'{r.latency_mean_ms:>9.2f}{r.latency_p50_ms:>9.2f}{r.latency_p99_ms:>9.2f}'
            f'{r.worker_queries:>9}{r.worker_queries_per_s:>8.1f}{r.cpu_percent:>7.1f}{r.idle_queries_per_s:>9.1f}'
        )
    return '\n'.join(lines)


async def main() -> None:
    container = PostgresContainer(POSTGRES_IMAGE)
    container.start()
    try:
        postgres_url = _to_asyncpg_url(container.get_connection_url())
        results = []
        for scenario in SCENARIOS:
            for profile in PROFILES:
                print(f'running {scenario} / {profile} ...', flush=True)
                results.append(await _execute_run(postgres_url, scenario, profile))
    finally:
        container.stop()

    table = _format_table(results)
    print(table)

    config = PactixConfig()
    policy = WakeupPolicy(enabled=True)
    payload = {
        'benchmark': 'standard polling vs wakeup-driven outbox scheduling (PostgreSQL)',
        'generated_at': datetime.now(UTC).isoformat(),
        'environment': {
            'python': platform.python_version(),
            'platform': platform.platform(),
            'machine': platform.machine(),
            'cpu': _cpu_model(),
            'postgres_image': POSTGRES_IMAGE,
        },
        'config': {
            'outbox_poll_interval_ms': config.outbox_poll_interval.total_seconds() * 1000,
            'outbox_batch_size': config.outbox_batch_size,
            'lease_duration_s': config.lease_duration.total_seconds(),
            'wakeup_channel': policy.channel,
            'coalesce_interval_ms': policy.coalesce_interval.total_seconds() * 1000,
            'fallback_initial_interval_s': policy.fallback_initial_interval.total_seconds(),
            'fallback_max_interval_s': policy.fallback_max_interval.total_seconds(),
            'fallback_multiplier': policy.fallback_multiplier,
            'local_wakeup': 'not wired (measuring the cross-process NOTIFY path)',
            'profiles': {
                'steady': {'events': STEADY_EVENTS, 'rate_per_s': STEADY_RATE_PER_S, 'events_per_txn': 1},
                'burst': {'events': BURST_EVENTS, 'events_per_txn': BURST_EVENTS_PER_TXN},
            },
        },
        'metric_definitions': {
            'latency_ms': 'published_at - created_at read back from the DB after drain (same process clock)',
            'throughput_eps': 'events / wall seconds from first append to last observed publish',
            'worker_queries': 'statements via before_cursor_execute on the worker engine only, within the wall window',
            'cpu_percent': 'process getrusage utime+stime delta / wall * 100 (producer and worker share the process)',
            'idle_queries_per_s': 'worker statements/s during a 5s post-drain window with an empty outbox',
        },
        'runs': [asdict(r) for r in results],
    }
    RESULTS_PATH.write_text(json.dumps(payload, indent=2) + '\n')
    print(f'\nwrote {RESULTS_PATH}')


if __name__ == '__main__':
    asyncio.run(main())
