"""A minimal worker-loop sketch for pactix.

Shows the caller-owned scheduling contract: pactix never spawns loops, it only
exposes ``run_once``. This wires an outbox publisher and an inbox handler, then
polls both runners on an interval. An optional wakeup-driven variant of the
outbox loop is included below (``run_wakeup_loop``); see docs/operations.md.

Run against a database that already has the ``outbox_message`` and
``inbox_message`` tables (see docs/operations.md). This file is illustrative; it
is not imported by the library or its tests.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from pactix import (
    HandleOutcome,
    InboxRunner,
    InboxStore,
    LocalWakeup,
    MessageEnvelope,
    MessageMetadata,
    OutboxRunner,
    OutboxStore,
    PactixConfig,
    ProcessAll,
    PublisherRegistry,
    PublishOutcome,
    WakeupPolicy,
    WakeupRunner,
    inbox_handler,
    outbox_publisher,
)

log = logging.getLogger('pactix.example')


async def publish(record: object) -> PublishOutcome:
    # Hand the record to your transport here; return the matching outcome.
    print(f'publishing {record.message_id}')  # type: ignore[attr-defined]
    return PublishOutcome.published()


async def handle(record: object) -> HandleOutcome:
    # Apply the business effect idempotently; return the matching outcome.
    print(f'handling {record.message_id}')  # type: ignore[attr-defined]
    return HandleOutcome.processed()


async def append_one(engine: AsyncEngine, store: OutboxStore) -> None:
    """Append an event in the same transaction as a (hypothetical) business write."""
    async with engine.begin() as conn:
        metadata = MessageMetadata.for_event_type('order.created').with_ordering_key('order-42')
        await store.append(conn, MessageEnvelope(metadata, {'order_id': 'order-42'}))


async def run_loop(engine: AsyncEngine, runner: OutboxRunner | InboxRunner, interval: float) -> None:
    while True:
        try:
            await runner.run_once(engine)
        except Exception as error:  # noqa: BLE001 - keep the loop alive
            log.warning('runner failed: %s', error)
        await asyncio.sleep(interval)


async def run_wakeup_loop(
    engine: AsyncEngine,
    runner: OutboxRunner,
    policy: WakeupPolicy,
    local_wakeup: LocalWakeup,
) -> None:
    """Optional outbox variant: wait on wake signals instead of polling.

    Pass ``config.wakeup`` with ``enabled=True`` — ``WakeupRunner`` raises
    ``ValidationError`` otherwise. Share ``local_wakeup`` with the code that
    appends rows and call ``local_wakeup.wake()`` right after each commit.
    """
    await WakeupRunner(runner, policy, local_wakeup).run(engine)


async def main(dsn: str) -> None:
    config = PactixConfig()
    engine = create_async_engine(dsn)

    outbox_store = OutboxStore(config)
    registry = PublisherRegistry()
    await registry.register_all(outbox_publisher(publish))
    outbox_runner = OutboxRunner(outbox_store, registry)

    inbox_store = InboxStore(config)
    inbox_runner = InboxRunner(inbox_store, inbox_handler(handle), ProcessAll())

    await append_one(engine, outbox_store)

    # The service owns scheduling — run both loops concurrently.
    await asyncio.gather(
        run_loop(engine, outbox_runner, config.outbox_poll_interval.total_seconds()),
        run_loop(engine, inbox_runner, config.inbox_poll_interval.total_seconds()),
    )

    # Optional: replace the fixed-interval outbox loop above with wakeup-driven
    # scheduling (see docs/operations.md). Off by default; enable and wire:
    #
    # config = PactixConfig(wakeup=WakeupPolicy(enabled=True))
    # local_wakeup = LocalWakeup()
    # ... local_wakeup.wake() after each commit that appends rows ...
    # await asyncio.gather(
    #     run_wakeup_loop(engine, outbox_runner, config.wakeup, local_wakeup),
    #     run_loop(engine, inbox_runner, config.inbox_poll_interval.total_seconds()),
    # )


if __name__ == '__main__':
    import sys

    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else 'postgresql+asyncpg://localhost/pactix'))
