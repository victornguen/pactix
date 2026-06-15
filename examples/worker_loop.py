"""A minimal worker-loop sketch for pactix.

Shows the caller-owned scheduling contract: pactix never spawns loops, it only
exposes ``run_once``. This wires an outbox publisher and an inbox handler, then
polls both runners on an interval.

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
    MessageEnvelope,
    MessageMetadata,
    OutboxRunner,
    OutboxStore,
    PactixConfig,
    ProcessAll,
    PublisherRegistry,
    PublishOutcome,
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


if __name__ == '__main__':
    import sys

    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else 'postgresql+asyncpg://localhost/pactix'))
