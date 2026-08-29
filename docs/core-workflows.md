# Core workflows

`pactix` has two sides: the **outbox** (publish events your service produces) and
the **inbox** (durably receive and process events from elsewhere). Both store
state in PostgreSQL and are driven by one-pass runners that your service
schedules.

## Outbox: append

Append an event inside the same transaction as your business write, using a
SQLAlchemy `AsyncConnection` (or `AsyncSession`). If the transaction rolls back,
the outbox row disappears with it.

```python
from pactix import MessageEnvelope, MessageMetadata, OutboxStore, PactixConfig

store = OutboxStore(PactixConfig())

async with engine.begin() as conn:
    await do_business_write(conn)
    metadata = MessageMetadata.for_event_type('order.created').with_ordering_key('order-42')
    await store.append(conn, MessageEnvelope(metadata, {'order_id': 'order-42'}))
```

## Outbox: publish

Register a publisher per event type (or a catch-all) and run the outbox runner.
`run_once` reclaims expired leases, claims ready rows (FIFO per
`event_type + ordering_key`), publishes each, and records the result.

```python
from pactix import OutboxRunner, PublisherRegistry, PublishOutcome, outbox_publisher

registry = PublisherRegistry()
await registry.register(
    'order.created',
    outbox_publisher(lambda record: publish_to_transport(record)),
)

runner = OutboxRunner(store, registry)
summary = await runner.run_once(engine)   # summary.published <= summary.claimed
```

A publisher returns `PublishOutcome.published()`, `PublishOutcome.retryable(msg)`,
or `PublishOutcome.terminal(msg)`.

Scheduling is caller-owned — poll on an interval, or use the optional
wakeup-driven loop (`WakeupRunner`) for millisecond reaction; see
[Operations](operations.md#wakeup-driven-scheduling-optional).

## Inbox: save received

Durably store an incoming message before any business processing. Deduplication
is enforced on `(source, message_id)`.

```python
from pactix import InboxStore, MessageEnvelope, MessageMetadata, PactixConfig

inbox = InboxStore(PactixConfig())

async with engine.begin() as conn:
    outcome = await inbox.save_received(conn, 'kafka:orders', envelope)
    # outcome.is_inserted / outcome.is_duplicate
```

## Inbox: process

Wire a handler and an idempotency hook, then run the inbox runner. For the common
case where every pending row should reach the handler, use `ProcessAll`.

```python
from pactix import HandleOutcome, InboxRunner, ProcessAll, inbox_handler

handler = inbox_handler(lambda record: apply_business_effect(record))
runner = InboxRunner(inbox, handler, ProcessAll())
summary = await runner.run_once(engine)   # summary.processed <= summary.claimed
```

A handler returns `HandleOutcome.processed()`, `HandleOutcome.retryable(msg)`, or
`HandleOutcome.terminal(msg)`. An idempotency hook returns
`IdempotencyDecision.PROCESS`, `.SKIP`, or `.ALREADY_APPLIED`.
