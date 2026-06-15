# Operations, migrations, and workers

## Migration ownership

`pactix` expects your service to own when migrations run. Either:

- run the bundled Alembic migrations in `src/pactix/db/migrations` (configure the
  URL via `PACTIX_DATABASE_URL` or `alembic.ini`; online upgrades use a sync
  driver, so use a `postgresql://` URL), or
- create the tables from the SQLAlchemy metadata in `pactix.db.tables` through
  your own pipeline.

The library expects these table names: `outbox_message` and `inbox_message`.

## Worker loops

The library exposes `run_once`. Your service owns scheduling.

```python
import asyncio

async def run_outbox_loop(engine, runner, interval=1.0):
    while True:
        try:
            await runner.run_once(engine)
        except Exception as error:
            log.warning('outbox runner failed: %s', error)
        await asyncio.sleep(interval)
```

The inbox loop is identical with an `InboxRunner`.

## Retry and lease behavior

Retry state lives in the same tables as the messages:

- `pending` rows are claimable when `available_at <= now`.
- `processing` rows are leased to one worker until `lease_until`.
- Retryable failures return the row to `pending` with a future `available_at`
  (exponential backoff capped at `max_interval`).
- Terminal failures, and reaching `max_attempts`, move the row to `failed`.
- Expired leases are reclaimed and made `pending` again.

`run_once` claims and reclaims in one transaction, runs your publisher/handler
**outside** any transaction, and applies each state change in its own short
transaction — so a slow publisher does not hold a row's lease open.

## Ordering and delivery semantics

- Delivery is at-least-once, not exactly-once.
- `InboxRunner(...)` preserves FIFO only within the same
  `event_type + ordering_key`.
- `InboxRunner.new_unordered(...)` claims rows without per-key FIFO gating and
  lets a `StaleEventFilter` mark obsolete rows processed before the handler runs.
- Different event types or ordering keys can run in parallel.
- Idempotency is still an application concern even though receipt is durable.

## Rollout checklist

- run the migrations (or create the tables) through your own pipeline
- decide which process owns the outbox worker loop
- decide which process owns the inbox worker loop
- pick a transport module or supply custom publisher/receiver logic
- make business effects idempotent before enabling multiple workers
- add a test run against a real PostgreSQL to CI
