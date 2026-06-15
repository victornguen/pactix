# pactix

`pactix` is a Python library for transactional outbox and inbox workflows on PostgreSQL, built on **SQLAlchemy** (async).

Use it when a service needs to commit business data and outgoing events in the same transaction, store incoming messages before handler execution, and retry failed work safely across multiple workers.

## Features

- Deduplication by `source + message_id` on the inbox side
- FIFO within the same `event_type + ordering_key`
- Optional unordered inbox processing with a stale-event filter contract
- Retry scheduling and lease-based recovery for stuck workers
- Transport-agnostic core protocols
- Built-in helpers for common hooks, stale-event filters, and closure-based adapters
- Optional Kafka adapters behind the `kafka` extra
- Optional HTTP adapters behind the `http` extra

If a publisher or handler may run more than once, that is normal. Business effects still need idempotent application logic.

## Installation

```bash
pip install pactix                 # core (sqlalchemy + asyncpg)
pip install "pactix[kafka]"        # + Kafka transport (aiokafka)
pip install "pactix[http]"         # + HTTP transport (httpx + starlette)
pip install "pactix[migrations]"   # + Alembic for running the migrations
```

## Database setup

`pactix` does not run migrations for you. Either run the bundled Alembic migrations
(`src/pactix/db/migrations`, see [alembic.ini](alembic.ini)) or copy the table
definitions in [`pactix.db.tables`](src/pactix/db/tables.py) into your own migration
system. The library expects two tables: `outbox_message` and `inbox_message`.

## Documentation

- [Core workflows](docs/core-workflows.md) — append, publish, save, and process flows
- [Helper adapters](docs/helpers.md) — closures, `ProcessAll`, `StaticIdempotency`, filters
- [Kafka transport](docs/kafka.md)
- [HTTP transport](docs/http.md)
- [Operations, migrations, and workers](docs/operations.md)

See [examples/worker_loop.py](examples/worker_loop.py) for a runnable worker-loop sketch.

## Development

```bash
make install   # uv sync with all extras
make check     # format + lint + mypy + test
```

Database-backed tests use [testcontainers](https://testcontainers.com/) and require Docker.
