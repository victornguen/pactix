# pactix

`pactix` is a Python library for transactional outbox and inbox workflows on PostgreSQL and MySQL 8, built on **SQLAlchemy** (async).

Use it when a service needs to commit business data and outgoing events in the same transaction, store incoming messages before handler execution, and retry failed work safely across multiple workers.

## Features

- PostgreSQL and MySQL 8 persistence, auto-detected per call from the SQLAlchemy engine/connection — no config knob
- Deduplication by `source + message_id` on the inbox side
- FIFO within the same `event_type + ordering_key`
- Optional unordered inbox processing with a stale-event filter contract
- Retry scheduling and lease-based recovery for stuck workers
- Optional wakeup-driven outbox scheduling — post-commit signal and PostgreSQL LISTEN/NOTIFY with adaptive fallback polling (see [Operations](docs/operations.md#wakeup-driven-scheduling-optional))
- Transport-agnostic core protocols
- Built-in helpers for common hooks, stale-event filters, and closure-based adapters
- Optional Kafka adapters behind the `kafka` extra
- Optional HTTP adapters behind the `http` extra

If a publisher or handler may run more than once, that is normal. Business effects still need idempotent application logic.

## Installation

```bash
pip install "pactix[postgres]"     # core + asyncpg driver (PostgreSQL)
pip install "pactix[mysql]"        # core + asyncmy driver (MySQL 8)
pip install "pactix[kafka]"        # + Kafka transport (aiokafka)
pip install "pactix[http]"         # + HTTP transport (httpx + starlette)
pip install "pactix[migrations]"   # + Alembic for running the migrations
```

The core package ships no database driver — install one of the driver extras
above (extras combine, e.g. `pactix[postgres,kafka]`).

**Breaking change:** `asyncpg` moved out of the core dependencies into the
`postgres` extra. Existing PostgreSQL services must change their requirement
from `pactix` to `pactix[postgres]` when upgrading — nothing else changes. The
failure mode without it is loud: SQLAlchemy raises at engine creation naming
the missing DBAPI.

MySQL 8 note: stock MySQL 8 authenticates with `caching_sha2_password`. Over a
non-TLS connection asyncmy's RSA password exchange additionally needs the
`cryptography` package, which the `mysql` extra already pulls in (TLS
connections do not need it).

## Database setup

`pactix` does not run migrations for you. Either run the bundled Alembic migrations
(`src/pactix/db/migrations`, see [alembic.ini](alembic.ini)) or copy the table
definitions in [`pactix.db.tables`](src/pactix/db/tables.py) into your own migration
system. The library expects two tables: `outbox_message` and `inbox_message`.

The same migrations run on both dialects (the wakeup-trigger revision no-ops on
MySQL); for MySQL run `alembic upgrade` with a sync `mysql+pymysql://` URL. See
[Operations](docs/operations.md#mysql) for the MySQL notes.

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
