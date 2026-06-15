"""Alembic environment for pactix's outbox/inbox migrations.

The database URL is taken from the Alembic config (``sqlalchemy.url``) or the
``PACTIX_DATABASE_URL`` environment variable. A synchronous (psycopg/pg8000)
or asyncpg URL both work for offline rendering; online upgrades use a sync
driver, so point this at a sync URL (e.g. ``postgresql://``) when running
``alembic upgrade``.
"""

from __future__ import annotations

import os

from alembic import context
from sqlalchemy import engine_from_config, pool

from pactix.db.tables import metadata as target_metadata

config = context.config

_url = os.environ.get('PACTIX_DATABASE_URL') or config.get_main_option('sqlalchemy.url')
if _url:
    config.set_main_option('sqlalchemy.url', _url)


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option('sqlalchemy.url'),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
