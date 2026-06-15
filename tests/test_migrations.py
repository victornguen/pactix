"""Verify the Alembic migrations apply against a real PostgreSQL."""

from __future__ import annotations

import re

import pytest
from sqlalchemy import create_engine, inspect

pytestmark = pytest.mark.postgres


def _to_sync_url(async_url: str) -> str:
    return re.sub(r'^postgresql\+asyncpg://', 'postgresql+psycopg://', async_url)


def test_migrations_create_tables_and_indexes(postgres_url: str) -> None:
    psycopg = pytest.importorskip('psycopg')  # sync driver for alembic online upgrade
    assert psycopg is not None

    from alembic import command
    from alembic.config import Config

    sync_url = _to_sync_url(postgres_url)

    # Start from a clean schema so the migration owns table creation.
    engine = create_engine(sync_url)
    with engine.begin() as conn:
        conn.exec_driver_sql('DROP TABLE IF EXISTS inbox_message')
        conn.exec_driver_sql('DROP TABLE IF EXISTS outbox_message')
        conn.exec_driver_sql('DROP TABLE IF EXISTS alembic_version')
    engine.dispose()

    config = Config('alembic.ini')
    config.set_main_option('sqlalchemy.url', sync_url)
    command.upgrade(config, 'head')

    engine = create_engine(sync_url)
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    assert {'outbox_message', 'inbox_message'} <= tables

    outbox_indexes = {idx['name'] for idx in inspector.get_indexes('outbox_message')}
    assert {
        'idx_outbox_message_ready',
        'idx_outbox_message_fifo',
        'idx_outbox_message_lease',
        'idx_outbox_message_correlation',
    } <= outbox_indexes

    inbox_uniques = {uc['name'] for uc in inspector.get_unique_constraints('inbox_message')}
    assert 'inbox_message_source_message_id_key' in inbox_uniques
    engine.dispose()
