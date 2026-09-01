"""Verify the Alembic migrations apply against a real MySQL 8."""

from __future__ import annotations

import re

import pytest
from sqlalchemy import create_engine, inspect, text

pytestmark = pytest.mark.mysql


def _to_sync_url(async_url: str) -> str:
    return re.sub(r'^mysql\+asyncmy://', 'mysql+pymysql://', async_url)


def test_migrations_create_tables_and_indexes(mysql_url: str) -> None:
    pymysql = pytest.importorskip('pymysql')  # sync driver for alembic online upgrade
    assert pymysql is not None

    from alembic import command
    from alembic.config import Config

    sync_url = _to_sync_url(mysql_url)

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

    # MySQL keeps the declared index names; the partial PostgreSQL indexes
    # degrade to plain ones, and unique constraints surface as unique indexes.
    outbox_indexes = {idx['name'] for idx in inspector.get_indexes('outbox_message')}
    assert {
        'idx_outbox_message_ready',
        'idx_outbox_message_fifo',
        'idx_outbox_message_lease',
        'idx_outbox_message_correlation',
    } <= outbox_indexes

    inbox_uniques = {uc['name'] for uc in inspector.get_unique_constraints('inbox_message')}
    assert 'inbox_message_source_message_id_key' in inbox_uniques

    # Revision 0003 (the wakeup trigger) is a no-op on MySQL: no trigger, no function.
    with engine.connect() as conn:
        triggers = conn.execute(
            text(
                'SELECT TRIGGER_NAME FROM information_schema.TRIGGERS '
                "WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME LIKE '%outbox_wake%'"
            )
        ).all()
        routines = conn.execute(
            text(
                'SELECT ROUTINE_NAME FROM information_schema.ROUTINES '
                "WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'outbox_wake'"
            )
        ).all()
    assert triggers == []
    assert routines == []
    engine.dispose()
