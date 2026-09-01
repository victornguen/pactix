"""Dialect detection and timezone normalization helpers.

Stores branch per call on the dialect name resolved from the caller's
executor (``postgresql`` vs ``mysql``). MySQL ``DATETIME`` is timezone-naive
while the library compares with ``datetime.now(UTC)``, so writes strip to
naive UTC and reads re-attach ``tzinfo=UTC``.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from pactix._executor import Executor

POSTGRESQL = 'postgresql'
MYSQL = 'mysql'


def dialect_name(executor: Executor) -> str:
    """Resolve the DBAPI dialect name (``'postgresql'``, ``'mysql'``, ...) for an executor.

    ``AsyncConnection`` exposes ``.dialect`` directly; ``AsyncSession``
    resolves it through its bind.
    """
    if isinstance(executor, AsyncSession):
        return executor.get_bind().dialect.name
    if isinstance(executor, AsyncConnection):
        return executor.dialect.name
    raise TypeError(f'cannot resolve dialect for executor of type {type(executor).__name__!r}')


def naive_utc(value: datetime) -> datetime:
    """Strip an aware datetime to naive UTC for a MySQL ``DATETIME`` write."""
    return value.astimezone(UTC).replace(tzinfo=None)


def aware_utc(value: datetime) -> datetime:
    """Re-attach ``tzinfo=UTC`` to a datetime read back from the database.

    No-op for values that are already aware (PostgreSQL returns aware
    datetimes), so the read-side normalization applies unconditionally.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


__all__ = ['POSTGRESQL', 'MYSQL', 'dialect_name', 'naive_utc', 'aware_utc']
