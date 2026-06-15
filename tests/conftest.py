"""Shared test fixtures.

Pure-logic tests need no database. Tests marked ``@pytest.mark.postgres`` get a
real PostgreSQL via testcontainers (requires Docker); they are skipped if Docker
or testcontainers is unavailable.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from pactix.db.tables import metadata


def _to_asyncpg_url(url: str) -> str:
    return re.sub(r'^postgresql(\+\w+)?://', 'postgresql+asyncpg://', url)


@pytest.fixture(scope='session')
def postgres_url() -> Iterator[str]:
    try:
        from testcontainers.postgres import PostgresContainer
    except Exception as error:  # pragma: no cover
        pytest.skip(f'testcontainers unavailable: {error}')

    try:
        container = PostgresContainer('postgres:16-alpine')
        container.start()
    except Exception as error:  # pragma: no cover
        pytest.skip(f'could not start PostgreSQL container (Docker required): {error}')

    try:
        yield _to_asyncpg_url(container.get_connection_url())
    finally:
        container.stop()


@pytest_asyncio.fixture
async def engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_url)
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
        await conn.run_sync(metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()
